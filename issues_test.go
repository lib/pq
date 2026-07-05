package pq

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/pqerror"
)

// #1046: stmt.QueryRowContext doesn't respect canceled context
func TestQueryRowContext(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	ctxTimeout := time.Millisecond * 50
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, `SELECT pg_sleep(1) AS id`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	var d []uint8
	err = stmt.QueryRowContext(ctx).Scan(&d)
	dl, _ := ctx.Deadline()
	since := time.Since(dl)
	if since > ctxTimeout {
		t.Logf("FAIL %s: query returned after context deadline: %v\n", t.Name(), since)
		t.Fail()
	}
	mustAs(t, err, pqerror.QueryCanceled)
}

// #1062: drivers.ErrBadConn returned for DB.QueryRowContext.Scan when context is cancelled
func TestQueryRowContextBad(t *testing.T) {
	if !pqtest.Pgpool() {
		t.Parallel()
	}
	db := pqtest.MustDB(t)

	// Ensure that cancelling a QueryRowContext does not result in an ErrBadConn.
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		row := db.QueryRowContext(ctx, "select 1")

		var v int
		err := row.Scan(&v)

		// nil, context Canceled, and  QueryCancelled are all fine.
		if err != nil && err != context.Canceled {
			mustAs(t, err, pqerror.QueryCanceled)
		}
	}
}

func connIsValid(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// the connection must be valid
	err = conn.PingContext(ctx)
	if err != nil {
		t.Errorf("PingContext err=%#v", err)
	}
	// close must not return an error
	err = conn.Close()
	if err != nil {
		t.Errorf("Close err=%#v", err)
	}
}

func TestQueryCancelRace(t *testing.T) {
	db := pqtest.MustDB(t)

	// cancel a query while executing on Postgres: must return the cancelled error code
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	row := db.QueryRowContext(ctx, "select pg_sleep(0.5)")
	var pgSleepVoid string
	err := row.Scan(&pgSleepVoid)

	mustAs(t, err, pqerror.QueryCanceled)

	// get a connection: it must be a valid
	connIsValid(t, db)
}

// Test cancelling a scan after it is started. This broke with 1.10.4.
func TestQueryCancelledReused(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	// run a query that returns a lot of data
	rows, err := db.QueryContext(ctx, "select generate_series(1, 10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// scan the first value
	if !rows.Next() {
		t.Error("expected rows.Next() to return true")
	}
	var i int
	err = rows.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Error(i)
	}

	// cancel the context and close rows, ignoring errors
	cancel()
	rows.Close()

	// get a connection: it must be valid
	connIsValid(t, db)
}

// #1324: an ErrorResponse larger than proto.MaxErrlen sent after the startup
// handshake was misparsed as a pre-protocol plain-text error (garbling the
// message) and left the connection desynchronized: the ErrorResponse body and
// the trailing ReadyForQuery were never drained, so inProgress stayed true
// and the poisoned connection was handed back out by the pool.
func TestOversizedErrorResponse(t *testing.T) {
	t.Parallel()

	wantMsg := strings.Repeat("x", proto.MaxErrlen+10000)

	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		f.Startup(cn, nil)
		for {
			code, msg, ok := f.ReadMsg(cn)
			if !ok {
				return
			}
			switch code {
			case proto.Terminate:
				cn.Close()
				return
			case proto.Query:
				switch strings.TrimRight(string(msg), "\x00") {
				case ";":
					// MustDB's Ping.
					f.WriteMsg(cn, proto.EmptyQueryResponse, "")
					f.WriteMsg(cn, proto.ReadyForQuery, "I")
				case "select 1":
					f.WriteMsg(cn, proto.CommandComplete, "SELECT 1\x00")
					f.WriteMsg(cn, proto.ReadyForQuery, "I")
				default:
					f.WriteMsg(cn, proto.ErrorResponse, "SERROR\x00C58030\x00M"+wantMsg+"\x00\x00")
					f.WriteMsg(cn, proto.ReadyForQuery, "I")
				}
			}
		}
	})
	defer f.Close()

	db := pqtest.MustDB(t, f.DSN())
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err := db.Exec(`DO $$ BEGIN RAISE EXCEPTION 'x'; END $$;`)
	if err == nil {
		t.Fatal("first Exec: want non-nil error, got nil")
	}
	var pqErr *Error
	if !errors.As(err, &pqErr) {
		t.Fatalf("first Exec: want *pq.Error, got %T: %v", err, err)
	}
	if pqErr.Message != wantMsg {
		t.Errorf("first Exec: Message mangled: got %d bytes, want %d bytes", len(pqErr.Message), len(wantMsg))
	}
	if pqErr.Code != "58030" {
		t.Errorf("first Exec: Code = %q, want 58030", pqErr.Code)
	}

	// The connection must not be poisoned: a second query on the same pooled
	// connection must not fail with errQueryInProgress.
	_, err = db.Exec("select 1")
	if err != nil {
		t.Fatalf("second Exec: connection left poisoned: %v", err)
	}
}
