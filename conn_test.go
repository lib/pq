package pq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pgpass"
	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/pqutil"
	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/pqerror"
)

func TestReconnect(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	tx := pqtest.Begin(t, db)

	pid := pqtest.Query[int64](t, tx, `select pg_backend_pid() as p`)[0]["p"]

	pqtest.Exec(t, pqtest.MustDB(t), `select pg_terminate_backend($1)`, pid)
	tx.Rollback()
	have := pqtest.Query[int64](t, db, `select 42 as n`)[0]["n"]
	if have != 42 {
		t.Errorf("\nwant: 42\nhave: %v", have)
	}
}

func TestCommitInFailedTransaction(t *testing.T) {
	tx := pqtest.Begin(t, pqtest.MustDB(t))

	rows, err := tx.Query("select error")
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
	err = tx.Commit()
	if err != ErrInFailedTransaction {
		t.Fatalf("expected ErrInFailedTransaction; got %#v", err)
	}
}

func TestOpen(t *testing.T) {
	tests := []struct {
		dsn, wantErr string
	}{
		{"postgres://", ""},
		{"postgresql://", ""},
		{"host=doesnotexist hostaddr=127.0.0.1", ""}, // Should ignore the host

		{"hostaddr=255.255.255.255", "dial tcp 255.255.255.255"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.dsn, func(t *testing.T) {
			t.Parallel()
			_, err := pqtest.DB(t, tt.dsn)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestPgpass(t *testing.T) {
	warnbuf := new(bytes.Buffer)
	pqutil.WarnFD = warnbuf
	defer func() { pqutil.WarnFD = os.Stderr }()

	assertPassword := func(want string, extra map[string]string) {
		o := map[string]string{
			"host":            "localhost",
			"sslmode":         "disable",
			"connect_timeout": "20",
			"user":            "majid",
			"port":            "5432",
			"dbname":          "pqgo",
			"client_encoding": "UTF8",
			"datestyle":       "ISO, MDY",
		}
		for k, v := range extra {
			o[k] = v
		}
		have := pgpass.PasswordFromPgpass(o["passfile"], o["user"], o["password"], o["host"], o["port"], o["dbname"])
		if have != want {
			t.Fatalf("wrong password\nhave: %q\nwant: %q", have, want)
		}
	}

	file := pqtest.TempFile(t, "pgpass", pqtest.NormalizeIndent(`
		# comment
		server:5432:some_db:some_user:pass_A
		*:5432:some_db:some_user:pass_B
		localhost:*:*:*:pass_C
		*:*:*:*:pass_fallback
	`))

	// Missing passfile means empty password.
	assertPassword("", map[string]string{"host": "server", "dbname": "some_db", "user": "some_user"})

	// wrong permissions for the pgpass file means it should be ignored
	assertPassword("", map[string]string{"host": "example.com", "passfile": file, "user": "foo"})
	if h := "has group or world access"; !strings.Contains(warnbuf.String(), h) {
		t.Errorf("unexpected warning\nhave: %s\nwant: %s", warnbuf, h)
	}
	warnbuf.Reset()

	pqtest.Chmod(t, 0o600, file) // Fix the permissions

	assertPassword("pass_A", map[string]string{"host": "server", "passfile": file, "dbname": "some_db", "user": "some_user"})
	assertPassword("pass_fallback", map[string]string{"host": "example.com", "passfile": file, "user": "foo"})
	assertPassword("pass_B", map[string]string{"host": "example.com", "passfile": file, "dbname": "some_db", "user": "some_user"})

	// localhost also matches the default "" and UNIX sockets
	assertPassword("pass_C", map[string]string{"host": "", "passfile": file, "user": "some_user"})
	assertPassword("pass_C", map[string]string{"host": "/tmp", "passfile": file, "user": "some_user"})

	// Connection parameter takes precedence
	os.Setenv("PGPASSFILE", "/tmp")
	defer os.Unsetenv("PGPASSFILE")
	assertPassword("pass_A", map[string]string{"host": "server", "passfile": file, "dbname": "some_db", "user": "some_user"})
	if warnbuf.String() != "" {
		t.Errorf("warnbuf not empty: %s", warnbuf)
	}
}

func TestExecNilSlice(t *testing.T) {
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, `create temp table x (b1 text, b2 text, b3 text)`)
	var (
		b1 []byte
		b2 []string
		b3 = []byte{}
	)
	pqtest.Exec(t, db, `insert into x (b1, b2, b3) values ($1, $2, $3)`, b1, b2, b3)

	have := pqtest.QueryRow[*string](t, db, `select * from x`)

	var s string
	want := map[string]*string{"b1": nil, "b2": nil, "b3": &s}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestExec(t *testing.T) {
	tests := []struct {
		query string
		args  []any
		rows  int64
	}{
		{`insert into tbl values (1)`, nil, 1},
		{`insert into tbl values ($1), ($2), ($3)`, []any{1, 2, 3}, 3},
		{`select g from generate_series(1, 2) g`, nil, 2},
		{`select g from generate_series(1, $1) g`, []any{3}, 3},
	}

	db := pqtest.MustDB(t)
	pqtest.Exec(t, db, `create temp table tbl (a int)`)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			r, err := db.Exec(tt.query, tt.args...)
			if err != nil {
				t.Fatal(err)
			}
			if n, _ := r.RowsAffected(); n != tt.rows {
				t.Fatalf("want %d row affected, not %d", tt.rows, n)
			}
		})
	}
}

func TestStatment(t *testing.T) {
	db := pqtest.MustDB(t)

	stmt1 := pqtest.Prepare(t, db, "select 1")
	stmt2 := pqtest.Prepare(t, db, "select 2")

	r, err := stmt1.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		t.Fatal("expected row")
	}

	var i int
	err = r.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}

	r1, err := stmt2.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()

	if !r1.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	err = r1.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 2 {
		t.Fatalf("expected 2, got %d", i)
	}
}

func TestParameterCountMismatch(t *testing.T) {
	db := pqtest.MustDB(t)

	var notused int
	err := db.QueryRow("SELECT false", 1).Scan(&notused)
	if err == nil {
		t.Fatal("expected err")
	}
	// make sure we clean up correctly
	err = db.QueryRow("SELECT 1").Scan(&notused)
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow("SELECT $1").Scan(&notused)
	if err == nil {
		t.Fatal("expected err")
	}
	// make sure we clean up correctly
	err = db.QueryRow("SELECT 1").Scan(&notused)
	if err != nil {
		t.Fatal(err)
	}
}

// Test that EmptyQueryResponses are handled correctly.
func TestEmptyQuery(t *testing.T) {
	db := pqtest.MustDB(t)

	res, err := db.Exec("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.RowsAffected(); err != errNoRowsAffected {
		t.Fatalf("want %s, got %v", errNoRowsAffected, err)
	}
	if _, err := res.LastInsertId(); err != errNoLastInsertID {
		t.Fatalf("want %s, got %v", errNoLastInsertID, err)
	}

	have := pqtest.Query[any](t, db, ``)
	want := []map[string]any{}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}

	stmt := pqtest.Prepare(t, db, "")
	stmt.MustExec(t)
	res = stmt.MustExec(t)
	if _, err := res.RowsAffected(); err != errNoRowsAffected {
		t.Fatalf("expected %s, got %v", errNoRowsAffected, err)
	}
	if _, err := res.LastInsertId(); err != errNoLastInsertID {
		t.Fatalf("expected %s, got %v", errNoLastInsertID, err)
	}
	rows, err := stmt.Query()
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 0 {
		t.Fatalf("unexpected number of columns %d in response to an empty query", len(cols))
	}
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

// Test that rows.Columns() is correct even if there are no result rows.
func TestEmptyResultSetColumns(t *testing.T) {
	db := pqtest.MustDB(t)

	t.Run("query", func(t *testing.T) {
		rows, err := db.Query("select 1 as a, 'bar'::text as bar where false")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if len(cols) != 2 {
			t.Fatalf("unexpected number of columns %d in response to an empty query", len(cols))
		}
		if rows.Next() {
			t.Fatal("unexpected row")
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if cols[0] != "a" || cols[1] != "bar" {
			t.Fatalf("unexpected Columns result %v", cols)
		}
	})

	t.Run("prepared", func(t *testing.T) {
		rows, err := pqtest.Prepare(t, db, "select $1::int as a, text 'bar' AS bar where false").Query(1)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if len(cols) != 2 {
			t.Fatalf("unexpected number of columns %d in response to an empty query", len(cols))
		}
		if rows.Next() {
			t.Fatal("unexpected row")
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if cols[0] != "a" || cols[1] != "bar" {
			t.Fatalf("unexpected Columns result %v", cols)
		}
	})
}

func TestEncodeDecode(t *testing.T) {
	db := pqtest.MustDB(t)

	type h struct {
		got1                   []byte
		got2                   string
		got3                   sql.NullInt64
		got4                   time.Time
		got5, got6, got7, got8 any
	}
	have := h{got3: sql.NullInt64{Valid: true}}
	err := db.QueryRow(`
		select
			E'\\000\\001\\002'::bytea,
			'foobar'::text,
			NULL::integer,
			'2000-1-1 01:02:03.04-7'::timestamptz,
			0::boolean,
			123,
			-321,
			3.14::float8
		where
			E'\\000\\001\\002'::bytea = $1 and
			'foobar'::text = $2 and
			$3::integer is NULL
	`, []byte{0, 1, 2}, "foobar", nil).Scan(
		&have.got1, &have.got2, &have.got3, &have.got4, &have.got5, &have.got6, &have.got7, &have.got8,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := h{
		got1: []byte{0, 1, 2},
		got2: "foobar",
		got3: sql.NullInt64{},
		got4: time.Date(2000, 1, 1, 8, 2, 3, 40000000, time.UTC),
		got5: false,
		got6: int64(123),
		got7: int64(-321),
		got8: 3.14,
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %+v\nwant: %+v", have, want)
	}
}

func TestNoData(t *testing.T) {
	db := pqtest.MustDB(t)

	rows, err := pqtest.Prepare(t, db, "select 1 where true = false").Query()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		t.Fatal("unexpected row")
	}

	_, err = db.Query("select * from nonexistenttable where age=$1", 20)
	if err == nil {
		t.Fatal("Should have raised an error on non existent table")
	}

	_, err = db.Query("select * from nonexistenttable")
	if err == nil {
		t.Fatal("Should have raised an error on non existent table")
	}
}

func TestErrorDuringStartup(t *testing.T) {
	// TODO: fails with wrong error:
	//   wrong error code "protocol_violation": pq: "trust" authentication failed
	// May be an issue in how pgbouncer is configured, or just that pgbouncer
	// sends a different error.
	pqtest.SkipPgbouncer(t)

	// TODO: this one also:
	//   wrong error code "internal_error": pq: unable to get session context
	pqtest.SkipPgpool(t)

	t.Parallel()

	// Don't use the normal connection setup, this is intended to blow up in the
	// startup packet from a non-existent user.
	_, err := pqtest.DB(t, "user=thisuserreallydoesntexist")
	mustAs(t, err, pqerror.InvalidAuthorizationSpecification, pqerror.InvalidPassword)
}

type testConn struct {
	closed bool
	net.Conn
}

func (c *testConn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

type testDialer struct {
	conns []*testConn
}

func (d *testDialer) Dial(ntw, addr string) (net.Conn, error) {
	c, err := net.Dial(ntw, addr)
	if err != nil {
		return nil, err
	}
	tc := &testConn{Conn: c}
	d.conns = append(d.conns, tc)
	return tc, nil
}

func (d *testDialer) DialTimeout(ntw, addr string, timeout time.Duration) (net.Conn, error) {
	c, err := net.DialTimeout(ntw, addr, timeout)
	if err != nil {
		return nil, err
	}
	tc := &testConn{Conn: c}
	d.conns = append(d.conns, tc)
	return tc, nil
}

func TestErrorDuringStartupClosesConn(t *testing.T) {
	// Don't use the normal connection setup, this is intended to
	// blow up in the startup packet from a non-existent user.
	var d testDialer
	c, err := DialOpen(&d, pqtest.DSN("user=thisuserreallydoesntexist"))
	if err == nil {
		c.Close()
		t.Fatal("expected dial error")
	}
	if len(d.conns) != 1 {
		t.Fatalf("got len(d.conns) = %d, want = %d", len(d.conns), 1)
	}
	if !d.conns[0].closed {
		t.Error("connection leaked")
	}
}

func TestBadConn(t *testing.T) {
	t.Parallel()
	for _, tt := range []error{io.EOF, &Error{Severity: pqerror.SeverityFatal}} {
		t.Run(fmt.Sprintf("%s", tt), func(t *testing.T) {
			var cn conn
			err := cn.handleError(tt)
			if err != driver.ErrBadConn {
				t.Fatalf("expected driver.ErrBadConn, got: %#v", err)
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Fatalf("expected driver.ErrBadConn, got %#v", err)
			}
		})
	}
}

func TestConnClose(t *testing.T) {
	// Ensure the underlying connection can be closed with Close after an error.
	t.Run("CloseBadConn", func(t *testing.T) {
		host := os.Getenv("PGHOST")
		if host == "" {
			host = "localhost"
		}
		if host[0] == '/' {
			t.Skip("cannot test bad connection close with a Unix-domain PGHOST")
		}
		port := os.Getenv("PGPORT")
		if port == "" {
			port = "5432"
		}
		nc, err := net.Dial("tcp", host+":"+port)
		if err != nil {
			t.Fatal(err)
		}
		cn := conn{c: nc}
		cn.handleError(io.EOF)

		// Verify we can write before closing and then close.
		if _, err := nc.Write(nil); err != nil {
			t.Fatal(err)
		}
		if err := cn.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify write after closing fails.
		const errClosing = "use of closed"
		_, err = nc.Write(nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), errClosing) {
			t.Fatalf("expected %s error, got %s", errClosing, err)
		}
		// Verify second close fails.
		err = cn.Close()
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), errClosing) {
			t.Fatalf("expected %s error, got %s", errClosing, err)
		}
	})
}

func TestErrorOnExec(t *testing.T) {
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table foo(f1 int primary key)`)

	_, err := tx.Exec("insert into foo values (0), (0)")
	mustAs(t, err, pqerror.UniqueViolation)
}

func TestErrorOnQuery(t *testing.T) {
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table foo(f1 int primary key)`)

	_, err := tx.Query("insert into foo values (0), (0)")
	mustAs(t, err, pqerror.UniqueViolation)
}

func TestErrorOnQueryRowSimpleQuery(t *testing.T) {
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table foo(f1 int primary key)`)

	var v int
	err := tx.QueryRow("insert into foo values (0), (0)").Scan(&v)
	mustAs(t, err, pqerror.UniqueViolation)
}

// Test the QueryRow bug workarounds in stmt.exec() and simpleQuery()
func TestQueryRowBugWorkaround(t *testing.T) {
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, "create temp table notnulltemp (a varchar(10) not null)")

	var a string
	err := db.QueryRow("insert into notnulltemp(a) values($1) returning a", nil).Scan(&a)
	mustAs(t, err, pqerror.NotNullViolation)

	// Test workaround in simpleQuery()
	tx := pqtest.Begin(t, db)

	pqtest.Exec(t, tx, `set local check_function_bodies to false`)
	pqtest.Exec(t, tx, `
		create or replace function bad_function()
		returns integer
		-- hack to prevent the function from being inlined
		set check_function_bodies to true
		as $$
			select text 'bad'
		$$ language sql
	`)

	err = tx.QueryRow("select * from bad_function()").Scan(&a)
	mustAs(t, err, pqerror.InvalidFunctionDefinition)

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("unexpected error %s in Rollback", err)
	}

	// Also test that simpleQuery()'s workaround works when the query fails
	// after a row has been received.
	rows, err := db.Query(`
		select (select generate_series(1, ss.i))
		from (select gs.i
			from generate_series(1, 2) gs(i)
			order by gs.i limit 2) ss
	`)
	if err != nil {
		t.Fatalf("query failed: %s", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected at least one result row; got %s", rows.Err())
	}
	var i int
	err = rows.Scan(&i)
	if err != nil {
		t.Fatalf("rows.Scan() failed: %s", err)
	}
	if i != 1 {
		t.Fatalf("unexpected value for i: %d", i)
	}
	if rows.Next() {
		t.Fatalf("unexpected row")
	}
	mustAs(t, rows.Err(), pqerror.CardinalityViolation)
}

func TestSimpleQuery(t *testing.T) {
	have := pqtest.QueryRow[int](t, pqtest.MustDB(t), `select 1`)
	want := map[string]int{"?column?": 1}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

// Make sure SimpleQuery doesn't panic if there is no query response. See #1059
// and #1173
func TestSimpleQueryWithoutResponse(t *testing.T) {
	t.Parallel()

	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		f.Startup(cn, nil)
		for {
			code, _, ok := f.ReadMsg(cn)
			if !ok {
				return
			}
			switch code {
			case proto.Query:
				// Make sure we DON'T send this
				//f.WriteMsg(cn, proto.EmptyQueryResponse, "")
				f.WriteMsg(cn, proto.ReadyForQuery, "I")
			case proto.Terminate:
				cn.Close()
				return
			}
		}
	})
	defer f.Close()

	err := pqtest.MustDB(t, f.DSN()).Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindError(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, `create temp table tbl (i integer)`)

	_, err := db.Query(`select * from tbl where i=$1`, "hhh")
	if err == nil {
		t.Fatal("expected an error")
	}

	have := pqtest.QueryRow[int](t, db, `select * from tbl where i=$1`, 1)
	var want map[string]int
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestParseErrorInExtendedQuery(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Query("parse_error $1", 1)
	mustAs(t, err, pqerror.SyntaxError)

	rows, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

// TestReturning tests that an INSERT query using the RETURNING clause returns a row.
func TestReturning(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, `create temp table tbl (did integer default 0, dname text)`)

	have := pqtest.Query[int](t, db, `insert into tbl (did, dname) values (default, 'a'), (5, 'b') returning did;`)
	want := []map[string]int{{"did": 0}, {"did": 5}}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestExecNoData(t *testing.T) { // See #186
	t.Parallel()
	db := pqtest.MustDB(t)

	// Exec() a query which returns results
	pqtest.Exec(t, db, "values (1), (2), (3)")
	pqtest.Exec(t, db, "values ($1), ($2), ($3)", 1, 2, 3)

	// Query() a query which doesn't return any results
	tx := pqtest.Begin(t, db)

	have := pqtest.QueryRow[any](t, tx, `create temp table foo(f1 int)`)
	var want map[string]any
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}

	// Get NoData from a parameterized query.
	pqtest.Exec(t, tx, `create rule nodata as on insert to foo do instead nothing`)
	have = pqtest.QueryRow[any](t, tx, `insert into foo values ($1)`, 1)
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

// Test that any CommandComplete messages sent before the query results are
// ignored.
func TestIssue282(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	have := pqtest.QueryRow[string](t, db, `
	 	set search_path to pg_catalog;
	 	set local search_path to pg_catalog;
	 	show search_path`)
	want := map[string]string{"search_path": "pg_catalog"}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestFloatPrecision(t *testing.T) { // See #196
	t.Parallel()
	db := pqtest.MustDB(t)

	have := pqtest.Query[bool](t, db, `select '0.10000122'::float4 = $1 as f4, '35.03554004971999'::float8 = $2 as f8`,
		float32(0.10000122), float64(35.03554004971999))[0]
	want := map[string]bool{"f4": true, "f8": true}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}

	type h struct {
		F4          float32
		F8, F4Short float64
	}
	var have2 h
	err := db.QueryRow("select '0.10000122'::float4, '35.03554004971999'::float8, '1.2'::float4").
		Scan(&have2.F4, &have2.F8, &have2.F4Short)
	if err != nil {
		t.Fatal(err)
	}
	want2 := h{0.10000122, 35.03554004971999, 1.2}
	if !reflect.DeepEqual(have2, want2) {
		t.Errorf("\nhave: %#v\nwant: %#v", have2, want2)
	}
}

func TestParseComplete(t *testing.T) {
	tests := []struct {
		in, want string
		wantRows int64
		wantErr  string
	}{
		{"ALTER TABLE", "ALTER TABLE", 0, ``},
		{"INSERT 0 1", "INSERT", 1, ``},
		{"UPDATE 100", "UPDATE", 100, ``},
		{"SELECT 100", "SELECT", 100, ``},
		{"FETCH 100", "FETCH", 100, ``},
		{"COPY", "COPY", 0, ``},                                           // allow COPY (and others) without row count
		{"UNKNOWNCOMMANDTAG", "UNKNOWNCOMMANDTAG", 0, ``},                 // don't fail on command tags we don't recognize
		{"INSERT 1", "", 0, `pq: unexpected INSERT command tag INSERT 1`}, // missing oid
		{"UPDATE 0 1", "", 0, `pq: could not parse commandTag: strconv.ParseInt: parsing "0 1": invalid syntax`}, // too many numbers
		{"SELECT foo", "", 0, `pq: could not parse commandTag: strconv.ParseInt: parsing "foo": invalid syntax`}, // invalid row count
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			res, have, err := (&conn{}).parseComplete(tt.in)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			if tt.wantErr != "" {
				return
			}
			if have != tt.want {
				t.Fatalf("\nhave: %q\nwant: %q", have, tt.want)
			}

			haveRows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if haveRows != tt.wantRows {
				t.Fatalf("\nhave: %q\nwant: %q", haveRows, tt.wantRows)
			}
		})
	}
}

func TestNullAfterNonNull(t *testing.T) {
	t.Parallel()
	have := pqtest.Query[sql.NullInt64](t, pqtest.MustDB(t), `select 9::integer union select NULL::integer`)
	want := []map[string]sql.NullInt64{{"int4": {Int64: 9, Valid: true}}, {"int4": {}}}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func Test64BitErrorChecking(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatal("panic due to 0xFFFFFFFF != -1 when int is 64 bits")
		}
	}()

	have := pqtest.Query[any](t, pqtest.MustDB(t), `select * from (values (0::integer, NULL::text), (1, 'test string')) as t`)
	want := []map[string]any{{"column1": int64(0), "column2": any(nil)}, {"column1": int64(1), "column2": "test string"}}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestCommit(t *testing.T) {
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, "create temp table tbl (a int)")

	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `insert into tbl values (1)`)

	err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	var i int
	err = db.QueryRow(`select * from tbl`).Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}
}

func TestRowsResultTag(t *testing.T) {
	tests := []struct {
		query string
		tag   string
		ra    int64
	}{
		{"CREATE TEMP TABLE temp (a int)", "CREATE TABLE", 0},
		{"INSERT INTO temp VALUES (1), (2)", "INSERT", 2},
		{"SELECT 1", "", 0},
		// A SELECT anywhere should take precedent.
		{"SELECT 1; INSERT INTO temp VALUES (1), (2)", "", 0},
		{"INSERT INTO temp VALUES (1), (2); SELECT 1", "", 0},
		// Multiple statements that don't return rows should return the last tag.
		{"CREATE TEMP TABLE t (a int); DROP TABLE t", "DROP TABLE", 0},
		// Ensure a rows-returning query in any position among various tags-returing
		// statements will prefer the rows.
		{"SELECT 1; CREATE TEMP TABLE t (a int); DROP TABLE t", "", 0},
		{"CREATE TEMP TABLE t (a int); SELECT 1; DROP TABLE t", "", 0},
		{"CREATE TEMP TABLE t (a int); DROP TABLE t; SELECT 1", "", 0},
	}

	type ResultTag interface {
		Result() driver.Result
		Tag() string
	}

	conn, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q := conn.(driver.QueryerContext)

	for _, tt := range tests {
		rows, err := q.QueryContext(context.Background(), tt.query, nil)
		if err != nil {
			t.Fatalf("%s: %s", tt.query, err)
		}

		r := rows.(ResultTag)
		if tag := r.Tag(); tag != tt.tag {
			t.Fatalf("%s: unexpected tag %q", tt.query, tag)
		}
		res := r.Result()
		if ra, _ := res.RowsAffected(); ra != tt.ra {
			t.Fatalf("%s: unexpected rows affected: %d", tt.query, ra)
		}
		rows.Close()
	}
}

func TestMultipleResult(t *testing.T) {
	t.Parallel()

	have := pqtest.Query[any](t, pqtest.MustDB(t), `
		begin;
			select 123 as i, 'str' as s;
			values (4.56::float8, 7.89::float8), (9.8::float8, 8.7::float8);
			select '\x6109'::bytea;
		commit;
	`)
	want := []map[string]any{
		{"i": int64(123), "s": "str"},
		{"(rs 1) column1": 4.56, "(rs 1) column2": 7.89},
		{"(rs 1) column1": 9.8, "(rs 1) column2": 8.7},
		{"(rs 2) bytea": []byte{0x61, 0x09}},
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestMultipleResultEmpty(t *testing.T) {
	t.Parallel()

	have := pqtest.QueryRow[int](t, pqtest.MustDB(t), `select 1 where false; select 2`)
	want := map[string]int{"(rs 1) ?column?": 2}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestMultipleSimpleQuery(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	have := pqtest.Query[any](t, db, `select 1; set time zone default; select 2; select 3`)
	want := []map[string]any{
		{"?column?": int64(1)},
		{"(rs 1) ?column?": int64(2)},
		{"(rs 2) ?column?": int64(3)},
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}

	// Make sure that if we ignore a result we can still query.
	have = pqtest.Query[any](t, db, `select 4; select 5`)
	want = []map[string]any{
		{"?column?": int64(4)},
		{"(rs 1) ?column?": int64(5)},
	}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestConnPrepareContext(t *testing.T) {
	tests := []struct {
		sql string
		err error
		ctx func() (context.Context, context.CancelFunc)
	}{
		{"select 1", nil, func() (context.Context, context.CancelFunc) {
			return context.Background(), nil
		}},
		{"select 1", context.DeadlineExceeded, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), -time.Minute)
		}},
		{"select 1", nil, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute)
		}},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx, cancel := tt.ctx()
			if cancel != nil {
				defer cancel()
			}
			_, err := pqtest.MustDB(t).PrepareContext(ctx, tt.sql)
			switch {
			case (err != nil) != (tt.err != nil):
				t.Fatalf("unexpected nil err got = %v, expected = %v", err, tt.err)
			case (err != nil && tt.err != nil) && (err.Error() != tt.err.Error()):
				t.Errorf("\nhave: %v\nwant: %v", err.Error(), tt.err.Error())
			}
		})
	}
}

func TestStmtQueryContext(t *testing.T) {
	tests := []struct {
		sql     string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr string
	}{
		{"select pg_sleep(1)",
			func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			},
			`pq: canceling statement due to user request (57014)`,
		},
		{"select pg_sleep(0.05)",
			func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			``,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			if !pqtest.Pgpool() {
				t.Parallel()
			}

			db := pqtest.MustDB(t)

			ctx, cancel := tt.ctx()
			defer cancel()

			stmt, err := db.PrepareContext(ctx, tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.QueryContext(ctx)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestStmtExecContext(t *testing.T) {
	tests := []struct {
		sql     string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr string
	}{
		{"select pg_sleep(1)",
			func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			},
			`pq: canceling statement due to user request (57014)`,
		},
		{"select pg_sleep(0.05)",
			func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			``,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			if !pqtest.Pgpool() {
				t.Parallel()
			}

			db := pqtest.MustDB(t)

			ctx, cancel := tt.ctx()
			defer cancel()

			stmt, err := db.PrepareContext(ctx, tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.ExecContext(ctx)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestContextCancelExec(t *testing.T) {
	t.Parallel()
	pqtest.SkipPgpool(t) // TODO: flaky in CI
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.ExecContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	_, err := db.ExecContext(ctx, "select pg_sleep(1)")
	mustAs(t, err, pqerror.QueryCanceled)

	// Context is already canceled, so error should come before execution.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < 100; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := db.ExecContext(ctx, "select 1"); err != nil {
				t.Fatal(err)
			}
		}()

		if _, err := db.Exec("select 1"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContextCancelQuery(t *testing.T) {
	t.Parallel()
	pqtest.SkipPgpool(t) // TODO: flaky in CI
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.QueryContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	_, err := db.QueryContext(ctx, "select pg_sleep(1)")
	mustAs(t, err, pqerror.QueryCanceled)

	// Context is already canceled, so error should come before execution.
	if _, err := db.QueryContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < 100; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			rows, err := db.QueryContext(ctx, "select 1")
			cancel()
			if err != nil {
				t.Fatal(err)
			} else if err := rows.Close(); err != nil && err != driver.ErrBadConn && err != context.Canceled {
				t.Fatal(err)
			}
		}()

		if rows, err := db.Query("select 1"); err != nil {
			t.Fatal(err)
		} else if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Failed query in QueryContext doesn't lead to a goroutine leak.
func TestIssue617(t *testing.T) {
	db := pqtest.MustDB(t)

	const N = 10

	numGoroutineStart := runtime.NumGoroutine()
	for i := 0; i < N; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := db.QueryContext(ctx, `SELECT * FROM DOESNOTEXIST`)
			mustAs(t, err, pqerror.UndefinedTable)
		}()
	}

	// Give time for goroutines to terminate
	delayTime := time.Millisecond * 50
	waitTime := time.Second
	iterations := int(waitTime / delayTime)

	var numGoroutineFinish int
	for i := 0; i < iterations; i++ {
		time.Sleep(delayTime)

		numGoroutineFinish = runtime.NumGoroutine()

		// We use N/2 and not N because the GC and other actors may increase or
		// decrease the number of goroutines.
		if numGoroutineFinish-numGoroutineStart < N/2 {
			return
		}
	}

	t.Errorf("goroutine leak detected, was %d, now %d", numGoroutineStart, numGoroutineFinish)
}

func TestContextCancelBegin(t *testing.T) {
	t.Parallel()
	pqtest.SkipPgpool(t) // TODO: flaky in CI
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Delay execution for just a bit until tx.Exec has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	_, err = tx.Exec("select pg_sleep(1)")
	mustAs(t, err, pqerror.QueryCanceled)

	// Transaction is canceled, so expect an error.
	if _, err := tx.Query("select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err != sql.ErrTxDone {
		t.Fatalf("unexpected error: %s", err)
	}

	// Context is canceled, so cannot begin a transaction.
	if _, err := db.BeginTx(ctx, nil); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < 100; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			tx, err := db.BeginTx(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			err = tx.Rollback()
			if err != nil && err != sql.ErrTxDone && err != driver.ErrBadConn && err != context.Canceled {
				mustAs(t, err, pqerror.QueryCanceled)
			}
		}()

		tx := pqtest.Begin(t, db)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTxOptions(t *testing.T) {
	t.Parallel()

	// TODO: fails with:
	// go18_test.go:296: wrong isolation level: read committed != read uncommitted
	// go18_test.go:296: wrong isolation level: read committed != repeatable read
	// go18_test.go:296: wrong isolation level: read committed != serializable
	// go18_test.go:306: read/[write,only] not set: true != off for level serializable
	// go18_test.go:296: wrong isolation level: read committed != serializable
	pqtest.SkipPgpool(t)

	db := pqtest.MustDB(t)
	ctx := context.Background()

	tests := []struct {
		level     sql.IsolationLevel
		isolation string
	}{
		{sql.LevelDefault, ""},
		{sql.LevelReadUncommitted, "read uncommitted"},
		{sql.LevelReadCommitted, "read committed"},
		{sql.LevelRepeatableRead, "repeatable read"},
		{sql.LevelSerializable, "serializable"},
	}

	for _, tt := range tests {
		for _, ro := range []bool{true, false} {
			tx, err := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: tt.level,
				ReadOnly:  ro,
			})
			if err != nil {
				t.Fatal(err)
			}

			var isolation string
			err = tx.QueryRow("select current_setting('transaction_isolation')").Scan(&isolation)
			if err != nil {
				t.Fatal(err)
			}

			if tt.isolation != "" && isolation != tt.isolation {
				t.Errorf("wrong isolation level: %s != %s", isolation, tt.isolation)
			}

			var isRO string
			err = tx.QueryRow("select current_setting('transaction_read_only')").Scan(&isRO)
			if err != nil {
				t.Fatal(err)
			}

			if ro != (isRO == "on") {
				t.Errorf("read/[write,only] not set: %t != %s for level %s",
					ro, isRO, tt.isolation)
			}

			tx.Rollback()
		}
	}

	_, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelLinearizable,
	})
	if err == nil {
		t.Fatal("expected LevelLinearizable to fail")
	}
	if !strings.Contains(err.Error(), "isolation level not supported") {
		t.Errorf("Expected error to mention isolation level, got %q", err)
	}
}

func TestPing(t *testing.T) {
	t.Parallel()
	pqtest.SkipPgpool(t) // TODO: hangs forever?

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := pqtest.MustDB(t)

	if err := db.PingContext(ctx); err != nil {
		t.Fatal("expected Ping to succeed")
	}

	// Grab a connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Start a transaction and read backend pid of our connection.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.Query("SELECT pg_backend_pid()")
	if err != nil {
		t.Fatal(err)
	}

	// read the pid from result
	var pid int
	for rows.Next() {
		if err := rows.Scan(&pid); err != nil {
			t.Fatal(err)
		}
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	// Fail the transaction and make sure we can still ping.
	if _, err := tx.Query("INVALID SQL"); err == nil {
		t.Fatal("expected error")
	}
	if err := conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// kill the process which handles our connection and test if the ping fails
	if _, err := db.Exec("SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	if err := conn.PingContext(ctx); err != driver.ErrBadConn {
		t.Fatalf("expected error %s, instead got %s", driver.ErrBadConn, err)
	}
}

func TestCommitInFailedTransactionWithCancelContext(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query("SELECT error")
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
	err = tx.Commit()
	if err != ErrInFailedTransaction {
		t.Fatalf("expected ErrInFailedTransaction; got %#v", err)
	}
}

func TestAuth(t *testing.T) {
	tests := []struct {
		buf     readBuf
		wantErr string
	}{
		{readBuf{0, 0, 0, 9}, `pq: unsupported authentication method: SSPI (9)`},
		{readBuf{0, 0, 0, 99}, `unknown authentication response: <unknown> (99)`},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Run("unsupported auth", func(t *testing.T) {
				err := (&conn{}).auth(&tt.buf, Config{})
				if !pqtest.ErrorContains(err, tt.wantErr) {
					t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
				}
			})
		})
	}

	t.Run("end to end", func(t *testing.T) {
		pqtest.SkipPgbouncer(t) // TODO: need to properly set up auth
		pqtest.SkipPgpool(t)    // TODO: need to properly set up auth

		tests := []struct {
			conn, wantErr string
		}{
			{"user=pqgomd5", `password authentication failed for user "pqgomd5"`},
			{"user=pqgopassword", `empty password returned by client`},
			{"user=pqgoscram", `password authentication failed for user "pqgoscram"`},

			{"user=pqgomd5 password=wrong", `password authentication failed for user "pqgomd5"`},
			{"user=pqgopassword password=wrong", `password authentication failed for user "pqgopassword"`},
			{"user=pqgoscram    password=wrong", `password authentication failed for user "pqgoscram"`},

			{"user=pqgomd5 password=wordpass", ``},
			{"user=pqgopassword password=wordpass", ``},
			{"user=pqgoscram password=wordpass", ``},

			{"user=pqgounknown password=wordpass", `role "pqgounknown" does not exist`},
		}

		for _, tt := range tests {
			t.Run(tt.conn, func(t *testing.T) {
				_, err := pqtest.DB(t, tt.conn)
				if !pqtest.ErrorContains(err, tt.wantErr) {
					t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
				}
			})
		}
	})
}

func TestUint64(t *testing.T) {
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, `create temp table tbl (n numeric)`)
	pqtest.Exec(t, db, `insert into tbl values ($1)`, uint64(math.MaxUint64))

	have := pqtest.QueryRow[uint64](t, db, `select n from tbl`)
	want := map[string]uint64{"n": math.MaxUint64}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestBytea(t *testing.T) {
	tests := []struct {
		in   any
		want string
	}{
		{[]byte{0x00, 0x01, 0x02, 0xff},
			`[]map[string][]uint8{map[string][]uint8{"b":[]uint8{0x0, 0x1, 0x2, 0xff}}}`},
		{[]byte(nil),
			`[]map[string][]uint8{map[string][]uint8{"b":[]uint8(nil)}}`},
		{json.RawMessage(`{"key":"value"}`),
			`[]map[string][]uint8{map[string][]uint8{"b":[]uint8{0x7b, 0x22, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d}}}`},
		{pqtest.Ptr(pqtest.Ptr([]byte{0x00, 0x01, 0x02, 0xff})),
			`[]map[string][]uint8{map[string][]uint8{"b":[]uint8{0x0, 0x1, 0x2, 0xff}}}`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			db := pqtest.MustDB(t)
			pqtest.Exec(t, db, `create temp table tbl (b bytea)`)
			pqtest.Exec(t, db, `insert into tbl values ($1)`, &tt.in)
			rows := pqtest.Query[[]byte](t, db, `select b from tbl`)
			if have := fmt.Sprintf("%#v", rows); have != tt.want {
				t.Fatalf("\nhave: %s\nwant: %s", have, tt.want)
			}
		})
	}
}

func TestJSONRawMessage(t *testing.T) {
	db := pqtest.MustDB(t)

	pqtest.Exec(t, db, `create temp table tbl (j json)`)

	// Test json.RawMessage (a named []byte type) is correctly stored as JSON,
	// not converted to a PostgreSQL array. This was a bug in CheckNamedValue
	// where named byte slice types would hit the reflect.Slice case and get
	// incorrectly converted to a PostgreSQL array.
	data := json.RawMessage(`{"key":"value"}`)
	pqtest.Exec(t, db, `insert into tbl values ($1)`, data)

	have := pqtest.QueryRow[json.RawMessage](t, db, `select j from tbl`)
	want := map[string]json.RawMessage{"j": data}
	if !reflect.DeepEqual(have, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", have, want)
	}
}

func TestPreProtocolError(t *testing.T) {
	tests := []struct {
		msg     string
		wantErr string
	}{
		{"could not fork new process for connection: Resource temporarily unavailable\n",
			"server error: could not fork new process for connection: Resource temporarily unavailable"},
		{"sorry, too many clients already\n",
			"server error: sorry, too many clients already"},
		{"out of memory\n",
			"server error: out of memory"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
				f.ReadStartup(cn)
				// Send pre-protocol error: 'E' followed by plain text; this
				// simulates what PostgreSQL sends when it can't fork.
				cn.Write(append([]byte{'E'}, tt.msg...))
				cn.Close()
			})
			defer f.Close()

			_, err := pqtest.DB(t, f.DSN())
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

// reading from circularConn yields content[:prefixLen] once, followed by
// content[prefixLen:] over and over again. It never returns EOF.
type circularConn struct {
	content   string
	prefixLen int
	pos       int
	net.Conn  // for all other net.Conn methods that will never be called
}

func (r *circularConn) Close() error                { return nil }
func (r *circularConn) Write(b []byte) (int, error) { return len(b), nil }
func (r *circularConn) Read(b []byte) (int, error) {
	n := copy(b, r.content[r.pos:])
	r.pos += n
	if r.pos >= len(r.content) {
		r.pos = r.prefixLen
	}
	return n, nil
}
func fakeConn(prefixLen int, content string) *conn {
	c := &circularConn{content: content, prefixLen: prefixLen}
	return &conn{buf: bufio.NewReader(c), c: c}
}

var seriesRowData = func() string {
	var buf bytes.Buffer
	for i := 1; i <= 100; i++ {
		digits := byte(2)
		if i >= 100 {
			digits = 3
		} else if i < 10 {
			digits = 1
		}
		buf.WriteString("D\x00\x00\x00")
		buf.WriteByte(10 + digits)
		buf.WriteString("\x00\x01\x00\x00\x00")
		buf.WriteByte(digits)
		buf.WriteString(strconv.Itoa(i))
	}
	return buf.String()
}()

func BenchmarkSelect(b *testing.B) {
	run := func(b *testing.B, result any, query string) {
		db := pqtest.MustDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(query)
			if err != nil {
				b.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(result)
				if err != nil {
					b.Fatalf("failed to scan: %v", err)
				}
			}
		}
	}

	b.Run("string", func(b *testing.B) {
		run(b, pqtest.Ptr(""), `select '`+strings.Repeat("0123456789", 10)+`'`)
	})
	b.Run("int", func(b *testing.B) {
		run(b, pqtest.Ptr(0), `select generate_series(1, 100)`)
	})
	b.Run("without read", func(b *testing.B) {
		db := pqtest.MustDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("select generate_series(1, 50000)")
			if err != nil {
				b.Fatal(err)
			}
			err = rows.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Same as above, but takes out some of the factors we can't control such as
	// network communication, so the numbers are less noisy.
	b.Run("mock", func(b *testing.B) {
		run := func(b *testing.B, c *conn, query string) {
			stmt, err := c.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			rows, err := stmt.(driver.StmtQueryContext).QueryContext(context.Background(), nil)
			if err != nil {
				b.Fatal(err)
			}
			defer rows.Close()
			var dest [1]driver.Value
			for {
				if err := rows.Next(dest[:]); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}

		b.Run("string", func(b *testing.B) {
			c := fakeConn(0, ""+
				"1\x00\x00\x00\x04"+
				"t\x00\x00\x00\x06\x00\x00"+
				"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00"+
				"Z\x00\x00\x00\x05I"+
				"2\x00\x00\x00\x04"+
				"D\x00\x00\x00n\x00\x01\x00\x00\x00d0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"+
				"C\x00\x00\x00\rSELECT 1\x00"+
				"Z\x00\x00\x00\x05I"+
				"3\x00\x00\x00\x04"+
				"Z\x00\x00\x00\x05I")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(b, c, `select '`+strings.Repeat("0123456789", 10)+`'`)
			}
		})
		b.Run("int", func(b *testing.B) {
			c := fakeConn(0, ""+
				"1\x00\x00\x00\x04"+
				"t\x00\x00\x00\x06\x00\x00"+
				"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00"+
				"Z\x00\x00\x00\x05I"+
				"2\x00\x00\x00\x04"+
				seriesRowData+
				"C\x00\x00\x00\x0fSELECT 100\x00"+
				"Z\x00\x00\x00\x05I"+
				"3\x00\x00\x00\x04"+
				"Z\x00\x00\x00\x05I")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(b, c, `select generate_series(1, 100)`)
			}
		})
	})
}

func BenchmarkPreparedSelect(b *testing.B) {
	run := func(b *testing.B, result any, query string) {
		stmt := pqtest.Prepare(b, pqtest.MustDB(b), query).Stmt
		defer stmt.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := stmt.Query()
			if err != nil {
				b.Fatal(err)
			}
			if !rows.Next() {
				rows.Close()
				b.Fatal("no rows")
			}
			defer rows.Close()
			for rows.Next() {
				err = rows.Scan(&result)
				if err != nil {
					b.Fatalf("failed to scan: %v", err)
				}
			}
		}
	}

	b.Run("string", func(b *testing.B) {
		run(b, pqtest.Ptr(""), `select '`+strings.Repeat("0123456789", 10)+`'`)
	})
	b.Run("int", func(b *testing.B) {
		run(b, pqtest.Ptr(0), `select generate_series(1, 100)`)
	})

	// Same as above, but takes out some of the factors we can't control such as
	// network communication, so the numbers are less noisy.
	b.Run("mock", func(b *testing.B) {
		run := func(b *testing.B, c *conn, stmt driver.Stmt) {
			rows, err := stmt.(driver.StmtQueryContext).QueryContext(context.Background(), nil)
			if err != nil {
				b.Fatal(err)
			}
			defer rows.Close()
			var dest [1]driver.Value
			for {
				if err := rows.Next(dest[:]); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}

		b.Run("string", func(b *testing.B) {
			resp := "1\x00\x00\x00\x04" +
				"t\x00\x00\x00\x06\x00\x00" +
				"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
				"Z\x00\x00\x00\x05I"
			c := fakeConn(len(resp), resp+
				"2\x00\x00\x00\x04"+
				"D\x00\x00\x00n\x00\x01\x00\x00\x00d0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"+
				"C\x00\x00\x00\rSELECT 1\x00"+
				"Z\x00\x00\x00\x05I")
			stmt, err := c.Prepare(`select '` + strings.Repeat("0123456789", 10) + `'`)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(b, c, stmt)
			}
		})
		b.Run("int", func(b *testing.B) {
			resp := "1\x00\x00\x00\x04" +
				"t\x00\x00\x00\x06\x00\x00" +
				"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
				"Z\x00\x00\x00\x05I"
			c := fakeConn(len(resp), resp+
				"2\x00\x00\x00\x04"+
				seriesRowData+
				"C\x00\x00\x00\x0fSELECT 100\x00"+
				"Z\x00\x00\x00\x05I")
			stmt, err := c.Prepare(`select generate_series(1, 100)`)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(b, c, stmt)
			}
		})
	})
}
