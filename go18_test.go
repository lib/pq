// +build go1.8

package pq

import (
	"context"
	"database/sql"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMultipleSimpleQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	rows, err := db.Query("select 1; set time zone default; select 2; select 3")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var i int
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expected 1, got %d", i)
		}
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expected 2, got %d", i)
		}
	}

	// Make sure that if we ignore a result we can still query.

	rows, err = db.Query("select 4; select 5")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 4 {
			t.Fatalf("expected 4, got %d", i)
		}
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 5 {
			t.Fatalf("expected 5, got %d", i)
		}
	}
	if rows.NextResultSet() {
		t.Fatal("unexpected result set")
	}
}

const contextRaceIterations = 100

func TestContextCancelExec(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.ExecContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "pq: canceling statement due to user request" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Context is already canceled, so error should come before execution.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < contextRaceIterations; i++ {
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
	db := openTestConn(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.QueryContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	if _, err := db.QueryContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "pq: canceling statement due to user request" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Context is already canceled, so error should come before execution.
	if _, err := db.QueryContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < contextRaceIterations; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			rows, err := db.QueryContext(ctx, "select 1")
			cancel()
			if err != nil {
				t.Fatal(err)
			} else if err := rows.Close(); err != nil {
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

// TestContextCancelQueryWhileScan checks for race conditions that arise when
// a query context is canceled while a user is calling rows.Scan(). The code
// is based on database/sql TestIssue18429.
// See https://github.com/golang/go/issues/23519
func TestContextCancelQueryWhileScan(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	const milliWait = 30
	sem := make(chan bool, 20)

	// This query seems to work well for triggering race conditions between
	// the rows.Scan() call below, and the implicit rows.Close() call triggered
	// by ctx timing out.
	sql := `SELECT (g/10)::int, json_agg(g) FROM generate_series(1, 1000) g, pg_sleep($1) GROUP BY 1;`
	var wg sync.WaitGroup
	for i := 0; i < contextRaceIterations; i++ {
		sem <- true
		wg.Add(1)
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			qwait := float64(time.Duration(rand.Intn(milliWait))*time.Millisecond) / 1000

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Intn(milliWait))*time.Millisecond)
			defer cancel()
			rows, _ := db.QueryContext(ctx, sql, qwait)
			if rows != nil {
				var d int
				var n string
				for rows.Next() {
					rows.Scan(&d, &n)
				}
				rows.Close()
			}
		}()
	}
	wg.Wait()
}

// TestIssue617 tests that a failed query in QueryContext doesn't lead to a
// goroutine leak.
func TestIssue617(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	const N = 10

	numGoroutineStart := runtime.NumGoroutine()
	for i := 0; i < N; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := db.QueryContext(ctx, `SELECT * FROM DOESNOTEXIST`)
			pqErr, _ := err.(*Error)
			// Expecting "pq: relation \"doesnotexist\" does not exist" error.
			if err == nil || pqErr == nil || pqErr.Code != "42P01" {
				t.Fatalf("expected undefined table error, got %v", err)
			}
		}()
	}
	numGoroutineFinish := runtime.NumGoroutine()

	// We use N/2 and not N because the GC and other actors may increase or
	// decrease the number of goroutines.
	if numGoroutineFinish-numGoroutineStart >= N/2 {
		t.Errorf("goroutine leak detected, was %d, now %d", numGoroutineStart, numGoroutineFinish)
	}
}

func TestContextCancelBegin(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Delay execution for just a bit until tx.Exec has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	if _, err := tx.Exec("select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "pq: canceling statement due to user request" {
		t.Fatalf("unexpected error: %s", err)
	}

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

	for i := 0; i < contextRaceIterations; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			tx, err := db.BeginTx(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			} else if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				t.Fatal(err)
			}
		}()

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTxOptions(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	ctx := context.Background()

	tests := []struct {
		level     sql.IsolationLevel
		isolation string
	}{
		{
			level:     sql.LevelDefault,
			isolation: "",
		},
		{
			level:     sql.LevelReadUncommitted,
			isolation: "read uncommitted",
		},
		{
			level:     sql.LevelReadCommitted,
			isolation: "read committed",
		},
		{
			level:     sql.LevelRepeatableRead,
			isolation: "repeatable read",
		},
		{
			level:     sql.LevelSerializable,
			isolation: "serializable",
		},
	}

	for _, test := range tests {
		for _, ro := range []bool{true, false} {
			tx, err := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: test.level,
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

			if test.isolation != "" && isolation != test.isolation {
				t.Errorf("wrong isolation level: %s != %s", isolation, test.isolation)
			}

			var isRO string
			err = tx.QueryRow("select current_setting('transaction_read_only')").Scan(&isRO)
			if err != nil {
				t.Fatal(err)
			}

			if ro != (isRO == "on") {
				t.Errorf("read/[write,only] not set: %t != %s for level %s",
					ro, isRO, test.isolation)
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
