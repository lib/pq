package pq

import (
	"database/sql/driver"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func TestCopyInError(t *testing.T) {
	tests := []struct {
		query   string
		wantErr string
	}{
		{`copy tbl (num) from stdin with binary`, `only text format supported for COPY`},
		{"-- comment\n  /* comment */  copy tbl (num) to stdout", `COPY TO is not supported`},
		{`copy syntax error`, `syntax error at or near "error" at column 13`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db := pqtest.MustDB(t)
			defer db.Close()
			tx := pqtest.Begin(t, db)

			pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

			_, err := tx.Prepare(tt.query)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			// Check that the protocol is in a valid state
			err = tx.Rollback()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCopyInErrorWrongType(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()
	tx := pqtest.Begin(t, db)

	pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

	stmt, err := tx.Prepare(`copy tbl (num) from stdin`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec("Héllö\n ☃!\r\t\\")
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if !pqtest.ErrorContains(err, `(22P02)`) {
		t.Errorf("wrong error: %v", err)
	}
}

func TestCopyInErrorOutsideTransaction(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()

	_, err := db.Prepare(`copy tbl (num) from stdin`)
	if err != errCopyNotSupportedOutsideTxn {
		t.Errorf("wrong error: %v", err)
	}
}

func TestCopyInQueryWhileCopy(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()
	tx := pqtest.Begin(t, db)

	pqtest.Exec(t, tx, `create temp table tbl (i int primary key)`)

	_, err := tx.Prepare("copy tbl (i) from stdin")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Query(`select 1`)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyInMultipleValues(t *testing.T) {
	tests := []struct {
		query string
	}{
		{`copy tbl (a, b) from stdin`},
		{`copy tbl from stdin`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db := pqtest.MustDB(t)
			defer db.Close()
			tx := pqtest.Begin(t, db)

			pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)
			stmt := pqtest.Prepare(t, tx, tt.query)

			str := strings.Repeat("#", 500)
			for i := 0; i < 500; i++ {
				_, err := stmt.Exec(int64(i), str)
				if err != nil {
					t.Fatal(err)
				}
			}

			res, err := stmt.Exec()
			if err != nil {
				t.Fatal(err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 500 {
				t.Fatalf("expected 500 rows affected, not %d", rows)
			}

			n, err := res.LastInsertId()
			if n != 0 || err == nil || err.Error() != `LastInsertId is not supported by this driver` {
				t.Errorf("n=%d; err=%v", n, err)
			}

			if err := stmt.Close(); err != nil {
				t.Fatal(err)
			}

			var num int
			err = tx.QueryRow("select count(*) from tbl").Scan(&num)
			if err != nil {
				t.Fatal(err)
			}
			if num != 500 {
				t.Fatalf("expected 500 items, not %d", num)
			}

		})
	}
}

func TestCopyInRaiseStmtTrigger(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()
	tx := pqtest.Begin(t, db)

	pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)
	pqtest.Exec(t, tx, `
		create or replace function pg_temp.temptest()
		returns trigger as
		$BODY$ begin
			raise notice 'Hello world';
			return new;
		end $BODY$
		language plpgsql
	`)
	pqtest.Exec(t, tx, `
		create trigger temptest_trigger
		before insert on tbl
		for each row execute procedure pg_temp.temptest()
	`)

	stmt, err := tx.Prepare(`copy tbl (a, b) from stdin`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.Exec(int64(1), strings.Repeat("#", 500))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}

	rows := pqtest.Query[any](t, tx, `select * from tbl`)
	want := []map[string]any{{
		"a": int64(1),
		"b": strings.Repeat("#", 500),
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
	}
}

func TestCopyInTypes(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()
	tx := pqtest.Begin(t, db)

	pqtest.Exec(t, tx, `create temp table tbl (num integer, text varchar, blob bytea, nothing varchar)`)

	stmt, err := tx.Prepare(`copy tbl (num, text, blob, nothing) from stdin`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.Exec(int64(1234567890), "Héllö\n ☃!\r\t\\", []byte{0, 255, 9, 10, 13}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}

	rows := pqtest.Query[any](t, tx, `select * from tbl`)
	want := []map[string]any{{
		"num":     int64(1234567890),
		"text":    "Héllö\n ☃!\r\t\\",
		"blob":    []byte{0, 255, 9, 10, 13},
		"nothing": nil,
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
	}
}

// Tests for connection errors in copyin.resploop()
func TestCopyInRespLoopConnectionError(t *testing.T) {
	// Executes f in a backoff loop until it doesn't return an error. If this
	// doesn't happen within duration, t.Fatal is called with the latest error.
	retry := func(t *testing.T, duration time.Duration, f func() error) {
		start := time.Now()
		next := time.Millisecond * 100
		for {
			err := f()
			if err == nil {
				return
			}
			if time.Since(start) > duration {
				t.Fatal(err)
			}
			time.Sleep(next)
			next *= 2
		}
	}

	t.Parallel()
	db := pqtest.MustDB(t)
	defer db.Close()
	tx := pqtest.Begin(t, db)

	pid := pqtest.Query[int64](t, tx, `select pg_backend_pid() as pid`)
	pqtest.Exec(t, tx, "create temp table tbl (a int)")

	stmt, err := tx.Prepare(`copy tbl (a) from stdin`)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, `select pg_terminate_backend($1)`, pid[0]["pid"])

	retry(t, time.Second*5, func() error {
		_, err = stmt.Exec()
		if err == nil {
			return fmt.Errorf("expected error")
		}
		return nil
	})
	switch pge := err.(type) {
	case *Error:
		if pge.Code.Name() != "admin_shutdown" {
			t.Fatalf("expected admin_shutdown, got %s", pge.Code.Name())
		}
	case *net.OpError:
		// ignore
	default:
		if err == driver.ErrBadConn {
			// likely an EPIPE
		} else if err == errCopyInClosed {
			// ignore
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkCopyIn(b *testing.B) {
	db := pqtest.MustDB(b)
	defer db.Close()
	tx := pqtest.Begin(b, db)

	pqtest.Exec(b, tx, `create temp table tbl (a int, b varchar)`)

	stmt, err := tx.Prepare(`copy tbl (a, b) from stdin`)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = stmt.Exec(int64(i), "hello world!")
		if err != nil {
			b.Fatal(err)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		b.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}

	rows := pqtest.Query[int](b, tx, `select count(*) from tbl`)
	if rows[0]["count"] != b.N {
		b.Fatalf("expected %d items, not %d", b.N, rows[0]["count"])
	}
}
