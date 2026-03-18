package pq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/pqerror"
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
			tx := pqtest.Begin(t, pqtest.MustDB(t))
			pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

			_, err := tx.Prepare(tt.query)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			// Check that the protocol is in a valid state
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCopyInErrorWrongType(t *testing.T) {
	t.Parallel()
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num) from stdin`)
	stmt.MustExec(t, "Héllö\n ☃!\r\t\\")
	_, err := stmt.Exec()
	mustAs(t, err, pqerror.InvalidTextRepresentation)
}

func TestCopyInErrorOutsideTransaction(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Prepare(`copy tbl (num) from stdin`)
	if err != errCopyNotSupportedOutsideTxn {
		t.Errorf("wrong error: %v", err)
	}
}

func TestCopyInQueryWhileCopy(t *testing.T) {
	t.Parallel()
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table tbl (i int primary key)`)

	pqtest.Prepare(t, tx, "copy tbl (i) from stdin")
	_, err := tx.Query(`select 1`)
	if !errors.Is(err, errQueryInProgress) {
		t.Errorf("wrong error:\nhave: %s\nwant: %s", err, errQueryInProgress)
	}
}

func TestCopyInNull(t *testing.T) {
	tests := []struct {
		null any
		copy string
	}{
		{nil, `copy tbl (i, t) from stdin`},
		{`NULL`, `copy tbl (i, t) from stdin with null 'NULL'`},
		{``, `copy tbl (i, t) from stdin with null ''`},
		{`\N`, `copy tbl (i, t) from stdin with null '\\N'`},

		// The default doesn't work as copyin.Exec() calls appendEncodedText(),
		// which escapes \N to \\N. To fix it we need to read query, see if
		// "WITH NULL" was passed, and don't escape that text (of the default of
		// \N).
		//{`\N`, `copy tbl (i, t) from stdin`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			tx := pqtest.Begin(t, pqtest.MustDB(t))

			pqtest.Exec(t, tx, `create temp table tbl (i int, t text)`)
			stmt := pqtest.Prepare(t, tx, tt.copy)
			stmt.MustExec(t, 42, "forty-two")
			stmt.MustExec(t, tt.null, tt.null)
			stmt.MustExec(t)
			stmt.MustClose(t)

			rows := pqtest.Query[any](t, tx, `select * from tbl`)
			want := []map[string]any{
				{"i": int64(42), "t": "forty-two"},
				{"i": nil, "t": nil},
			}
			if !reflect.DeepEqual(rows, want) {
				t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
			}
		})
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
			tx := pqtest.Begin(t, pqtest.MustDB(t))
			pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)

			stmt := pqtest.Prepare(t, tx, tt.query)
			for i := 0; i < 500; i++ {
				stmt.MustExec(t, int64(i), strings.Repeat("#", 500))
			}

			res := stmt.MustExec(t)
			rows, err := res.RowsAffected()
			if err != nil || rows != 500 {
				t.Fatalf("\nerr: %v\nrows: %v", err, rows)
			}

			n, err := res.LastInsertId()
			if n != 0 || err == nil || err.Error() != `LastInsertId is not supported by this driver` {
				t.Errorf("n=%d; err=%v", n, err)
			}

			stmt.MustClose(t)

			num := pqtest.Query[int](t, tx, `select count(*) from tbl`)[0]["count"]
			if num != 500 {
				t.Fatalf("expected 500 items, not %d", num)
			}
		})
	}
}

func TestCopyInRaiseStmtTrigger(t *testing.T) {
	t.Parallel()
	tx := pqtest.Begin(t, pqtest.MustDB(t))
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

	stmt := pqtest.Prepare(t, tx, `copy tbl (a, b) from stdin`)
	stmt.MustExec(t, int64(1), strings.Repeat("#", 500))
	stmt.MustExec(t)
	stmt.MustClose(t)

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
	tx := pqtest.Begin(t, pqtest.MustDB(t))
	pqtest.Exec(t, tx, `create temp table tbl (num integer, text varchar, blob bytea, nothing varchar)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num, text, blob, nothing) from stdin`)
	stmt.MustExec(t, int64(1234567890), "Héllö\n ☃!\r\t\\", []byte{0, 255, 9, 10, 13}, nil)
	stmt.MustExec(t)
	stmt.MustClose(t)

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
	tx := pqtest.Begin(t, db)

	pid := pqtest.Query[int64](t, tx, `select pg_backend_pid() as pid`)
	pqtest.Exec(t, tx, "create temp table tbl (a int)")
	stmt := pqtest.Prepare(t, tx, `copy tbl (a) from stdin`)
	pqtest.Exec(t, db, `select pg_terminate_backend($1)`, pid[0]["pid"])

	var err error
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
