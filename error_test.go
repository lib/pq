package pq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func TestErrorSQLState(t *testing.T) {
	r := readBuf([]byte{67, 52, 48, 48, 48, 49, 0, 0}) // 40001
	err := parseError(&r, "")
	var sqlErr interface{ SQLState() string }
	if !errors.As(err, &sqlErr) {
		t.Fatal("SQLState interface not satisfied")
	}
	if state := err.SQLState(); state != "40001" {
		t.Fatalf("unexpected SQL state %v", state)
	}
}

func TestError(t *testing.T) {
	tests := []struct {
		in, want, wantDetail string
	}{
		{`create schema pg_xx`, `pq: unacceptable schema name "pg_xx" (42939)`, `
			ERROR:   unacceptable schema name "pg_xx" (42939)
			DETAIL:  The prefix "pg_" is reserved for system schemas.
		`},
		{`create view x as select 1; copy x to stdout`, `pq: cannot copy from view "x" (42809)`, `
			ERROR:   cannot copy from view "x" (42809)
			HINT:    Try the COPY (SELECT ...) TO variant.
		`},
		{`select columndoesntexist`, `pq: column "columndoesntexist" does not exist at column 8 (42703)`, `
			ERROR:   column "columndoesntexist" does not exist (42703)
			CONTEXT: line 1, column 8:

			      1 | select columndoesntexist
			                 ^
		`},
		{`select !@#`, "pq: syntax error at end of input at column 11 (42601)", `
			ERROR:   syntax error at end of input (42601)
			CONTEXT: line 1, column 11:

			      1 | select !@#
			                    ^
		`},
		{"select 'asd',\n\t'asd'::jsonb", "pq: invalid input syntax for type json at position 2:2 (22P02)", `
			ERROR:   invalid input syntax for type json (22P02)
			DETAIL:  Token "asd" is invalid.
			CONTEXT: line 2, column 2:

			      1 | select 'asd',
			      2 |         'asd'::jsonb
			                  ^
		`},
		{"select 'asd'\n,'zxc',\n'def',\n123,\n'foo', 'asd'::jsonb", "pq: invalid input syntax for type json at position 5:8 (22P02)", `
			ERROR:   invalid input syntax for type json (22P02)
			DETAIL:  Token "asd" is invalid.
			CONTEXT: line 5, column 8:

			      3 | 'def',
			      4 | 123,
			      5 | 'foo', 'asd'::jsonb
			                 ^
		`},
		{"select '€€€', a", `pq: column "a" does not exist at column 15 (42703)`, `
			ERROR:   column "a" does not exist (42703)
			CONTEXT: line 1, column 15:

			      1 | select '€€€', a
			                        ^
		`},
		{"select '€€€',\n'€',a", `pq: column "a" does not exist at position 2:5 (42703)`, `
			ERROR:   column "a" does not exist (42703)
			CONTEXT: line 2, column 5:

			      1 | select '€€€',
			      2 | '€',a
			              ^
		`},
		{pqtest.NormalizeIndent(`
			create table browsers (
			    browser_id     serial,
			    name           varchar,
			    version        varchar
			);
			create unique index "browsers#name#version" on browsers(name, version);

			create table systems (
			    system_id      serial,
			    name           varchar,
			    version        varchar,
			);
			create unique index "systems#name#version"  on systems(name, version);
		`), `pq: syntax error at or near ")" at position 12:1 (42601)`, `
			ERROR:   syntax error at or near ")" (42601)
			CONTEXT: line 12, column 1:

			     10 |     name           varchar,
			     11 |     version        varchar,
			     12 | );
			          ^
		`},

		{pqtest.NormalizeIndent(`
			create table browsers (browser_id serial, name varchar, version varchar); create unique index "browsers#name#version" on browsers(name, version);
			create table systems (system_id serial, name varchar, version varchar,); create unique index "systems#name#version"  on systems(name, version);
		`), `pq: syntax error at or near ")" at position 2:71 (42601)`, `
			ERROR:   syntax error at or near ")" (42601)
			CONTEXT: line 2, column 71:

			      1 | create table browsers (browser_id serial, name varchar, version varchar); create unique index "browsers#name#version" on browsers(name, version);
			      2 | create table systems (system_id serial, name varchar, version varchar,); create unique index "systems#name#version"  on systems(name, version);
			                                                                                ^
		`},
	}

	t.Parallel()
	db := pqtest.MustDB(t)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, err := db.Exec(tt.in)
			if err == nil {
				t.Fatal("no error?")
			}
			pqErr := new(Error)
			if !errors.As(err, &pqErr) {
				t.Fatalf("wrong error %T: %[1]s", err)
			}

			if err.Error() != tt.want {
				t.Errorf("\nhave: %s\nwant: %s", err.Error(), tt.want)
			}
			tt.wantDetail = pqtest.NormalizeIndent(tt.wantDetail)
			if pqErr.query != "" && pqErr.Position != "" {
				tt.wantDetail += "\n"
			}
			if pqErr.ErrorWithDetail() != tt.wantDetail {
				t.Errorf("\nhave:\n%s\nwant:\n%s", pqErr.ErrorWithDetail(), tt.wantDetail)
			}
		})
	}
}

func BenchmarkError(b *testing.B) {
	db := pqtest.MustDB(b)
	_, err := db.Exec(pqtest.NormalizeIndent(`
		create table browsers (
			browser_id     serial,
			name           varchar,
			version        varchar
		);
		create unique index "browsers#name#version" on browsers(name, version);

		create table systems (
			system_id      serial,
			name           varchar,
			version        varchar,
		);
		create unique index "systems#name#version"  on systems(name, version);
	`))
	if err == nil {
		b.Fatal("err is nil?")
	}

	b.ResetTimer()
	b.Run("error", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})
	b.Run("errorWithDetail", func(b *testing.B) {
		pqErr := new(Error)
		if !errors.As(err, &pqErr) {
			b.Fatalf("not pq.Error: %T", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = pqErr.ErrorWithDetail()
		}
	})
}

type (
	failConn   struct{ net.Conn }
	failDialer struct{ d net.Dialer }
)

func (cn *failConn) Write(b []byte) (n int, err error) {
	if n, ok := os.LookupEnv("PQTEST_FAILNUM"); ok {
		nn, err := strconv.Atoi(n)
		if err != nil {
			panic(err)
		}
		nn--
		if nn == 0 {
			os.Unsetenv("PQTEST_FAILNUM")
		} else {
			os.Setenv("PQTEST_FAILNUM", strconv.Itoa(nn))
		}
		//debug.PrintStack()
		if _, ok := os.LookupEnv("PQTEST_FAILNET"); ok {
			return 1, &net.OpError{Op: "write", Net: "tcp", Err: fmt.Errorf("failConn: PQTEST_FAILNUM=%d", nn+1)}
		}
		return 0, fmt.Errorf("failConn: PQTEST_FAILNUM=%d", nn+1)
	}
	return cn.Conn.Write(b)
}

func (d failDialer) Dial(n, a string) (net.Conn, error) {
	return d.DialContext(context.Background(), n, a)
}

func (d failDialer) DialTimeout(n, a string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, n, a)
}

func (d failDialer) DialContext(ctx context.Context, n, a string) (net.Conn, error) {
	cn, err := d.d.DialContext(ctx, n, a)
	if err != nil {
		return nil, err
	}
	return &failConn{cn}, nil
}

// Make sure it retries on network errors when it failed to write any data.
func TestRetryError(t *testing.T) {
	c, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	c.Dialer(failDialer{})
	db := sql.OpenDB(c)
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Unsetenv("PQTEST_FAILNUM")
		os.Unsetenv("PQTEST_FAILNET")
	})

	// Make write fail once so that safeRetryError{} is used.
	for i := 0; i < 10; i++ {
		os.Setenv("PQTEST_FAILNUM", "1")
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}

	// Should fail if it returns ErrBadConn too often.
	os.Setenv("PQTEST_FAILNUM", "5")
	if _, err := db.Begin(); err == nil {
		t.Fatal("no error?")
	}
}

func TestNetworkError(t *testing.T) {
	c, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	c.Dialer(failDialer{})
	db := sql.OpenDB(c)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Unsetenv("PQTEST_FAILNUM")
		os.Unsetenv("PQTEST_FAILNET")
	})

	os.Setenv("PQTEST_FAILNUM", "1")
	os.Setenv("PQTEST_FAILNET", "1")
	_, err = db.Begin()
	if err == nil || !errors.As(err, new(*net.OpError)) {
		t.Fatalf("wrong error %T: %[1]s", err)
	}

	// TODO: should make sure this opens a new connection.
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}
