package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pgpass"
	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/proto"
)

func TestReconnect(t *testing.T) {
	t.Parallel()
	db1 := pqtest.MustDB(t)
	tx, err := db1.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var pid1 int
	err = tx.QueryRow("SELECT pg_backend_pid()").Scan(&pid1)
	if err != nil {
		t.Fatal(err)
	}
	db2 := pqtest.MustDB(t)
	_, err = db2.Exec("SELECT pg_terminate_backend($1)", pid1)
	if err != nil {
		t.Fatal(err)
	}
	// The rollback will probably "fail" because we just killed
	// its connection above
	_ = tx.Rollback()

	const expected int = 42
	var result int
	err = db1.QueryRow(fmt.Sprintf("SELECT %d", expected)).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	if result != expected {
		t.Errorf("got %v; expected %v", result, expected)
	}
}

func TestCommitInFailedTransaction(t *testing.T) {
	db := pqtest.MustDB(t)

	txn := pqtest.Begin(t, db)

	rows, err := txn.Query("SELECT error")
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
	err = txn.Commit()
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

			db, err := pqtest.DB(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			err = db.Ping()
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestPgpass(t *testing.T) {
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
		_, pwd := o["password"]
		have := pgpass.PasswordFromPgpass(o["passfile"], o["user"], o["password"], o["host"], o["port"], o["dbname"], pwd)
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

	if err := os.Chmod(file, 0600); err != nil { // Fix the permissions
		t.Fatal(err)
	}

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
}

func TestExecNilSlice(t *testing.T) {
	db := pqtest.MustDB(t)

	_, err := db.Exec("create temp table x (b1 text, b2 text, b3 text)")
	if err != nil {
		t.Fatal(err)
	}
	var (
		b1 []byte
		b2 []string
		b3 = []byte{}
	)
	_, err = db.Exec("insert into x (b1, b2, b3) values ($1, $2, $3)", b1, b2, b3)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`select * from x`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		var b1, b2, b3 *string
		err = rows.Scan(&b1, &b2, &b3)
		if err != nil {
			t.Fatal(err)
		}

		deref := func(s *string) string {
			if s == nil {
				return "<nil>"
			}
			return fmt.Sprintf("%q", *s)
		}

		want := `b1=<nil>; b2=<nil>; b3=""`
		have := fmt.Sprintf("b1=%s; b2=%s; b3=%s", deref(b1), deref(b2), deref(b3))
		if want != have {
			t.Errorf("\nwant: %s\nhave: %s", want, have)
		}
	}
}

func TestExec(t *testing.T) {
	db := pqtest.MustDB(t)

	_, err := db.Exec("CREATE TEMP TABLE temp (a int)")
	if err != nil {
		t.Fatal(err)
	}

	r, err := db.Exec("INSERT INTO temp VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 row affected, not %d", n)
	}

	r, err = db.Exec("INSERT INTO temp VALUES ($1), ($2), ($3)", 1, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := r.RowsAffected(); n != 3 {
		t.Fatalf("expected 3 rows affected, not %d", n)
	}

	r, err = db.Exec("SELECT g FROM generate_series(1, 2) g")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := r.RowsAffected(); n != 2 {
		t.Fatalf("expected 2 rows affected, not %d", n)
	}

	r, err = db.Exec("SELECT g FROM generate_series(1, $1) g", 3)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := r.RowsAffected(); n != 3 {
		t.Fatalf("expected 3 rows affected, not %d", n)
	}
}

func TestStatment(t *testing.T) {
	db := pqtest.MustDB(t)

	st, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	st1, err := db.Prepare("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}

	r, err := st.Query()
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

	r1, err := st1.Query()
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
		t.Fatalf("expected %s, got %v", errNoRowsAffected, err)
	}
	if _, err := res.LastInsertId(); err != errNoLastInsertID {
		t.Fatalf("expected %s, got %v", errNoLastInsertID, err)
	}
	rows, err := db.Query("")
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

	stmt, err := db.Prepare("")
	if err != nil {
		t.Fatal(err)
	}
	res, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.RowsAffected(); err != errNoRowsAffected {
		t.Fatalf("expected %s, got %v", errNoRowsAffected, err)
	}
	if _, err := res.LastInsertId(); err != errNoLastInsertID {
		t.Fatalf("expected %s, got %v", errNoLastInsertID, err)
	}
	rows, err = stmt.Query()
	if err != nil {
		t.Fatal(err)
	}
	cols, err = rows.Columns()
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

	rows, err := db.Query("SELECT 1 AS a, text 'bar' AS bar WHERE FALSE")
	if err != nil {
		t.Fatal(err)
	}
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

	stmt, err := db.Prepare("SELECT $1::int AS a, text 'bar' AS bar WHERE FALSE")
	if err != nil {
		t.Fatal(err)
	}
	rows, err = stmt.Query(1)
	if err != nil {
		t.Fatal(err)
	}
	cols, err = rows.Columns()
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

}

func TestEncodeDecode(t *testing.T) {
	db := pqtest.MustDB(t)

	q := `
		SELECT
			E'\\000\\001\\002'::bytea,
			'foobar'::text,
			NULL::integer,
			'2000-1-1 01:02:03.04-7'::timestamptz,
			0::boolean,
			123,
			-321,
			3.14::float8
		WHERE
			    E'\\000\\001\\002'::bytea = $1
			AND 'foobar'::text = $2
			AND $3::integer is NULL
	`
	// AND '2000-1-1 12:00:00.000000-7'::timestamp = $3

	exp1 := []byte{0, 1, 2}
	exp2 := "foobar"

	r, err := db.Query(q, exp1, exp2, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	var got1 []byte
	var got2 string
	var got3 = sql.NullInt64{Valid: true}
	var got4 time.Time
	var got5, got6, got7, got8 any

	err = r.Scan(&got1, &got2, &got3, &got4, &got5, &got6, &got7, &got8)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exp1, got1) {
		t.Errorf("expected %q byte: %q", exp1, got1)
	}

	if !reflect.DeepEqual(exp2, got2) {
		t.Errorf("expected %q byte: %q", exp2, got2)
	}

	if got3.Valid {
		t.Fatal("expected invalid")
	}

	if got4.Year() != 2000 {
		t.Fatal("wrong year")
	}

	if got5 != false {
		t.Fatalf("expected false, got %q", got5)
	}

	if got6 != int64(123) {
		t.Fatalf("expected 123, got %d", got6)
	}

	if got7 != int64(-321) {
		t.Fatalf("expected -321, got %d", got7)
	}

	if got8 != float64(3.14) {
		t.Fatalf("expected 3.14, got %f", got8)
	}
}

func TestNoData(t *testing.T) {
	db := pqtest.MustDB(t)

	st, err := db.Prepare("SELECT 1 WHERE true = false")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	r, err := st.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("unexpected row")
	}

	_, err = db.Query("SELECT * FROM nonexistenttable WHERE age=$1", 20)
	if err == nil {
		t.Fatal("Should have raised an error on non existent table")
	}

	_, err = db.Query("SELECT * FROM nonexistenttable")
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
	db := pqtest.MustDB(t, "user=thisuserreallydoesntexist")

	_, err := db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}

	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("wrong error type %T: %[1]s", err)
	}
	if e.Code.Name() != "invalid_authorization_specification" && e.Code.Name() != "invalid_password" {
		t.Fatalf("wrong error code %q: %s", e.Code.Name(), err)
	}
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
	for _, tt := range []error{io.EOF, &Error{Severity: Efatal}} {
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

		// During the Go 1.9 cycle, https://github.com/golang/go/commit/3792db5
		// changed this error from
		//
		// net.errClosing = errors.New("use of closed network connection")
		//
		// to
		//
		// internal/poll.ErrClosing = errors.New("use of closed file or network connection")
		const errClosing = "use of closed"

		// Verify write after closing fails.
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
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMPORARY TABLE foo(f1 int PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec("INSERT INTO foo VALUES (0), (0)")
	if err == nil {
		t.Fatal("Should have raised error")
	}

	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", err)
	} else if e.Code.Name() != "unique_violation" {
		t.Fatalf("expected unique_violation, got %s (%+v)", e.Code.Name(), err)
	}
}

func TestErrorOnQuery(t *testing.T) {
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMPORARY TABLE foo(f1 int PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Query("INSERT INTO foo VALUES (0), (0)")
	if err == nil {
		t.Fatal("Should have raised error")
	}

	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", err)
	} else if e.Code.Name() != "unique_violation" {
		t.Fatalf("expected unique_violation, got %s (%+v)", e.Code.Name(), err)
	}
}

func TestErrorOnQueryRowSimpleQuery(t *testing.T) {
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMPORARY TABLE foo(f1 int PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	var v int
	err = txn.QueryRow("INSERT INTO foo VALUES (0), (0)").Scan(&v)
	if err == nil {
		t.Fatal("Should have raised error")
	}

	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", err)
	} else if e.Code.Name() != "unique_violation" {
		t.Fatalf("expected unique_violation, got %s (%+v)", e.Code.Name(), err)
	}
}

// Test the QueryRow bug workarounds in stmt.exec() and simpleQuery()
func TestQueryRowBugWorkaround(t *testing.T) {
	db := pqtest.MustDB(t)

	// stmt.exec()
	_, err := db.Exec("CREATE TEMP TABLE notnulltemp (a varchar(10) not null)")
	if err != nil {
		t.Fatal(err)
	}

	var a string
	err = db.QueryRow("INSERT INTO notnulltemp(a) values($1) RETURNING a", nil).Scan(&a)
	if err == sql.ErrNoRows {
		t.Fatalf("expected constraint violation error; got: %v", err)
	}
	pge, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *Error; got: %#v", err)
	}
	if pge.Code.Name() != "not_null_violation" {
		t.Fatalf("expected not_null_violation; got: %s (%+v)", pge.Code.Name(), err)
	}

	// Test workaround in simpleQuery()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("unexpected error %s in Begin", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("SET LOCAL check_function_bodies TO FALSE")
	if err != nil {
		t.Fatalf("could not disable check_function_bodies: %s", err)
	}
	_, err = tx.Exec(`
		CREATE OR REPLACE FUNCTION bad_function()
		RETURNS integer
		-- hack to prevent the function from being inlined
		SET check_function_bodies TO TRUE
		AS $$
			SELECT text 'bad'
		$$ LANGUAGE sql
	`)
	if err != nil {
		t.Fatalf("could not create function: %s", err)
	}

	err = tx.QueryRow("SELECT * FROM bad_function()").Scan(&a)
	if err == nil {
		t.Fatalf("expected error")
	}
	pge, ok = err.(*Error)
	if !ok {
		t.Fatalf("expected *Error; got: %#v", err)
	}
	if pge.Code.Name() != "invalid_function_definition" {
		t.Fatalf("expected invalid_function_definition; got: %s (%+v)", pge.Code.Name(), err)
	}

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
	pge, ok = rows.Err().(*Error)
	if !ok {
		t.Fatalf("expected *Error; got: %#v", err)
	}
	if pge.Code.Name() != "cardinality_violation" {
		t.Fatalf("expected cardinality_violation; got: %s (%+v)", pge.Code.Name(), rows.Err())
	}
}

func TestSimpleQuery(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	r, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		t.Fatal("expected row")
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

	db := pqtest.MustDB(t, f.DSN())
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestBindError(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Exec("create temp table test (i integer)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Query("select * from test where i=$1", "hhh")
	if err == nil {
		t.Fatal("expected an error")
	}

	// Should not get error here
	r, err := db.Query("select * from test where i=$1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestParseErrorInExtendedQuery(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Query("PARSE_ERROR $1", 1)
	pqErr, _ := err.(*Error)
	// Expecting a syntax error.
	if err == nil || pqErr == nil || pqErr.Code != "42601" {
		t.Fatalf("expected syntax error, got %s", err)
	}

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

// TestReturning tests that an INSERT query using the RETURNING clause returns a row.
func TestReturning(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Exec("CREATE TEMP TABLE distributors (did integer default 0, dname text)")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("INSERT INTO distributors (did, dname) VALUES (DEFAULT, 'XYZ Widgets') " +
		"RETURNING did;")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("no rows")
	}
	var did int
	err = rows.Scan(&did)
	if err != nil {
		t.Fatal(err)
	}
	if did != 0 {
		t.Fatalf("bad value for did: got %d, want %d", did, 0)
	}

	if rows.Next() {
		t.Fatal("unexpected next row")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIssue186(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	// Exec() a query which returns results
	_, err := db.Exec("VALUES (1), (2), (3)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("VALUES ($1), ($2), ($3)", 1, 2, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Query() a query which doesn't return any results
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	rows, err := txn.Query("CREATE TEMP TABLE foo(f1 int)")
	if err != nil {
		t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}

	// small trick to get NoData from a parameterized query
	_, err = txn.Exec("CREATE RULE nodata AS ON INSERT TO foo DO INSTEAD NOTHING")
	if err != nil {
		t.Fatal(err)
	}
	rows, err = txn.Query("INSERT INTO foo VALUES ($1)", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIssue196(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	row := db.QueryRow("SELECT float4 '0.10000122' = $1, float8 '35.03554004971999' = $2",
		float32(0.10000122), float64(35.03554004971999))

	var float4match, float8match bool
	err := row.Scan(&float4match, &float8match)
	if err != nil {
		t.Fatal(err)
	}
	if !float4match {
		t.Errorf("Expected float4 fidelity to be maintained; got no match")
	}
	if !float8match {
		t.Errorf("Expected float8 fidelity to be maintained; got no match")
	}
}

// Test that any CommandComplete messages sent before the query results are
// ignored.
func TestIssue282(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	var searchPath string
	err := db.QueryRow(`
		SET LOCAL search_path TO pg_catalog;
		SET LOCAL search_path TO pg_catalog;
		SHOW search_path`).Scan(&searchPath)
	if err != nil {
		t.Fatal(err)
	}
	if searchPath != "pg_catalog" {
		t.Fatalf("unexpected search_path %s", searchPath)
	}
}

func TestReadFloatPrecision(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	row := db.QueryRow("SELECT float4 '0.10000122', float8 '35.03554004971999', float4 '1.2'")
	var float4val float32
	var float8val float64
	var float4val2 float64
	err := row.Scan(&float4val, &float8val, &float4val2)
	if err != nil {
		t.Fatal(err)
	}
	if float4val != float32(0.10000122) {
		t.Errorf("Expected float4 fidelity to be maintained; got no match")
	}
	if float8val != float64(35.03554004971999) {
		t.Errorf("Expected float8 fidelity to be maintained; got no match")
	}
	if float4val2 != float64(1.2) {
		t.Errorf("Expected float4 fidelity into a float64 to be maintained; got no match")
	}
}

func TestXactMultiStmt(t *testing.T) {
	// minified test case based on bug reports from
	// pico303@gmail.com and rangelspam@gmail.com
	t.Skip("Skipping failing test")
	db := pqtest.MustDB(t)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Commit()

	rows, err := tx.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}

	if rows.Next() {
		var val int32
		if err = rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected at least one row in first query in xact")
	}

	rows2, err := tx.Query("select 2")
	if err != nil {
		t.Fatal(err)
	}

	if rows2.Next() {
		var val2 int32
		if err := rows2.Scan(&val2); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected at least one row in second query in xact")
	}

	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	if err = rows2.Err(); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestParseComplete(t *testing.T) {
	tpc := func(commandTag string, command string, affectedRows int64, shouldFail bool) {
		cn := new(conn)
		res, c, err := cn.parseComplete(commandTag)
		if err != nil {
			if !shouldFail {
				t.Fatal(err)
			}
			return
		}
		if c != command {
			t.Errorf("Expected %v, got %v", command, c)
		}
		n, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if n != affectedRows {
			t.Errorf("Expected %d, got %d", affectedRows, n)
		}
	}

	tpc("ALTER TABLE", "ALTER TABLE", 0, false)
	tpc("INSERT 0 1", "INSERT", 1, false)
	tpc("UPDATE 100", "UPDATE", 100, false)
	tpc("SELECT 100", "SELECT", 100, false)
	tpc("FETCH 100", "FETCH", 100, false)
	// allow COPY (and others) without row count
	tpc("COPY", "COPY", 0, false)
	// don't fail on command tags we don't recognize
	tpc("UNKNOWNCOMMANDTAG", "UNKNOWNCOMMANDTAG", 0, false)

	// failure cases
	tpc("INSERT 1", "", 0, true)   // missing oid
	tpc("UPDATE 0 1", "", 0, true) // too many numbers
	tpc("SELECT foo", "", 0, true) // invalid row count
}

func TestNullAfterNonNull(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	r, err := db.Query("SELECT 9::integer UNION SELECT NULL::integer")
	if err != nil {
		t.Fatal(err)
	}

	var n sql.NullInt64

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	if err := r.Scan(&n); err != nil {
		t.Fatal(err)
	}

	if n.Int64 != 9 {
		t.Fatalf("expected 2, not %d", n.Int64)
	}

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	if err := r.Scan(&n); err != nil {
		t.Fatal(err)
	}

	if n.Valid {
		t.Fatal("expected n to be invalid")
	}

	if n.Int64 != 0 {
		t.Fatalf("expected n to 2, not %d", n.Int64)
	}
}

func Test64BitErrorChecking(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatal("panic due to 0xFFFFFFFF != -1 when int is 64 bits")
		}
	}()

	t.Parallel()
	db := pqtest.MustDB(t)

	r, err := db.Query(`SELECT *
		FROM (VALUES (0::integer, NULL::text), (1, 'test string')) AS t;`)

	if err != nil {
		t.Fatal(err)
	}

	defer r.Close()

	for r.Next() {
	}
}

func TestCommit(t *testing.T) {
	db := pqtest.MustDB(t)

	_, err := db.Exec("CREATE TEMP TABLE temp (a int)")
	if err != nil {
		t.Fatal(err)
	}
	sqlInsert := "INSERT INTO temp VALUES (1)"
	sqlSelect := "SELECT * FROM temp"
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(sqlInsert)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	var i int
	err = db.QueryRow(sqlSelect).Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}
}

func TestErrorClass(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Query("SELECT int 'notint'")
	if err == nil {
		t.Fatal("expected error")
	}
	pge, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *pq.Error, got %#+v", err)
	}
	if pge.Code.Class() != "22" {
		t.Fatalf("expected class 28, got %v", pge.Code.Class())
	}
	if pge.Code.Class().Name() != "data_exception" {
		t.Fatalf("expected data_exception, got %v", pge.Code.Class().Name())
	}
}

func TestRowsResultTag(t *testing.T) {
	type ResultTag interface {
		Result() driver.Result
		Tag() string
	}

	tests := []struct {
		query string
		tag   string
		ra    int64
	}{
		{
			query: "CREATE TEMP TABLE temp (a int)",
			tag:   "CREATE TABLE",
		},
		{
			query: "INSERT INTO temp VALUES (1), (2)",
			tag:   "INSERT",
			ra:    2,
		},
		{
			query: "SELECT 1",
		},
		// A SELECT anywhere should take precedent.
		{
			query: "SELECT 1; INSERT INTO temp VALUES (1), (2)",
		},
		{
			query: "INSERT INTO temp VALUES (1), (2); SELECT 1",
		},
		// Multiple statements that don't return rows should return the last tag.
		{
			query: "CREATE TEMP TABLE t (a int); DROP TABLE t",
			tag:   "DROP TABLE",
		},
		// Ensure a rows-returning query in any position among various tags-returing
		// statements will prefer the rows.
		{
			query: "SELECT 1; CREATE TEMP TABLE t (a int); DROP TABLE t",
		},
		{
			query: "CREATE TEMP TABLE t (a int); SELECT 1; DROP TABLE t",
		},
		{
			query: "CREATE TEMP TABLE t (a int); DROP TABLE t; SELECT 1",
		},
	}

	conn, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q := conn.(driver.QueryerContext)

	for _, test := range tests {
		rows, err := q.QueryContext(context.Background(), test.query, nil)
		if err != nil {
			t.Fatalf("%s: %s", test.query, err)
		}

		r := rows.(ResultTag)
		if tag := r.Tag(); tag != test.tag {
			t.Fatalf("%s: unexpected tag %q", test.query, tag)
		}
		res := r.Result()
		if ra, _ := res.RowsAffected(); ra != test.ra {
			t.Fatalf("%s: unexpected rows affected: %d", test.query, ra)
		}
		rows.Close()
	}
}

func TestMultipleResult(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	rows, err := db.Query(`
		begin;
			select * from information_schema.tables limit 1;
			select * from information_schema.columns limit 2;
		commit;
	`)
	if err != nil {
		t.Fatal(err)
	}
	type set struct {
		cols     []string
		rowCount int
	}
	buf := []*set{}
	for {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		s := &set{
			cols: cols,
		}
		buf = append(buf, s)

		for rows.Next() {
			s.rowCount++
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if len(buf) != 2 {
		t.Fatalf("got %d sets, expected 2", len(buf))
	}
	if len(buf[0].cols) == len(buf[1].cols) || len(buf[1].cols) == 0 {
		t.Fatal("invalid cols size, expected different column count and greater then zero")
	}
	if buf[0].rowCount != 1 || buf[1].rowCount != 2 {
		t.Fatal("incorrect number of rows returned")
	}
}

func TestMultipleEmptyResult(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	rows, err := db.Query("select 1 where false; select 2")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		t.Fatal("unexpected row")
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		var i int
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expected 2, got %d", i)
		}
	}
	if rows.NextResultSet() {
		t.Fatal("unexpected result set")
	}
}

func TestCopyInStmtAffectedRows(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Exec("CREATE TEMP TABLE temp (a int)")
	if err != nil {
		t.Fatal(err)
	}

	txn, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Fatal(err)
	}

	copyStmt, err := txn.Prepare(CopyIn("temp", "a"))
	if err != nil {
		t.Fatal(err)
	}

	res, err := copyStmt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	res.RowsAffected()
	res.LastInsertId()
}

func TestConnPrepareContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		sql  string
		err  error
	}{
		{
			name: "context.Background",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), nil
			},
			sql: "SELECT 1",
			err: nil,
		},
		{
			name: "context.WithTimeout exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), -time.Minute)
			},
			sql: "SELECT 1",
			err: context.DeadlineExceeded,
		},
		{
			name: "context.WithTimeout",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			sql: "SELECT 1",
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := pqtest.MustDB(t)

			ctx, cancel := tt.ctx()
			if cancel != nil {
				defer cancel()
			}
			_, err := db.PrepareContext(ctx, tt.sql)
			switch {
			case (err != nil) != (tt.err != nil):
				t.Fatalf("conn.PrepareContext() unexpected nil err got = %v, expected = %v", err, tt.err)
			case (err != nil && tt.err != nil) && (err.Error() != tt.err.Error()):
				t.Errorf("conn.PrepareContext() got = %v, expected = %v", err.Error(), tt.err.Error())
			}
		})
	}
}

func TestStmtQueryContext(t *testing.T) {
	if !pqtest.Pgpool() {
		t.Parallel()
	}

	tests := []struct {
		name           string
		ctx            func() (context.Context, context.CancelFunc)
		sql            string
		cancelExpected bool
	}{
		{
			name: "context.Background",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), nil
			},
			sql:            "SELECT pg_sleep(1);",
			cancelExpected: false,
		},
		{
			name: "context.WithTimeout exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Second)
			},
			sql:            "SELECT pg_sleep(10);",
			cancelExpected: true,
		},
		{
			name: "context.WithTimeout",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			sql:            "SELECT pg_sleep(1);",
			cancelExpected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := pqtest.MustDB(t)

			ctx, cancel := tt.ctx()
			if cancel != nil {
				defer cancel()
			}
			stmt, err := db.PrepareContext(ctx, tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.QueryContext(ctx)
			pgErr := (*Error)(nil)
			switch {
			case (err != nil) != tt.cancelExpected:
				t.Fatalf("stmt.QueryContext() unexpected nil err got = %v, cancelExpected = %v", err, tt.cancelExpected)
			case (err != nil && tt.cancelExpected) && !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode):
				t.Errorf("stmt.QueryContext() got = %v, cancelExpected = %v", err.Error(), tt.cancelExpected)
			}
		})
	}
}

func TestStmtExecContext(t *testing.T) {
	if !pqtest.Pgpool() {
		t.Parallel()
	}

	tests := []struct {
		name           string
		ctx            func() (context.Context, context.CancelFunc)
		sql            string
		cancelExpected bool
	}{
		{
			name: "context.Background",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), nil
			},
			sql:            "SELECT pg_sleep(1);",
			cancelExpected: false,
		},
		{
			name: "context.WithTimeout exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Second)
			},
			sql:            "SELECT pg_sleep(10);",
			cancelExpected: true,
		},
		{
			name: "context.WithTimeout",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
			},
			sql:            "SELECT pg_sleep(1);",
			cancelExpected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := pqtest.MustDB(t)

			ctx, cancel := tt.ctx()
			if cancel != nil {
				defer cancel()
			}
			stmt, err := db.PrepareContext(ctx, tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.ExecContext(ctx)
			pgErr := (*Error)(nil)
			switch {
			case (err != nil) != tt.cancelExpected:
				t.Fatalf("stmt.QueryContext() unexpected nil err got = %v, cancelExpected = %v", err, tt.cancelExpected)
			case (err != nil && tt.cancelExpected) && !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode):
				t.Errorf("stmt.QueryContext() got = %v, cancelExpected = %v", err.Error(), tt.cancelExpected)
			}
		})
	}
}

func TestMultipleSimpleQuery(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

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

func TestContextCancelExec(t *testing.T) {
	t.Parallel()
	pqtest.SkipPgpool(t) // TODO: flaky in CI
	db := pqtest.MustDB(t)

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.ExecContext has begun.
	defer time.AfterFunc(time.Millisecond*10, cancel).Stop()

	// Not canceled until after the exec has started.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if pgErr := (*Error)(nil); !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) {
		t.Fatalf("unexpected error: %s", err)
	}

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
	if _, err := db.QueryContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if pgErr := (*Error)(nil); !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) {
		t.Fatalf("unexpected error: %s", err)
	}

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

// TestIssue617 tests that a failed query in QueryContext doesn't lead to a
// goroutine leak.
func TestIssue617(t *testing.T) {
	db := pqtest.MustDB(t)

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
	if _, err := tx.Exec("select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if pgErr := (*Error)(nil); !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) {
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

	for i := 0; i < 100; i++ {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			tx, err := db.BeginTx(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			} else if err, pgErr := tx.Rollback(), (*Error)(nil); err != nil &&
				!(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) &&
				err != sql.ErrTxDone && err != driver.ErrBadConn && err != context.Canceled {
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
	// TODO: hangs forever?
	pqtest.SkipPgpool(t)

	ctx, cancel := context.WithCancel(context.Background())
	db := pqtest.MustDB(t)

	if _, ok := reflect.TypeOf(db).MethodByName("Conn"); !ok {
		t.Skipf("Conn method undefined on type %T, skipping test (requires at least go1.9)", db)
	}

	if err := db.PingContext(ctx); err != nil {
		t.Fatal("expected Ping to succeed")
	}
	defer cancel()

	// grab a connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// start a transaction and read backend pid of our connection
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
	defer rows.Close()

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

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := txn.Query("SELECT error")
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
	err = txn.Commit()
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
				db := pqtest.MustDB(t, tt.conn)

				err := db.Ping()
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

	rows, err := db.Query("select n from tbl")
	if err != nil {
		t.Fatal(err)
	}

	if rows.Next() {
		var i uint64
		err := rows.Scan(&i)
		if err != nil {
			t.Fatal(err)
		}

		if i != math.MaxUint64 {
			t.Fatalf("\nwant: %d\nhave: %d", uint64(math.MaxUint64), i)
		}
	}
}

func TestPreProtocolError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		msg     string
		wantErr string
	}{
		{
			name:    "could not fork",
			msg:     "could not fork new process for connection: Resource temporarily unavailable\n",
			wantErr: "server error: could not fork new process for connection: Resource temporarily unavailable",
		},
		{
			name:    "too many connections",
			msg:     "sorry, too many clients already\n",
			wantErr: "server error: sorry, too many clients already",
		},
		{
			name:    "out of memory",
			msg:     "out of memory\n",
			wantErr: "server error: out of memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
				f.ReadStartup(cn)
				// Send pre-protocol error: 'E' followed by plain text
				// This simulates what PostgreSQL sends when it can't fork
				cn.Write(append([]byte{'E'}, tt.msg...))
				cn.Close()
			})
			defer f.Close()

			db := pqtest.MustDB(t, f.DSN())
			err := db.Ping()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}
