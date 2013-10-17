package pq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type Fatalistic interface {
	Fatal(args ...interface{})
}

func openTestConnConninfo(conninfo string) (*sql.DB, error) {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	return sql.Open("postgres", conninfo)
}

func openTestConn(t Fatalistic) *sql.DB {
	conn, err := openTestConnConninfo("")
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func TestReconnect(t *testing.T) {
	if runtime.Version() == "go1.0.2" {
		fmt.Println("Skipping failing test; " +
			"fixed in database/sql on go1.0.3+")
		return
	}

	db1 := openTestConn(t)
	defer db1.Close()
	tx, err := db1.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var pid1 int
	err = tx.QueryRow("SELECT pg_backend_pid()").Scan(&pid1)
	if err != nil {
		t.Fatal(err)
	}
	db2 := openTestConn(t)
	defer db2.Close()
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

func TestOpenURL(t *testing.T) {
	db, err := openTestConnConninfo("postgres://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestExec(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

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
	db := openTestConn(t)
	defer db.Close()

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

	// st1

	r1, err := st1.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()

	if !r1.Next() {
		if r.Err() != nil {
			t.Fatal(r1.Err())
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

func TestRowsCloseBeforeDone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	if r.Next() {
		t.Fatal("unexpected row")
	}

	if r.Err() != nil {
		t.Fatal(r.Err())
	}
}

func TestEncodeDecode(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	q := `
		SELECT
			E'\\x000102'::bytea,
			'foobar'::text,
			NULL::integer,
			'2000-1-1 01:02:03.04-7'::timestamptz,
			0::boolean,
			123,
			3.14::float8
		WHERE
			    E'\\x000102'::bytea = $1
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
	var got5, got6, got7 interface{}

	err = r.Scan(&got1, &got2, &got3, &got4, &got5, &got6, &got7)
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

	if got7 != float64(3.14) {
		t.Fatalf("expected 3.14, got %f", got7)
	}
}

func TestNoData(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

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

func TestError(t *testing.T) {
	// Don't use the normal connection setup, this is intended to
	// blow up in the startup packet from a non-existent user.
	db, err := openTestConnConninfo("user=thisuserreallydoesntexist")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}

	if err != driver.ErrBadConn {
		t.Fatalf("expected driver.ErrBadConn, got: %v", err)
	}
}

func TestBadConn(t *testing.T) {
	var err error

	func() {
		defer errRecover(&err)
		panic(io.EOF)
	}()

	if err != driver.ErrBadConn {
		t.Fatalf("expected driver.ErrBadConn, got: %#v", err)
	}

	func() {
		defer errRecover(&err)
		e := &Error{Severity: Efatal}
		panic(e)
	}()

	if err != driver.ErrBadConn {
		t.Fatalf("expected driver.ErrBadConn, got: %#v", err)
	}
}

func TestErrorOnExec(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	sql := "DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END; $$;"
	_, err := db.Exec(sql)
	_, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, was: %#v", err)
	}

	_, err = db.Exec("SELECT 1 WHERE true = false") // returns no rows
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrorOnQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	sql := "DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END; $$;"
	r, err := db.Query(sql)
	if r != nil {
		t.Fatal("Should not return rows")
	}
	if err == nil {
		t.Fatal("Should have raised error")
	}
	_, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, was: %#v", err)
	}

	r, err = db.Query("SELECT 1 WHERE true = false") // returns no rows
	if err != nil {
		t.Fatal(err)
	}

	if r.Next() {
		t.Fatal("unexpected row")
	}
}

func TestErrorOnQueryRow(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	sql := "DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END; $$;"
	r := db.QueryRow(sql)
	var v int
	err := r.Scan(&v)
	if err == nil {
		t.Fatal("Should have raised error")
	}

	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected Error, got %#v", err)
	} else if e.Code.Name() != "unique_violation" {
		t.Fatalf("expected unique_violation, got %s (%+v", e.Code.Name(), err)
	}
}

func TestSimpleQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query("select 1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		t.Fatal("expected row")
	}
}

func TestBindError(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

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

// TestReturning tests that an INSERT query using the RETURNING clause returns a row.
func TestReturning(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

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

var envParseTests = []struct {
	Expected map[string]string
	Env      []string
}{
	{
		Env:      []string{"PGDATABASE=hello", "PGUSER=goodbye"},
		Expected: map[string]string{"dbname": "hello", "user": "goodbye"},
	},
	{
		Env:      []string{"PGDATESTYLE=ISO, MDY"},
		Expected: map[string]string{"datestyle": "ISO, MDY"},
	},
}

func TestParseEnviron(t *testing.T) {
	for i, tt := range envParseTests {
		results := parseEnviron(tt.Env)
		if !reflect.DeepEqual(tt.Expected, results) {
			t.Errorf("%d: Expected: %#v Got: %#v", i, tt.Expected, results)
		}
	}
}

func TestExecerInterface(t *testing.T) {
	// Gin up a straw man private struct just for the type check
	cn := &conn{c: nil}
	var cni interface{} = cn

	_, ok := cni.(driver.Execer)
	if !ok {
		t.Fatal("Driver doesn't implement Execer")
	}
}

func TestNullAfterNonNull(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query("SELECT 9::integer UNION SELECT NULL::integer")
	if err != nil {
		t.Fatal(err)
	}

	var n sql.NullInt64

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
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
			t.Fatal(err)
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
			t.Fatal("panic due to 0xFFFFFFFF != -1 " +
				"when int is 64 bits")
		}
	}()

	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query(`SELECT *
FROM (VALUES (0::integer, NULL::text), (1, 'test string')) AS t;`)

	if err != nil {
		t.Fatal(err)
	}

	defer r.Close()

	for r.Next() {
	}
}

// Open transaction, issue INSERT query inside transaction, rollback
// transaction, issue SELECT query to same db used to create the tx.  No rows
// should be returned.
func TestRollback(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

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
	_, err = tx.Query(sqlInsert)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Query(sqlSelect)
	if err != nil {
		t.Fatal(err)
	}
	// Next() returns false if query returned no rows.
	if r.Next() {
		t.Fatal("Transaction rollback failed")
	}
}

func TestParseOpts(t *testing.T) {
	tests := []struct {
		in       string
		expected values
		valid    bool
	}{
		{"dbname=hello user=goodbye", values{"dbname": "hello", "user": "goodbye"}, true},
		{"dbname=hello user=goodbye  ", values{"dbname": "hello", "user": "goodbye"}, true},
		{"dbname = hello user=goodbye", values{"dbname": "hello", "user": "goodbye"}, true},
		{"dbname=hello user =goodbye", values{"dbname": "hello", "user": "goodbye"}, true},
		{"dbname=hello user= goodbye", values{"dbname": "hello", "user": "goodbye"}, true},
		{"host=localhost password='correct horse battery staple'", values{"host": "localhost", "password": "correct horse battery staple"}, true},
		{"dbname=データベース password=パスワード", values{"dbname": "データベース", "password": "パスワード"}, true},
		{"dbname=hello user=''", values{"dbname": "hello", "user": ""}, true},
		{"user='' dbname=hello", values{"dbname": "hello", "user": ""}, true},
		// The last option value is an empty string if there's no non-whitespace after its =
		{"dbname=hello user=   ", values{"dbname": "hello", "user": ""}, true},

		// The parser ignores spaces after = and interprets the next set of non-whitespace characters as the value.
		{"user= password=foo", values{"user": "password=foo"}, true},

		// No '=' after the key
		{"postgre://marko@internet", values{}, false},
		{"dbname user=goodbye", values{}, false},
		{"user=foo blah", values{}, false},
		{"user=foo blah   ", values{}, false},

		// Unterminated quoted value
		{"dbname=hello user='unterminated", values{}, false},
	}

	for _, test := range tests {
		o := make(values)
		err := parseOpts(test.in, o)

		switch {
		case err != nil && test.valid:
			t.Errorf("%q got unexpected error: %s", test.in, err)
		case err == nil && test.valid && !reflect.DeepEqual(test.expected, o):
			t.Errorf("%q got: %#v want: %#v", test.in, o, test.expected)
		case err == nil && !test.valid:
			t.Errorf("%q expected an error", test.in)
		}
	}
}

func TestRuntimeParameters(t *testing.T) {
	type RuntimeTestResult int
	const (
		ResultBadConn RuntimeTestResult = iota
		ResultPanic
		ResultSuccess
		ResultError // other error
	)

	tests := []struct {
		conninfo        string
		param           string
		expected        string
		expectedOutcome RuntimeTestResult
	}{
		// invalid parameter
		{"DOESNOTEXIST=foo", "", "", ResultBadConn},
		// we can only work with a specific value for these two
		{"client_encoding=SQL_ASCII", "", "", ResultError},
		{"datestyle='ISO, YDM'", "", "", ResultPanic},
		// "options" should work exactly as it does in libpq
		{"options='-c search_path=pqgotest'", "search_path", "pqgotest", ResultSuccess},
		// pq should override client_encoding in this case
		{"options='-c client_encoding=SQL_ASCII'", "client_encoding", "UTF8", ResultSuccess},
		// allow client_encoding to be set explicitly
		{"client_encoding=UTF8", "client_encoding", "UTF8", ResultSuccess},
		// test a runtime parameter not supported by libpq
		{"work_mem='139kB'", "work_mem", "139kB", ResultSuccess},
	}

	for _, test := range tests {
		db, err := openTestConnConninfo(test.conninfo)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		tryGetParameterValue := func() (value string, outcome RuntimeTestResult) {
			defer func() {
				if p := recover(); p != nil {
					outcome = ResultPanic
				}
			}()
			row := db.QueryRow("SELECT current_setting($1)", test.param)
			err = row.Scan(&value)
			if err == driver.ErrBadConn {
				return "", ResultBadConn
			}
			if err != nil {
				return "", ResultError
			}
			return value, ResultSuccess
		}

		value, outcome := tryGetParameterValue()
		if outcome != test.expectedOutcome && outcome == ResultError {
			t.Fatalf("%v: unexpected error: %v", test.conninfo, err)
		}
		if outcome != test.expectedOutcome {
			t.Fatalf("unexpected outcome %v (was expecting %v) for conninfo \"%s\"",
				outcome, test.expectedOutcome, test.conninfo)
		}
		if value != test.expected {
			t.Fatalf("bad value for %s: got %s, want %s with conninfo \"%s\"",
				test.param, value, test.expected, test.conninfo)
		}
	}
}

func TestIsUTF8(t *testing.T) {
	var cases = []struct {
		name string
		want bool
	}{
		{"unicode", true},
		{"utf-8", true},
		{"utf_8", true},
		{"UTF-8", true},
		{"UTF8", true},
		{"utf8", true},
		{"u n ic_ode", true},
		{"ut_f%8", true},
		{"ubf8", false},
		{"punycode", false},
	}

	for _, test := range cases {
		if g := isUTF8(test.name); g != test.want {
			t.Errorf("isUTF8(%q) = %v want %v", test.name, g, test.want)
		}
	}
}
