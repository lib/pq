package pq

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
	"time"
)

var cs = "user=pqgotest sslmode=disable"

func TestExec(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DELETE FROM temp")

	r, err := db.Exec("INSERT INTO temp VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 row affected, not %d", n)
	}
}

func TestStatment(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
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

	r, err = st1.Query()
	if err != nil {
		t.Fatal(err)
	}

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	err = r.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 2 {
		t.Fatalf("expected 2, got %d", i)
	}
}

func TestRowsCloseBeforeDone(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}

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
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	q := `
		SELECT 
			'\x000102'::bytea,
			'foobar'::text,
			NULL::integer,
			'2000-1-1 01:02:03.04-7'::timestamptz
		WHERE 
			    '\x000102'::bytea = $1
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

	err = r.Scan(&got1, &got2, &got3, &got4)
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
}

func TestNoData(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestPGError(t *testing.T) {
	db, err := sql.Open("postgres", "user=asdf")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}

	if err != driver.ErrBadConn {
		t.Fatalf("expected a PGError, got: %v", err)
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
		e := &PGError{c: make(map[byte]string)}
		e.c['S'] = Efatal
		panic(e)
	}()

	if err != driver.ErrBadConn {
		t.Fatalf("expected driver.ErrBadConn, got: %#v", err)
	}
}

func TestErrorOnExec(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sql := "DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END; $$;"
	_, err = db.Exec(sql)
	_, ok := err.(*PGError)
	if !ok {
		t.Fatalf("expected PGError, was: %#v", err)
	}

	_, err = db.Exec("SELECT 1 WHERE true = false") // returns no rows
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrorOnQuery(t *testing.T) {
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sql := "DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END; $$;"
	r, err := db.Query(sql)
	if err != nil {
		t.Fatal(err)
	}

	if r.Next() {
		t.Fatal("unexpected row, want error")
	}

	_, ok := r.Err().(*PGError)
	if !ok {
		t.Fatalf("expected PGError, was: %#v", r.Err())
	}

	r, err = db.Query("SELECT 1 WHERE true = false") // returns no rows
	if err != nil {
		t.Fatal(err)
	}

	if r.Next() {
		t.Fatal("unexpected row")
	}
}
