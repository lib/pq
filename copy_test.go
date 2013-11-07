package pq

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"
)

func TestCopyInMultipleValues(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (a int, b varchar)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("COPY temp (a, b) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}

	longString := strings.Repeat("#", 500)

	for i := 0; i < 500; i++ {
		_, err = stmt.Exec(int64(i), longString)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	var num int
	row := db.QueryRow("SELECT COUNT(*) FROM temp")
	err = row.Scan(&num)
	if err != nil {
		t.Fatal(err)
	}

	if num != 500 {
		t.Fatalf("expected 500 items, not %d", num)
	}

}

func TestCopyInTypes(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (num INTEGER, text VARCHAR, blob BYTEA, nothing VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("COPY temp (num, text, blob, nothing) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec(int64(1234567890), "Héllö\n ☃!\r\t\\", []byte{0, 255, 9, 10, 13}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	var num int
	var text string
	var blob []byte
	var nothing sql.NullString

	row := db.QueryRow("SELECT * FROM temp")
	err = row.Scan(&num, &text, &blob, &nothing)
	if err != nil {
		t.Fatal(err)
	}

	if num != 1234567890 {
		t.Fatal("unexpected result", num)
	}
	if text != "Héllö\n ☃!\r\t\\" {
		t.Fatal("unexpected result", text)
	}
	if bytes.Compare(blob, []byte{0, 255, 9, 10, 13}) != 0 {
		t.Fatal("unexpected result", blob)
	}
	if nothing.Valid {
		t.Fatal("unexpected result", nothing.String)
	}
}

func TestCopyInWrongType(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (num INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("COPY temp (num) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec("Héllö\n ☃!\r\t\\")
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if err == nil {
		t.Fatal("expected 'invalid input syntax for integer' error")
	}

}

func TestCopyInBinary(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (num INTEGER, text VARCHAR, blob BYTEA, nothing VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Prepare("COPY temp (num, text, blob, nothing) FROM STDIN WITH binary")
	if err == nil {
		t.Fatal("COPY with binary format did not return error")
	}

}

func BenchmarkCopyIn(b *testing.B) {
	db := openTestConn(b)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (a int, b varchar)")
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := db.Prepare("COPY temp (a, b) FROM STDIN")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = stmt.Exec(int64(i), "hello world!")
		if err != nil {
			b.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		b.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		b.Fatal(err)
	}

	var num int
	row := db.QueryRow("SELECT COUNT(*) FROM temp")
	err = row.Scan(&num)
	if err != nil {
		b.Fatal(err)
	}

	if num != b.N {
		b.Fatalf("expected %d items, not %d", b.N, num)
	}

}
