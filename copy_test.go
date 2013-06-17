package pq

import (
	"bytes"
	"database/sql"
	"testing"
)

func TestCopyInMultipleValues(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE temp (a int)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("COPY temp (a) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		_, err = stmt.Exec(int64(i))
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
