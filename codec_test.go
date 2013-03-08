package pq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func openTestConnDsn(t *testing.T, dsn string) *sql.DB {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

var _ driver.ColumnConverter = &stmt{}

////////////////////////// start codec support

type CustomArray []string

func int4ArrayDecoder(b []byte) (driver.Value, error) {
	ca := make(CustomArray, 0)
	for _, s := range strings.Split(string(b), ",") {
		ca = append(ca, s)
	}
	return ca, nil
}

func int4ArrayEncoder(v interface{}) (driver.Value, error) {
	switch x := v.(type) {
	case []int:
		s := make([]string, len(x))
		for i, n := range x {
			s[i] = strconv.Itoa(n)
		}
		return []byte(fmt.Sprintf(`{%s}`, strings.Join(s, ","))), nil
	}
	panic("unexpected type")
}

func noopDecoder(b []byte) (driver.Value, error) {
	return b, nil
}

func customCodec() *codec {
	c := NewCodec()
	c.RegisterDecoder(1007, int4ArrayDecoder)
	c.RegisterEncoder(1007, int4ArrayEncoder)
	return c
}

///////////////////////// end codec support

func TestCustomDecode(t *testing.T) {
	RegisterCodec("CUSTOMCODEC2", customCodec())
	db := openTestConnDsn(t, "codec=CUSTOMCODEC2")
	defer db.Close()

	q := `
		SELECT ARRAY[1,2,3]
	`
	r, err := db.Query(q)
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

	var got1 CustomArray

	err = r.Scan(&got1)
	if err != nil {
		t.Fatal(err)
	}

	if len(got1) == 0 {
		t.Errorf(`expected len to be 3 got %d`, len(got1))
	}

	if got1[1] != "2" {
		t.Errorf(`expected got1[1] to be "2" got %s`, got1[1])
	}
}

func TestCustomEncode(t *testing.T) {
	RegisterCodec("CUSTOMCODEC", customCodec())
	db := openTestConnDsn(t, "codec=CUSTOMCODEC")
	defer db.Close()

	q := `
		SELECT 1 WHERE $1 = '{1,2,3}'::int[]
	`
	v1 := []int{1, 2, 3}
	r, err := db.Query(q, v1)
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

	var got1 int

	err = r.Scan(&got1)
	if err != nil {
		t.Fatal(err)
	}

	if got1 != 1 {
		t.Errorf(`expected got1 == 1 got: %d`, got1)
	}
}

func TestGlobalDecoder(t *testing.T) {
	RegisterDecoder(1007, int4ArrayDecoder)
	db := openTestConn(t)
	defer db.Close()

	q := `
		SELECT ARRAY[1,2,3]
	`
	r, err := db.Query(q)
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

	var got1 CustomArray

	err = r.Scan(&got1)
	if err != nil {
		t.Fatal(err)
	}

	if len(got1) == 0 {
		t.Errorf(`expected len to be 3 got %d`, len(got1))
	}

	if got1[1] != "2" {
		t.Errorf(`expected got1[1] to be "2" got %s`, got1[1])
	}
}

func TestCannotRegisterTwice(t *testing.T) {
	err := RegisterDecoder(1560, noopDecoder)
	if err != nil {
		t.Errorf("should have registered decoder for oid 1560")
	}
	err = RegisterDecoder(1560, noopDecoder)
	if err == nil {
		t.Errorf("Should have FAILED to register decoder for oid 1560 as it was already registered")
	}
}

func TestCannotRegisterStandardType(t *testing.T) {
	err := RegisterDecoder(16, noopDecoder)
	if err == nil {
		t.Errorf("Should have FAILED to register decoder for oid 16 as it is a standard type")
	}
}
