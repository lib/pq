package pq

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
)

func TestHstore(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	RegisterHstore(db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err = tx.Exec("create temp table test (id int, h hstore)"); err != nil {
		t.Fatal(err)
	}

	// null test
	if _, err := tx.Exec("insert into test (id) values ($1)", 1); err != nil {
		t.Fatal(err)
	}
	var out Hstore
	if err = tx.QueryRow("select h from test where id = $1", 1).Scan(&out); err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Errorf("null test: wanted nil Hstore, got %q", out)
	}
	if _, err := tx.Exec("delete from test where id = $1", 1); err != nil {
		t.Fatal(err)
	}

	for i, in := range []map[string]string{
		{},
		{"k1": "v1"},
		{"k1": "v1", "k2": "v2"},
	} {
		if _, err := tx.Exec("insert into test values ($1, $2)", i, Hstore(in)); err != nil {
			t.Fatal(err)
		}
		var out Hstore
		if err = tx.QueryRow("select h from test where id = $1", i).Scan(&out); err != nil {
			t.Fatal(err)
		}
		if !mapsEqual(in, out) {
			t.Errorf("%d: want %q, got %q", i, in, out)
		}
		if _, err := tx.Exec("delete from test where id = $1", i); err != nil {
			t.Fatal(err)
		}
	}
}

func mapsEqual(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k := range m1 {
		if m1[k] != m2[k] {
			return false
		}
	}
	return true
}

var db *sql.DB

func ExampleHstore() {
	RegisterHstore(db)
	rows, err := db.Query("SELECT attributes FROM people")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var attrs Hstore
		if err := rows.Scan(&attrs); err != nil {
			log.Fatal(err)
		}
		for k, v := range attrs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
