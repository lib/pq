package hstore

import (
	"database/sql"
	_ "github.com/lib/pq"
	"os"
	"testing"
)

type Fatalistic interface {
	Fatal(args ...interface{})
}

func openTestConn(t Fatalistic) *sql.DB {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	conn, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func TestHstore(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	// quitely create hstore if it doesn't exist
	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS hstore")
	if err != nil {
		t.Log("Skipping hstore tests - hstore extension create failed. " + err.Error())
		return
	}

	hs := Hstore{}

	// test for null-valued hstores
	err = db.QueryRow("SELECT NULL::hstore").Scan(&hs)
	if err != nil {
		t.Fatal(err)
	}
	if hs != nil {
		t.Fatalf("expected null map")
	}

	err = db.QueryRow("SELECT $1::hstore", hs).Scan(&hs)
	if err != nil {
		t.Fatalf("re-query null map failed: %s", err.Error())
	}
	if hs != nil {
		t.Fatalf("expected null map")
	}

	// test for empty hstores
	err = db.QueryRow("SELECT ''::hstore").Scan(&hs)
	if err != nil {
		t.Fatal(err)
	}
	if hs == nil {
		t.Fatalf("expected empty map, got null map")
	}
	if len(hs) != 0 {
		t.Fatalf("expected empty map, got len(map)=%d", len(hs))
	}

	err = db.QueryRow("SELECT $1::hstore", hs).Scan(&hs)
	if err != nil {
		t.Fatalf("re-query empty map failed: %s", err.Error())
	}
	if hs == nil {
		t.Fatalf("expected empty map, got null map")
	}
	if len(hs) != 0 {
		t.Fatalf("expected empty map, got len(map)=%d", len(hs))
	}

	// a few example maps to test out
	hsOnePair := Hstore{
		"key1": sql.NullString{"value1", true},
	}

	hsThreePairs := Hstore{
		"key1": sql.NullString{"value1", true},
		"key2": sql.NullString{"value2", true},
		"key3": sql.NullString{"value3", true},
	}

	hsSmorgasbord := Hstore{
		"nullstring":             sql.NullString{"NULL", true},
		"actuallynull":           sql.NullString{"", false},
		"NULL":                   sql.NullString{"NULL string key", true},
		"withbracket":            sql.NullString{"value>42", true},
		"withequal":              sql.NullString{"value=42", true},
		`"withquotes1"`:          sql.NullString{`this "should" be fine`, true},
		`"withquotes"2"`:         sql.NullString{`this "should\" also be fine`, true},
		"embedded1":              sql.NullString{"value1=>x1", true},
		"embedded2":              sql.NullString{`"value2"=>x2`, true},
		"withnewlines":           sql.NullString{"\n\nvalue\t=>2", true},
		"<<all sorts of crazy>>": sql.NullString{`this, "should,\" also, => be fine`, true},
	}

	// test encoding in query params, then decoding during Scan
	testBidirectional := func(h Hstore) {
		err = db.QueryRow("SELECT $1::hstore", h).Scan(&hs)
		if err != nil {
			t.Fatalf("re-query %d-pair map failed: %s", len(h), err.Error())
		}
		if hs == nil {
			t.Fatalf("expected %d-pair map, got null map", len(h))
		}
		if len(hs) != len(h) {
			t.Fatalf("expected %d-pair map, got len(map)=%d", len(h), len(hs))
		}

		for key, val := range hs {
			otherval, found := h[key]
			if !found {
				t.Fatalf("  key '%v' not found in %d-pair map", key, len(h))
			}
			if otherval.Valid != val.Valid {
				t.Fatalf("  value %v <> %v in %d-pair map", otherval, val, len(h))
			}
			if otherval.String != val.String {
				t.Fatalf("  value '%v' <> '%v' in %d-pair map", otherval.String, val.String, len(h))
			}
		}
	}

	testBidirectional(hsOnePair)
	testBidirectional(hsThreePairs)
	testBidirectional(hsSmorgasbord)
}
