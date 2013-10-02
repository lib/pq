package hstore

import (
	"database/sql"
	_ "github.com/lib/pq"
	"strings"
	"testing"
	"os"
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
	_, err := db.Exec("CREATE EXTENSION hstore")
	if err != nil {
		if !strings.Contains(err.Error(), "extension \\\"hstore\\\" already exists") {
			t.Fatalf("Unable to create hstore extensio: %s", err.Error())
		}
	}

	hs := Hstore{}
	hsn := HstoreWithNulls{}

	// test for null-valued hstores
	err = db.QueryRow("SELECT NULL::hstore").Scan(&hs)
	if err != nil {
		t.Fatal(err)
	}
	if hs.Map != nil {
		t.Fatalf("expected null map")
	}

	err = db.QueryRow("SELECT $1::hstore", hs).Scan(&hs)
	if err != nil {
		t.Fatalf("re-query null map failed: %s", err.Error())
	}
	if hs.Map != nil {
		t.Fatalf("expected null map")
	}

	// test for empty hstores
	err = db.QueryRow("SELECT ''::hstore").Scan(&hs)
	if err != nil {
		t.Fatal(err)
	}
	if hs.Map == nil {
		t.Fatalf("expected empty map, got null map")
	}
	if len(hs.Map) != 0 {
		t.Fatalf("expected empty map, got len(map)=%d", len(hs.Map))
	}

	err = db.QueryRow("SELECT $1::hstore", hs).Scan(&hs)
	if err != nil {
		t.Fatalf("re-query empty map failed: %s", err.Error())
	}
	if hs.Map == nil {
		t.Fatalf("expected empty map, got null map")
	}
	if len(hs.Map) != 0 {
		t.Fatalf("expected empty map, got len(map)=%d", len(hs.Map))
	}

	// a few example maps to test out
	hsOnePair := Hstore{
		Map: map[string]string{
			"key1": "value1",
		},
	}

	hsThreePairs := Hstore{
		Map: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}

	hsSmorgasbord := Hstore{
		Map: map[string]string{
			"nullstring":             "NULL",
			"NULL":                   "NULL string key",
			"withbracket":            "value>42",
			"withequal":              "value=42",
			`"withquotes1"`:          `this "should" be fine`,
			`"withquotes"2"`:         `this "should\" also be fine`,
			"embedded1":              "value1=>x1",
			"embedded2":              `"value2"=>x2`,
			"withnewlines":           "\n\nvalue\t=>2",
			"<<all sorts of crazy>>": `this, "should,\" also, => be fine`,
		},
	}

	// test encoding in query params, then decoding during Scan
	testBidirectional := func(h Hstore) {
		err = db.QueryRow("SELECT $1::hstore", h).Scan(&hs)
		if err != nil {
			t.Fatalf("re-query %d-pair map failed: %s", len(h.Map), err.Error())
		}
		if hs.Map == nil {
			t.Fatalf("expected %d-pair map, got null map", len(h.Map))
		}
		if len(hs.Map) != len(h.Map) {
			t.Fatalf("expected %d-pair map, got len(map)=%d", len(h.Map), len(hs.Map))
		}
		for key, val := range hs.Map {
			otherval, found := h.Map[key]
			if !found {
				t.Fatalf("  key '%v' not found in %d-pair map", key, len(h.Map))
			}
			if otherval != val {
				t.Fatalf("  value '%v' <> '%v' in %d-pair map", otherval, val, len(h.Map))
			}
		}
	}

	// adds a null value to input map, converts to HstoreWithNulls and tests everything
	testBidirectionalWithNulls := func(hnn Hstore) {
		h := HstoreWithNulls{Map: make(map[string]sql.NullString)}
		for key, val := range hnn.Map {
			h.Map[key] = sql.NullString{String: val, Valid: true}
		}
		h.Map["key with a NULL value"] = sql.NullString{String: "", Valid: false}

		err = db.QueryRow("SELECT $1::hstore", h).Scan(&hsn)
		if err != nil {
			t.Fatalf("re-query %d-pair map failed: %s", len(h.Map), err.Error())
		}
		if hsn.Map == nil {
			t.Fatalf("expected %d-pair map, got null map", len(h.Map))
		}
		if len(hsn.Map) != len(h.Map) {
			t.Fatalf("expected %d-pair map, got len(map)=%d", len(h.Map), len(hsn.Map))
		}

		// test NULL => "NULL" support also
		err = db.QueryRow("SELECT $1::hstore", h).Scan(&hs)
		if err != nil {
			t.Fatalf("re-query %d-pair map failed: %s", len(h.Map), err.Error())
		}
		if hs.Map == nil {
			t.Fatalf("expected %d-pair map, got null map", len(h.Map))
		}
		if len(hs.Map) != len(h.Map) {
			t.Fatalf("expected %d-pair map, got len(map)=%d", len(h.Map), len(hs.Map))
		}

		for key, val := range hsn.Map {
			otherval, found := h.Map[key]
			if !found {
				t.Fatalf("  key '%v' not found in %d-pair map", key, len(h.Map))
			}
			if otherval.Valid != val.Valid {
				t.Fatalf("  value %v <> %v in %d-pair map", otherval, val, len(h.Map))
			}
			if otherval.String != val.String {
				t.Fatalf("  value '%v' <> '%v' in %d-pair map", otherval.String, val.String, len(h.Map))
			}
			if !val.Valid {
				othervalstr, found := hs.Map[key]
				if !found {
					t.Fatalf("  null key '%v' not found in non-null %d-pair map", key, len(h.Map))
				}
				if othervalstr != "NULL" {
					t.Fatalf("  null value is not null: '%v' in %d-pair map", othervalstr, len(h.Map))
				}
			}
		}
	}

	testBidirectional(hsOnePair)
	testBidirectional(hsThreePairs)
	testBidirectional(hsSmorgasbord)

	testBidirectionalWithNulls(hsOnePair)
	testBidirectionalWithNulls(hsThreePairs)
	testBidirectionalWithNulls(hsSmorgasbord)
}
