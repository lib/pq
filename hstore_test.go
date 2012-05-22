package pq

import (
	"database/sql"
	"regexp"
	"testing"
)

type matchTest struct {
	in     string
	out    []string
	hstore Hstore
}

func checkMatch(t *testing.T, matches [][]int, mt matchTest) {
	if len(matches) != len(mt.out) {
		t.Fatalf("Match count, expected %d, got %d", len(mt.out), len(matches))
	}
	for i, match := range matches {
		if s := mt.in[match[0]:match[1]]; s != mt.out[i] {
			t.Errorf("Mismatch for %s (%d): expected %q, got %q", mt.in, i, mt.out[i], s)
		}
	}
}

func checkHstore(t *testing.T, expected Hstore, actual Hstore) {

	if len(expected) != len(actual) {
		t.Errorf("Wrong number of parsed elements, expected %q, got %q", len(expected), len(actual))
	}
	for k, v := range actual {
		if v != expected[k] {
			t.Errorf("Parsed data mismatch, expected %q, got %q", expected[k], v)
		}
	}
}

func check(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func fatal(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestMatchChar(t *testing.T) {
	tests := []matchTest{
		{in: "a", out: []string{"a"}},
		{in: "\\\\a", out: []string{"\\\\", "a"}},
		{in: "\\\"a", out: []string{"\\\"", "a"}},
		{in: "\\\\\"a", out: []string{"\\\\", "a"}},
		{in: "a\\\"\\a", out: []string{"a", "\\\"", "a"}},
		{in: "a\\\"\\\\a", out: []string{"a", "\\\"", "\\\\", "a"}},
		{in: "a\\\\\"\\a", out: []string{"a", "\\\\", "a"}},
	}

	re := regexp.MustCompile(hstoreChar)
	for _, mt := range tests {
		matches := re.FindAllStringIndex(mt.in, -1)
		checkMatch(t, matches, mt)
	}
}

func TestMatchString(t *testing.T) {
	tests := []matchTest{
		{in: "a", out: []string{}},
		{in: "\"a\"", out: []string{"\"a\""}},
		{in: "\"id\"=>\"44\", \"foo\"=>\"dfs => somf\", \"name\"=>\"Wash\\\"ington\", \"null\"=>NULL, \"quote\"=>\"\\\"fs ' \"",
			out: []string{
				"\"id\"",
				"\"44\"",
				"\"foo\"",
				"\"dfs => somf\"",
				"\"name\"",
				"\"Wash\\\"ington\"",
				"\"null\"",
				"\"quote\"",
				"\"\\\"fs ' \"",
			}},
	}

	re := regexp.MustCompile("\"" + hstoreString + "\"")
	for _, mt := range tests {
		matches := re.FindAllStringIndex(mt.in, -1)
		checkMatch(t, matches, mt)
	}
}

func TestMatchKey(t *testing.T) {
	tests := []matchTest{
		{in: "a", out: []string{}},
		{in: "\"a\"", out: []string{"\"a\""}},
		{in: "\"id\"=>\"44\", \"foo\"=>\"dfs => somf\", \"name\"=>\"Wash\\\"ington\", \"null\"=>NULL, \"quote\"=>\"\\\"fs ' \"",
			out: []string{
				"\"id\"",
				"\"44\"",
				"\"foo\"",
				"\"dfs => somf\"",
				"\"name\"",
				"\"Wash\\\"ington\"",
				"\"null\"",
				"\"quote\"",
				"\"\\\"fs ' \"",
			}},
	}

	re := regexp.MustCompile(hstoreKey)
	for _, mt := range tests {
		matches := re.FindAllStringIndex(mt.in, -1)
		checkMatch(t, matches, mt)
	}
}

func TestMatchValue(t *testing.T) {
	tests := []matchTest{
		{in: "a", out: []string{}},
		{in: "\"a\"", out: []string{"\"a\""}},
		{in: "\"id\"=>\"44\", \"foo\"=>\"dfs => somf\", \"name\"=>\"Wash\\\"ington\", \"null\"=>NULL, \"quote\"=>\"\\\"fs ' \"",
			out: []string{
				"\"id\"",
				"\"44\"",
				"\"foo\"",
				"\"dfs => somf\"",
				"\"name\"",
				"\"Wash\\\"ington\"",
				"\"null\"",
				"NULL",
				"\"quote\"",
				"\"\\\"fs ' \"",
			}},
	}

	re := regexp.MustCompile(hstoreValue)
	for _, mt := range tests {
		matches := re.FindAllStringIndex(mt.in, -1)
		checkMatch(t, matches, mt)
	}
}

func nullString(v interface{}) sql.NullString {
	if v == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: v.(string), Valid: true}
}


var fullTests = []matchTest{
	{in: "NULL", out:[]string{}},
	{in: "\"a\" => \"b\"",
		out:    []string{"\"a\" => \"b\""},
		hstore: Hstore{"a": nullString("b")}},
	{in: "\"a\"   =>   NULL",
		out:    []string{"\"a\"   =>   NULL"},
		hstore: Hstore{"a": sql.NullString{Valid: false}}},
	{in: "\"id\"=>\"44\", \"foo\"=>\"dfs => somf\", \"name\"=>\"Wash\\\"ington\", \"null\"=>NULL, \"quote\"=>\"\\\"fs ' \"",
		out: []string{
			"\"id\"=>\"44\"",
			"\"foo\"=>\"dfs => somf\"",
			"\"name\"=>\"Wash\\\"ington\"",
			"\"null\"=>NULL",
			"\"quote\"=>\"\\\"fs ' \"",
		},
		hstore: Hstore{
			"id":    nullString("44"),
			"foo":   nullString("dfs => somf"),
			"name":  nullString("Wash\\\"ington"),
			"null":  nullString(nil),
			"quote": nullString("\\\"fs ' "),
		},
	},
}


func TestParseHstore(t *testing.T) {
	for _, mt := range fullTests {
		matches := pairExp.FindAllStringIndex(mt.in, -1)
		checkMatch(t, matches, mt)

		hs := make(Hstore)
		err := parseHstore(mt.in, &hs)
		if err != nil {
			t.Errorf("Error parsing %q: %v", mt.in, err)
		}
		checkHstore(t, mt.hstore, hs)

	}
}

func TestValueAndScan(t *testing.T) {
	for _, mt := range fullTests {
		valued, err := mt.hstore.Value()
		if err != nil {
			t.Fatalf("Value failed: %q", err)
		}
		var scanned Hstore
		(&scanned).Scan([]byte(valued.(string)))
		checkHstore(t, mt.hstore, scanned)
	}
}

func TestDBRoundTrip(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()


	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS hstore")
	fatal(t, err)
	_, err = db.Exec("CREATE TEMP TABLE temp (id serial, data hstore)")
	fatal(t, err)


	for _, mt := range fullTests {
		v, err := mt.hstore.Value()
		check(t, err)
		r, err := db.Exec("INSERT INTO temp (data) VALUES ($1)", v)
		check(t, err)

		if n, _ := r.RowsAffected(); n != 1 {
			t.Fatalf("expected 1 row affected, not %d", n)
		}		
	}
	
	rows, err := db.Query("SELECT data FROM temp ORDER BY id ASC")
	check(t, err)

	for _, mt := range fullTests {
		if !rows.Next() {
			t.Errorf("Ran out of rows!")
		}
		var data Hstore
		err = rows.Scan(&data)
		check(t, err)
		t.Logf("%+v", data)
		checkHstore(t, mt.hstore, data)
	}

	if rows.Next() {
		t.Errorf("Too many rows!")
	}

}
