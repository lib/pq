package pq

import (
	"fmt"
	"testing"
	"time"
)

func TestScanTimestamp(t *testing.T) {
	var nt NullTime
	tn := time.Now()
	nt.Scan(tn)
	if !nt.Valid {
		t.Errorf("Expected Valid=false")
	}
	if nt.Time != tn {
		t.Errorf("Time value mismatch")
	}
}

func TestScanNilTimestamp(t *testing.T) {
	var nt NullTime
	nt.Scan(nil)
	if nt.Valid {
		t.Errorf("Expected Valid=false")
	}
}

var timeTests = []struct{
	str string
	expected time.Time
}{
	{ "22001-02-03", time.Date(22001,time.February,3,0,0,0,0,time.UTC), },
	{ "2001-02-03", time.Date(2001,time.February,3,0,0,0,0,time.UTC), },
	{ "2001-02-03 04:05:06", time.Date(2001,time.February,3,4,5,6,0,time.UTC), },
	{ "2001-02-03 04:05:06.000001", time.Date(2001,time.February,3,4,5,6,1000,time.UTC), },
	{ "2001-02-03 04:05:06.00001", time.Date(2001,time.February,3,4,5,6,10000,time.UTC), },
	{ "2001-02-03 04:05:06.0001", time.Date(2001,time.February,3,4,5,6,100000,time.UTC), },
	{ "2001-02-03 04:05:06.001", time.Date(2001,time.February,3,4,5,6,1000000,time.UTC), },
	{ "2001-02-03 04:05:06.01", time.Date(2001,time.February,3,4,5,6,10000000,time.UTC), },
	{ "2001-02-03 04:05:06.1", time.Date(2001,time.February,3,4,5,6,100000000,time.UTC), },
	{ "2001-02-03 04:05:06.12", time.Date(2001,time.February,3,4,5,6,120000000,time.UTC), },
	{ "2001-02-03 04:05:06.123", time.Date(2001,time.February,3,4,5,6,123000000,time.UTC), },
	{ "2001-02-03 04:05:06.1234", time.Date(2001,time.February,3,4,5,6,123400000,time.UTC), },
	{ "2001-02-03 04:05:06.12345", time.Date(2001,time.February,3,4,5,6,123450000,time.UTC), },
	{ "2001-02-03 04:05:06.123456", time.Date(2001,time.February,3,4,5,6,123456000,time.UTC), },
	{ "2001-02-03 04:05:06.123-07", time.Date(2001,time.February,3,4,5,6,123000000,
		time.FixedZone("", -7*60*60)), },
	{ "2001-02-03 04:05:06-07", time.Date(2001,time.February,3,4,5,6,0,
		time.FixedZone("", -7*60*60)), },
	{ "2001-02-03 04:05:06-07:42", time.Date(2001,time.February,3,4,5,6,0,
		time.FixedZone("", -7*60*60 + 42*60)), },
	{ "2001-02-03 04:05:06-07:30:09", time.Date(2001,time.February,3,4,5,6,0,
		time.FixedZone("", -7*60*60 + 30*60 + 9)), },
	{ "2001-02-03 04:05:06+07", time.Date(2001,time.February,3,4,5,6,0,
		time.FixedZone("", 7*60*60)), },
	{ "10000-02-03 04:05:06 BC", time.Date(-10000,time.February,3,4,5,6,0,time.UTC), },
	{ "0010-02-03 04:05:06 BC", time.Date(-10,time.February,3,4,5,6,0,time.UTC), },
	{ "0010-02-03 04:05:06.123 BC", time.Date(-10,time.February,3,4,5,6,123000000,time.UTC), },
	{ "0010-02-03 04:05:06.123-07 BC", time.Date(-10,time.February,3,4,5,6,123000000,
		time.FixedZone("", -7*60*60)), },

}

func tryParse(str string) (t time.Time, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
			return
		}
	}()
	t = parseTs(str)
	return
}

func TestParseTs(t *testing.T) {
	for i, tt := range timeTests {
		val, err := tryParse(tt.str)
		if !val.Equal(tt.expected) {
			t.Errorf("%d: expected to parse '%v' into '%v'; got '%v'",
				i, tt.str, tt.expected, val)
		}
		if err != nil {
			t.Errorf("%d: got error: %v", i, err)
		}
	}
}

func TestTimestampWithTimeZone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("create temp table test (t timestamp with time zone)")
	if err != nil {
		t.Fatal(err)
	}

	// try several different locations, all included in Go's zoneinfo.zip
	for _, locName := range []string{
		"UTC",
		"America/Chicago",
		"America/New_York",
		"Australia/Darwin",
		"Australia/Perth",
	} {
		loc, err := time.LoadLocation(locName)
		if err != nil {
			t.Logf("Could not load time zone %s - skipping", locName)
			continue
		}

		// Postgres timestamps have a resolution of 1 microsecond, so don't
		// use the full range of the Nanosecond argument
		refTime := time.Date(2012, 11, 6, 10, 23, 42, 123456000, loc)
		_, err = tx.Exec("insert into test(t) values($1)", refTime)
		if err != nil {
			t.Fatal(err)
		}

		for _, pgTimeZone := range []string{"US/Eastern", "Australia/Darwin"} {
			// Switch Postgres's timezone to test different output timestamp formats
			_, err = tx.Exec(fmt.Sprintf("set time zone '%s'", pgTimeZone))
			if err != nil {
				t.Fatal(err)
			}

			var gotTime time.Time
			row := tx.QueryRow("select t from test")
			err = row.Scan(&gotTime)
			if err != nil {
				t.Fatal(err)
			}

			if !refTime.Equal(gotTime) {
				t.Errorf("timestamps not equal: %s != %s", refTime, gotTime)
			}
		}

		_, err = tx.Exec("delete from test")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTimestampWithOutTimezone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	test := func(ts, pgts string) {
		r, err := db.Query("SELECT $1::timestamp", pgts)
		if err != nil {
			t.Fatalf("Could not run query: %v", err)
		}

		n := r.Next()

		if n != true {
			t.Fatal("Expected at least one row")
		}

		var result time.Time
		err = r.Scan(&result)
		if err != nil {
			t.Fatalf("Did not expect error scanning row: %v", err)
		}

		expected, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			t.Fatalf("Could not parse test time literal: %v", err)
		}

		if !result.Equal(expected) {
			t.Fatalf("Expected time to match %v: got mismatch %v",
				expected, result)
		}

		n = r.Next()
		if n != false {
			t.Fatal("Expected only one row")
		}
	}

	test("2000-01-01T00:00:00Z", "2000-01-01T00:00:00")

	// Test higher precision time
	test("2013-01-04T20:14:58.80033Z", "2013-01-04 20:14:58.80033")
}

func TestStringWithNul(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	hello0world := string("hello\x00world")
	_, err := db.Query("SELECT $1::text", &hello0world)
	if err == nil {
		t.Fatal("Postgres accepts a string with nul in it; " +
			"injection attacks may be plausible")
	}
}

func TestByteToText(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	b := []byte("hello world")
	row := db.QueryRow("SELECT $1::text", b)

	var result []byte
	err := row.Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	if string(result) != string(b) {
		t.Fatalf("expected %v but got %v", b, result)
	}
}
