package pq

import (
	"fmt"
	"testing"
	"time"
)

func TestScanTimestamp(t *testing.T) {
	var nt NullTime
	tn := time.Now()
	(&nt).Scan(tn)
	if !nt.Valid {
		t.Errorf("Expected Valid=false")
	}
	if nt.Time != tn {
		t.Errorf("Time value mismatch")
	}
}

func TestScanNilTimestamp(t *testing.T) {
	var nt NullTime
	(&nt).Scan(nil)
	if nt.Valid {
		t.Errorf("Expected Valid=false")
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
