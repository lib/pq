package pq

import (
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq/oid"
)

var timeTests = []struct {
	str     string
	timeval time.Time
}{
	{"22001-02-03", time.Date(22001, time.February, 3, 0, 0, 0, 0, time.FixedZone("", 0))},
	{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.FixedZone("", 0))},
	{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000,
		time.FixedZone("", -7*60*60))},
	{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0,
		time.FixedZone("", -7*60*60))},
	{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0,
		time.FixedZone("", -(7*60*60+42*60)))},
	{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0,
		time.FixedZone("", -(7*60*60+30*60+9)))},
	{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0,
		time.FixedZone("", 7*60*60))},
	{"0011-02-03 04:05:06 BC", time.Date(-10, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 0))},
	{"0011-02-03 04:05:06.123 BC", time.Date(-10, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	{"0011-02-03 04:05:06.123-07 BC", time.Date(-10, time.February, 3, 4, 5, 6, 123000000,
		time.FixedZone("", -7*60*60))},
	{"0001-02-03 04:05:06.123", time.Date(1, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	{"0001-02-03 04:05:06.123 BC", time.Date(1, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0)).AddDate(-1, 0, 0)},
	{"0001-02-03 04:05:06.123 BC", time.Date(0, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	{"0002-02-03 04:05:06.123 BC", time.Date(0, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0)).AddDate(-1, 0, 0)},
	{"0002-02-03 04:05:06.123 BC", time.Date(-1, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	{"12345-02-03 04:05:06.1", time.Date(12345, time.February, 3, 4, 5, 6, 100000000, time.FixedZone("", 0))},
	{"123456-02-03 04:05:06.1", time.Date(123456, time.February, 3, 4, 5, 6, 100000000, time.FixedZone("", 0))},
}

// Helper function for the two tests below
func tryParse(str string) (t time.Time, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
			return
		}
	}()
	i := parseTs(nil, str)
	t, ok := i.(time.Time)
	if !ok {
		err = fmt.Errorf("Not a time.Time type, got %#v", i)
	}
	return
}

// Test that parsing the string results in the expected value.
func TestParseTs(t *testing.T) {
	for i, tt := range timeTests {
		val, err := tryParse(tt.str)
		if err != nil {
			t.Errorf("%d: got error: %v", i, err)
		} else if val.String() != tt.timeval.String() {
			t.Errorf("%d: expected to parse %q into %q; got %q",
				i, tt.str, tt.timeval, val)
		}
	}
}

// Now test that sending the value into the database and parsing it back
// returns the same time.Time value.
func TestEncodeAndParseTs(t *testing.T) {
	db, err := openTestConnConninfo("timezone='Etc/UTC'")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i, tt := range timeTests {
		var dbstr string
		err = db.QueryRow("SELECT ($1::timestamptz)::text", tt.timeval).Scan(&dbstr)
		if err != nil {
			t.Errorf("%d: could not send value %q to the database: %s", i, tt.timeval, err)
			continue
		}

		val, err := tryParse(dbstr)
		if err != nil {
			t.Errorf("%d: could not parse value %q: %s", i, dbstr, err)
			continue
		}
		val = val.In(tt.timeval.Location())
		if val.String() != tt.timeval.String() {
			t.Errorf("%d: expected to parse %q into %q; got %q", i, dbstr, tt.timeval, val)
		}
	}
}

var formatTimeTests = []struct {
	time     time.Time
	expected string
	pgtypOid oid.Oid
}{
	{time.Time{}, "0001-01-01T00:00:00Z", oid.T_timestamp},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "2001-02-03T04:05:06.123456789Z", oid.T_timestamp},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "2001-02-03T04:05:06.123456789+02:00", oid.T_timestamp},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "2001-02-03T04:05:06.123456789-06:00", oid.T_timestamp},
	{time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "2001-02-03T04:05:06-07:30:09", oid.T_timestamp},

	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03T04:05:06.123456789Z", oid.T_timestamp},
	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03T04:05:06.123456789+02:00", oid.T_timestamp},
	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03T04:05:06.123456789-06:00", oid.T_timestamp},

	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03T04:05:06.123456789Z BC", oid.T_timestamp},
	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03T04:05:06.123456789+02:00 BC", oid.T_timestamp},
	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03T04:05:06.123456789-06:00 BC", oid.T_timestamp},

	{time.Date(1, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03T04:05:06-07:30:09", oid.T_timestamp},
	{time.Date(0, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03T04:05:06-07:30:09 BC", oid.T_timestamp},

	{time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 0)), "2001-02-03", oid.T_date},
	{time.Date(2001, time.February, 3, 16, 5, 6, 0, time.FixedZone("", 0)), "16:05:06", oid.T_time},
	{time.Date(2001, time.February, 3, 16, 5, 6, 0, time.FixedZone("", -7*60*60)), "16:05:06-0700", oid.T_timetz},
}

func TestFormatTs(t *testing.T) {
	for i, tt := range formatTimeTests {
		val := string(formatTs(tt.time, tt.pgtypOid))
		if val != tt.expected {
			t.Error(tt.time.Zone())
			t.Errorf("%d: incorrect time format %q, want %q", i, val, tt.expected)
		}
	}
}
