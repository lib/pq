package pqtime_test

import (
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/lib/pq/internal/pqtest"
	. "github.com/lib/pq/internal/pqtime"
)

func TestParse(t *testing.T) {
	var (
		notz   = time.FixedZone("", 0)
		m7     = time.FixedZone("", -7*60*60)               // -7
		p7     = time.FixedZone("", 7*60*60)                // +7
		m742   = time.FixedZone("", -(7*60*60 + 42*60))     // -7:42
		m73009 = time.FixedZone("", -(7*60*60 + 30*60 + 9)) // -7:30:09
		p73009 = time.FixedZone("", 7*60*60+30*60+9)        // +7:30:09
	)

	tests := []struct {
		str     string
		time    time.Time
		wantErr string
	}{
		{"22001-02-03", time.Date(22001, 2, 3, 0, 0, 0, 0, notz), ``},
		{"2001-02-03", time.Date(2001, 2, 3, 0, 0, 0, 0, notz), ``},
		{"0001-12-31 BC", time.Date(0, 12, 31, 0, 0, 0, 0, notz), ``},
		{"2001-02-03 BC", time.Date(-2000, 2, 3, 0, 0, 0, 0, notz), ``},
		{"2001-02-03 04:05:06", time.Date(2001, 2, 3, 4, 5, 6, 0, notz), ``},
		{"2001-02-03 04:05:06.000001", time.Date(2001, 2, 3, 4, 5, 6, 1000, notz), ``},
		{"2001-02-03 04:05:06.00001", time.Date(2001, 2, 3, 4, 5, 6, 10000, notz), ``},
		{"2001-02-03 04:05:06.0001", time.Date(2001, 2, 3, 4, 5, 6, 100000, notz), ``},
		{"2001-02-03 04:05:06.001", time.Date(2001, 2, 3, 4, 5, 6, 1000000, notz), ``},
		{"2001-02-03 04:05:06.01", time.Date(2001, 2, 3, 4, 5, 6, 10000000, notz), ``},
		{"2001-02-03 04:05:06.1", time.Date(2001, 2, 3, 4, 5, 6, 100000000, notz), ``},
		{"2001-02-03 04:05:06.12", time.Date(2001, 2, 3, 4, 5, 6, 120000000, notz), ``},
		{"2001-02-03 04:05:06.123", time.Date(2001, 2, 3, 4, 5, 6, 123000000, notz), ``},
		{"2001-02-03 04:05:06.1234", time.Date(2001, 2, 3, 4, 5, 6, 123400000, notz), ``},
		{"2001-02-03 04:05:06.12345", time.Date(2001, 2, 3, 4, 5, 6, 123450000, notz), ``},
		{"2001-02-03 04:05:06.123456", time.Date(2001, 2, 3, 4, 5, 6, 123456000, notz), ``},
		{"2001-02-03 04:05:06.123-07", time.Date(2001, 2, 3, 4, 5, 6, 123000000, m7), ``},
		{"2001-02-03 04:05:06-07", time.Date(2001, 2, 3, 4, 5, 6, 0, m7), ``},
		{"2001-02-03 04:05:06-07:42", time.Date(2001, 2, 3, 4, 5, 6, 0, m742), ``},
		{"2001-02-03 04:05:06-07:30:09", time.Date(2001, 2, 3, 4, 5, 6, 0, m73009), ``},
		{"2001-02-03 04:05:06+07:30:09", time.Date(2001, 2, 3, 4, 5, 6, 0, p73009), ``},
		{"2001-02-03 04:05:06+07", time.Date(2001, 2, 3, 4, 5, 6, 0, p7), ``},
		{"0011-02-03 04:05:06 BC", time.Date(-10, 2, 3, 4, 5, 6, 0, notz), ``},
		{"0011-02-03 04:05:06.123 BC", time.Date(-10, 2, 3, 4, 5, 6, 123000000, notz), ``},
		{"0011-02-03 04:05:06.123-07 BC", time.Date(-10, 2, 3, 4, 5, 6, 123000000, m7), ``},
		{"0001-02-03 04:05:06.123", time.Date(1, 2, 3, 4, 5, 6, 123000000, notz), ``},
		{"0001-02-03 04:05:06.123 BC", time.Date(1, 2, 3, 4, 5, 6, 123000000, notz).AddDate(-1, 0, 0), ``},
		{"0001-02-03 04:05:06.123 BC", time.Date(0, 2, 3, 4, 5, 6, 123000000, notz), ``},
		{"0002-02-03 04:05:06.123 BC", time.Date(0, 2, 3, 4, 5, 6, 123000000, notz).AddDate(-1, 0, 0), ``},
		{"0002-02-03 04:05:06.123 BC", time.Date(-1, 2, 3, 4, 5, 6, 123000000, notz), ``},
		{"12345-02-03 04:05:06.1", time.Date(12345, 2, 3, 4, 5, 6, 100000000, notz), ``},
		{"123456-02-03 04:05:06.1", time.Date(123456, 2, 3, 4, 5, 6, 100000000, notz), ``},

		{"BC", time.Time{}, `invalid timestamp`},
		{" BC", time.Time{}, `invalid timestamp`},
		{"2001", time.Time{}, `invalid timestamp`},
		{"2001-2-03", time.Time{}, `expected number; got '2001-2-03'`}, // TODO: need "invalid timestamp" in error
		{"2001-02-3", time.Time{}, `invalid timestamp`},
		{"2001-02-03 ", time.Time{}, `invalid timestamp`},
		{"2001-02-03 B", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04:", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04:05", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04:05 B", time.Time{}, `expected '58' at position 16; got '32'`},  // TODO: need "invalid timestamp"
		{"2001-02-03 04:05 BC", time.Time{}, `expected '58' at position 16; got '32'`}, // TODO: need "invalid timestamp"
		{"2001-02-03 04:05:", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04:05:6", time.Time{}, `invalid timestamp`},
		{"2001-02-03 04:05:06 B", time.Time{}, `expected end of input, got  B`},     // TODO: need "invalid timestamp"
		{"2001-02-03 04:05:06BC", time.Time{}, `expected end of input, got BC`},     // TODO: need "invalid timestamp"
		{"2001-02-03 04:05:06.123 B", time.Time{}, `expected end of input, got  B`}, // TODO: need "invalid timestamp"
	}

	db := pqtest.MustDB(t, "timezone='Etc/UTC'")
	t.Parallel()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.str, func(t *testing.T) {
			{ // Parse()
				have, err := Parse(nil, tt.str)
				if !pqtest.ErrorContains(err, tt.wantErr) {
					t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
				}
				// TODO: returns all sorts of wonky times on errors; should
				// return time.Time{}
				if tt.wantErr == "" && have.String() != tt.time.String() {
					t.Fatalf("Parse wrong\nhave: %v\nwant: %v", have, tt.time)
				}
			}

			if tt.wantErr == "" { // Round-trip time to PostgreSQL
				var dbstr string
				err := db.QueryRow("select ($1::timestamptz)::text", tt.time).Scan(&dbstr)
				if err != nil {
					t.Fatal(err)
				}

				have, err := Parse(nil, dbstr)
				if err != nil {
					t.Fatal(err)
				}
				have = have.In(tt.time.Location())
				if have.String() != tt.time.String() {
					t.Fatalf("Roundtrip wrong\nhave: %v\nwant: %v", have, tt.time)
				}
			}
		})
	}
}

func TestFormat(t *testing.T) {
	tests := []struct {
		in   time.Time
		want string
	}{
		{time.Time{}, "0001-01-01 00:00:00Z"},
		{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "2001-02-03 04:05:06.123456789Z"},
		{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "2001-02-03 04:05:06.123456789+02:00"},
		{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "2001-02-03 04:05:06.123456789-06:00"},
		{time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "2001-02-03 04:05:06-07:30:09"},

		{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03 04:05:06.123456789Z"},
		{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03 04:05:06.123456789+02:00"},
		{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03 04:05:06.123456789-06:00"},

		{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03 04:05:06.123456789Z BC"},
		{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03 04:05:06.123456789+02:00 BC"},
		{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03 04:05:06.123456789-06:00 BC"},

		{time.Date(1, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03 04:05:06-07:30:09"},
		{time.Date(0, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03 04:05:06-07:30:09 BC"},
	}

	db := pqtest.MustDB(t)
	t.Parallel()
	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			{ // Format()
				have := string(Format(tt.in))
				if have != tt.want {
					t.Fatalf("Format wrong\nhave: %v\nwant: %v", have, tt.want)
				}

				havep, err := Parse(nil, have)
				if err != nil {
					t.Fatal(err)
				}
				if !havep.Equal(tt.in) {
					t.Errorf("roundtrip failed\nhave: %s\nin:   %s", havep, tt.in)
				}
			}

			{ // Round-trip time to PostgreSQL
				var str string
				err := db.QueryRow("SELECT '2001-02-03T04:05:06.007-08:09:10'::time::text").Scan(&str)
				if err == nil {
					t.Fatalf("PostgreSQL is accepting an ISO timestamp input for time")
				}

				for _, typ := range []string{"date", "time", "timetz", "timestamp", "timestamptz"} {
					err := db.QueryRow("select $1::"+typ+"::text", tt.in).Scan(&str)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
