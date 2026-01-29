package pq

import (
	"strings"
	"testing"
	"time"
)

func TestClockScanUnsupported(t *testing.T) {
	var clock Clock
	err := clock.Scan(true)

	if err == nil {
		t.Fatal("Expected error when scanning from bool")
	}
	if !strings.Contains(err.Error(), "bool to Clock") {
		t.Errorf("Expected type to be mentioned when scanning, got %q", err)
	}
}

func TestClockScanTime(t *testing.T) {
	clock := Clock{9, 9, 9, 9}
	err := clock.Scan(time.Date(2001, time.February, 3, 4, 5, 6, 7, time.UTC))

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if clock != (Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7}) {
		t.Errorf("Expected 04:05:06.000000007, got %+v", clock)
	}
}

func TestClockScanBytes(t *testing.T) {
	for _, tt := range []struct {
		bytes []byte
		clock Clock
	}{
		{[]byte(`04:05:06`), Clock{Hour: 4, Minute: 5, Second: 6}},
		{[]byte(`04:05:06.007`), Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000000}},
		{[]byte(`04:05:06.000007`), Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000}},
	} {
		clock := Clock{9, 9, 9, 9}
		err := clock.Scan(tt.bytes)

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.bytes, err)
		}
		if clock != tt.clock {
			t.Errorf("Expected %+v, got %+v", tt.clock, clock)
		}
	}
}

func TestClockScanString(t *testing.T) {
	for _, tt := range []struct {
		str   string
		clock Clock
	}{
		{`04:05:06`, Clock{Hour: 4, Minute: 5, Second: 6}},
		{`04:05:06.007`, Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000000}},
		{`04:05:06.000007`, Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000}},
	} {
		clock := Clock{9, 9, 9, 9}
		err := clock.Scan(tt.str)

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.str, err)
		}
		if clock != tt.clock {
			t.Errorf("Expected %+v, got %+v", tt.clock, clock)
		}
	}
}

func TestClockValue(t *testing.T) {
	for _, tt := range []struct {
		str   string
		clock Clock
	}{
		{`04:05:06.000000000`, Clock{Hour: 4, Minute: 5, Second: 6}},
		{`04:05:06.007000000`, Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000000}},
		{`04:05:06.000007000`, Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7000}},
		{`04:05:06.000000007`, Clock{Hour: 4, Minute: 5, Second: 6, Nanosecond: 7}},
	} {
		value, err := tt.clock.Value()

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.clock, err)
		}
		if value != tt.str {
			t.Errorf("Expected %v, got %v", tt.str, value)
		}
	}
}

func TestDateScanUnsupported(t *testing.T) {
	var date Date
	err := date.Scan(true)

	if err == nil {
		t.Fatal("Expected error when scanning from bool")
	}
	if !strings.Contains(err.Error(), "bool to Date") {
		t.Errorf("Expected type to be mentioned when scanning, got %q", err)
	}
}

func TestDateScanTime(t *testing.T) {
	date := Date{9, 9, 9, 9}
	err := date.Scan(time.Date(2001, time.February, 3, 4, 5, 6, 7, time.UTC))

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if date != (Date{Year: 2001, Month: 2, Day: 3}) {
		t.Errorf("Expected 2001-02-03, got %v", date)
	}
}

func TestDateScanBytes(t *testing.T) {
	for _, tt := range []struct {
		bytes []byte
		date  Date
	}{
		{[]byte(`infinity`), Date{Infinity: 1}},
		{[]byte(`-infinity`), Date{Infinity: -1}},
		{[]byte(`2001-02-03`), Date{Year: 2001, Month: 2, Day: 3}},
	} {
		date := Date{9, 9, 9, 9}
		err := date.Scan(tt.bytes)

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.bytes, err)
		}
		if date != tt.date {
			t.Errorf("Expected %+v, got %+v", tt.date, date)
		}
	}
}

var DateStringTests = []struct {
	str  string
	date Date
}{
	{`infinity`, Date{Infinity: 1}},
	{`-infinity`, Date{Infinity: -1}},
	{`2001-02-03`, Date{Year: 2001, Month: 2, Day: 3}},
}

func TestDateScanString(t *testing.T) {
	for _, tt := range DateStringTests {
		date := Date{9, 9, 9, 9}
		err := date.Scan(tt.str)

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.str, err)
		}
		if date != tt.date {
			t.Errorf("Expected %+v, got %+v", tt.date, date)
		}
	}
}

func TestDateValue(t *testing.T) {
	for _, tt := range DateStringTests {
		value, err := tt.date.Value()

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.date, err)
		}
		if value != tt.str {
			t.Errorf("Expected %v, got %v", tt.str, value)
		}
	}
}
