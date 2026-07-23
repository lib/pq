package pq

import (
	"testing"
	"time"
)

func TestParseInterval(t *testing.T) {
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"00:00:01", time.Second},
		{"00:01:00", time.Minute},
		{"01:00:00", time.Hour},
		{"1 day", 24 * time.Hour},
		{"2 days", 48 * time.Hour},
		{"1 day 02:03:04", 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second},
		{"3 days 04:05:06.5", 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6500*time.Millisecond},
		{"-1 days", -24 * time.Hour},
		{"-02:03:04", -(2*time.Hour + 3*time.Minute + 4*time.Second)},
		{"1 hour 2 minutes 3 seconds", time.Hour + 2*time.Minute + 3*time.Second},
		{"90 minutes", 90 * time.Minute},
		{"P1DT2H3M4S", 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second},
		{"PT15M", 15 * time.Minute},
		{"@ 1 day", 24 * time.Hour},
	}
	for _, tc := range tests {
		got, err := ParseInterval(tc.in)
		if err != nil {
			t.Fatalf("ParseInterval(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseInterval(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseIntervalRejectsMonthsYears(t *testing.T) {
	for _, in := range []string{"1 mon", "2 months", "1 year", "P1M", "P1Y"} {
		if _, err := ParseInterval(in); err == nil {
			t.Fatalf("expected error for %q", in)
		}
	}
}

func TestDurationScanValue(t *testing.T) {
	var d Duration
	if err := d.Scan("1 day 01:00:00"); err != nil {
		t.Fatal(err)
	}
	if time.Duration(d) != 25*time.Hour {
		t.Fatalf("got %v", d)
	}
	v, err := d.Value()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok || s == "" {
		t.Fatalf("Value=%T %v", v, v)
	}
	// round-trip
	got, err := ParseInterval(s)
	if err != nil || got != 25*time.Hour {
		t.Fatalf("round-trip %q -> %v err=%v", s, got, err)
	}
}

func TestNullDuration(t *testing.T) {
	var n NullDuration
	if err := n.Scan(nil); err != nil || n.Valid {
		t.Fatalf("NULL: %+v err=%v", n, err)
	}
	if err := n.Scan([]byte("00:00:05")); err != nil || !n.Valid || n.Duration != 5*time.Second {
		t.Fatalf("scan: %+v err=%v", n, err)
	}
	v, err := n.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v.(string) == "" {
		t.Fatal("empty value")
	}
	n.Valid = false
	v, err = n.Value()
	if err != nil || v != nil {
		t.Fatalf("null value: %v %v", v, err)
	}
}

func TestFormatInterval(t *testing.T) {
	if got := formatInterval(0); got != "00:00:00" {
		t.Fatal(got)
	}
	if got := formatInterval(time.Second); got != "00:00:01" {
		t.Fatal(got)
	}
	if got := formatInterval(25 * time.Hour); got != "1 day 01:00:00" {
		t.Fatal(got)
	}
}
