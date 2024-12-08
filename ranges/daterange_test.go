package ranges

import (
	"testing"
	"time"
)

func TestDateRangeScan(t *testing.T) {
	test := func(input string, lowers, uppers string) {
		r := DateRange{}
		if err := r.Scan([]byte(input)); err != nil {
			t.Fatalf("unexpected error: " + err.Error())
		}
		lower, _ := time.Parse("2006-01-02", lowers)
		upper, _ := time.Parse("2006-01-02", uppers)
		if !r.Lower.Equal(lower) {
			t.Errorf("expected lower date '%v', got '%v'", lower, r.Lower)
		}
		if !r.Upper.Equal(upper) {
			t.Errorf("expected upper date '%v', got '%v'", upper, r.Upper)
		}
	}

	test("[2000-01-01,2017-05-09)", "2000-01-01", "2017-05-09")
	test("[2000-01-01,)", "2000-01-01", "0001-01-01")
	test("[,2000-01-01)", "0001-01-01", "2000-01-01")
}

func TestDateRangeString(t *testing.T) {
	test := func(lowers, uppers string, expect string) {
		var lower, upper time.Time
		if lowers != "" {
			lower, _ = time.Parse("2006-01-02", lowers)
		}
		if uppers != "" {
			upper, _ = time.Parse("2006-01-02", uppers)
		}
		if s := (DateRange{lower, upper}).String(); s != expect {
			t.Errorf("expected '%s', got '%s'", expect, s)
		}
	}

	test("2001-06-02", "2007-05-04", "[2001-06-02,2007-05-04)")
	test("2001-06-02", "", "[2001-06-02,)")
	test("", "2001-06-02", "(,2001-06-02)")
	test("", "", "(,)")
}

func TestDateRangeValueError(t *testing.T) {
	expectError := func(lowers, uppers string) {
		lower, _ := time.Parse("2006-01-02 15:04:05", lowers)
		upper, _ := time.Parse("2006-01-02 15:04:05", uppers)
		r := DateRange{lower, upper}
		if _, err := r.Value(); err == nil {
			t.Errorf("expected an error for '%s' but did not get one", r.String())
		}
	}

	expectError("2001-01-02 00:00:00", "2001-01-01 00:00:00")
	expectError("2001-02-01 00:00:00", "2001-01-01 00:00:00")
	expectError("2001-02-01 12:00:03", "2001-01-01 00:00:00")
	expectError("2001-02-01 00:00:00", "2001-01-01 13:00:00")
}
