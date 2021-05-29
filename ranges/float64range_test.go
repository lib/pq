package ranges

import (
	"testing"
)

func TestFloat64RangeString(t *testing.T) {
	test := func(lower, upper float64, lowerIn, upperIn bool, expect string) {
		s := Float64Range{lower, lowerIn, upper, upperIn}.String()
		if s != expect {
			t.Errorf("expected '%s', got '%s'", expect, s)
		}
	}

	test(-1.0, 2.1, false, true, "(-1.000000,2.100000]")
	test(9.99, 0.01, true, true, "[9.990000,0.010000]")
	test(80.0, 90.0, false, false, "(80.000000,90.000000)")
}
