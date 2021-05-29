package ranges

import (
	"testing"
)

func TestInt32RangeString(t *testing.T) {
	test := func(lower, upper int32, expect string) {
		s := Int32Range{lower, upper}.String()
		if s != expect {
			t.Errorf("expected '%s', got '%s'", expect, s)
		}
	}

	test(0, 2, "[0,2)")
	test(0, 0, "[0,0)")
	test(-2, 8, "[-2,8)")
	test(8, -2, "[8,-2)")
}

func TestInt32RangeValue(t *testing.T) {
	expectError := func(lower, upper int32) {
		r := Int32Range{lower, upper}
		if _, err := r.Value(); err == nil {
			t.Errorf("expected an error for '%s' but did not get one", r.String())
		}
	}

	expectError(2, 0)
	expectError(8, -4)
	expectError(-8, -9)
}
