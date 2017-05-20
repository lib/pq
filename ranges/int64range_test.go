package ranges

import (
	"testing"
)

func TestInt64RangeString(t *testing.T) {
	test := func(lower, upper int64, expect string) {
		s := Int64Range{lower, upper}.String()
		if s != expect {
			t.Errorf("expected '%s', got '%s'", expect, s)
		}
	}

	test(0, 2, "[0,2)")
	test(0, 0, "[0,0)")
	test(-2, 8, "[-2,8)")
	test(8, -2, "[8,-2)")
}
