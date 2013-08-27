package hstore

import (
	"testing"
)

func TestEncode(t *testing.T) {
	for i, test := range encDecTests {
		if got := Encode(test.in); got != test.out {
			t.Errorf("%d: want %q, got %q", i, test.out, got)
		}
	}
}
