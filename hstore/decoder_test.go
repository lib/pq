package hstore

import (
	"testing"
)

func TestDecode(t *testing.T) {
	for i, test := range encDecTests {
		if got := Decode(test.out); !mapsEqual(got, test.in) {
			t.Errorf("%d: want %q, got %q", i, test.in, got)
		}
	}
}

func mapsEqual(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k := range m1 {
		if m1[k] != m2[k] {
			return false
		}
	}
	return true
}
