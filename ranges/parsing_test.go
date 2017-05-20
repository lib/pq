package ranges

import (
	"testing"
)

func TestReadRange(t *testing.T) {
	cases := []struct {
		Input string
		MinIn bool
		MaxIn bool
		Min   string
		Max   string
	}{
		{"[-1.23,98.0]", true, true, "-1.23", "98.0"},
		{"(1,2]", false, true, "1", "2"},
		{"[0,0.0]", true, true, "0", "0.0"},
		{"(1.29,-0.5)", false, false, "1.29", "-0.5"},
	}

	for _, tc := range cases {
		minIn, maxIn, min, max, err := readRange([]byte(tc.Input))
		if err != nil {
			t.Fatalf("unexpected error: " + err.Error())
		}
		if minIn != tc.MinIn {
			t.Fatalf("expected min to be inclusive=%t, got %t", tc.MinIn, minIn)
		}
		if maxIn != tc.MaxIn {
			t.Fatalf("expected max to be inclusive=%t, got %t", tc.MaxIn, maxIn)
		}
		if string(min) != tc.Min {
			t.Fatalf("expected min to be '%s', got '%s'", tc.Min, min)
		}
		if string(max) != tc.Max {
			t.Fatalf("expected max to be '%s', got '%s'", tc.Max, max)
		}
	}
}
