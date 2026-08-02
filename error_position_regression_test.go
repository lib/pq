package pq

import "testing"

func TestErrorRegressionOutOfRangePositionDoesNotPanic(t *testing.T) {
	err := &Error{
		Message:  "backend supplied an invalid cursor position",
		Position: "999",
		query:    "select 1",
	}

	panicValue, _ := regressionCallWithoutPanic(func() error {
		_ = err.ErrorWithDetail()
		return nil
	})
	if panicValue != nil {
		t.Fatalf("out-of-range backend error position caused a panic: %v", panicValue)
	}
}
