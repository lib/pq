package pqerror

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestAs(t *testing.T) {
	tests := []struct {
		err        error
		codes      []ErrorCode
		wantReturn bool
	}{
		{nil, nil, false},
		{nil, []ErrorCode{SyntaxError}, false},
		{errors.New("oh noes"), []ErrorCode{SyntaxError}, false},
		{&Error{Code: "00000", Message: "okay"}, nil, true},

		{&Error{Code: "00000", Message: "okay"}, []ErrorCode{SyntaxError}, false},
		{&Error{Code: "00000", Message: "okay"}, []ErrorCode{SyntaxError, SuccessfulCompletion}, true},
	}

	//t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := As(tt.err, tt.codes...)
			if tt.wantReturn {
				if !reflect.DeepEqual(have, tt.err) {
					t.Errorf("\nhave: %#v\nwant: %#v", have, tt.err)
				}
			} else {
				if have != nil {
					t.Errorf("expected return to be nil, but have:\n%#v", have)
				}
			}
		})
	}
}

func BenchmarkAs(b *testing.B) {
	b.Run("nil", func(b *testing.B) {
		var nilerr error
		for i := 0; i < b.N; i++ {
			_ = As(nilerr, SuccessfulCompletion)
		}
	})
	b.Run("other error", func(b *testing.B) {
		patherr := &os.PathError{}
		for i := 0; i < b.N; i++ {
			_ = As(patherr, SuccessfulCompletion)
		}
	})
	b.Run("pq.Error", func(b *testing.B) {
		pqerr := &Error{Code: "00000", Message: "okay"}
		for i := 0; i < b.N; i++ {
			_ = As(pqerr, SuccessfulCompletion)
		}
	})
}
