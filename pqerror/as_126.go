//go:build go1.26

package pqerror

import (
	"errors"
	"slices"
)

// As asserts that the given error is [pqerror.Error] and returns it.
//
// It will return nil if the Error is not one of the given error codes. If no
// codes are given it will always return the Error.
//
// This is safe to call with a nil error.
func As(err error, codes ...ErrorCode) *Error {
	if pqErr, ok := errors.AsType[*Error](err); ok && (len(codes) == 0 || slices.Contains(codes, pqErr.Code)) {
		return pqErr
	}
	return nil
}
