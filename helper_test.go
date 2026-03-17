package pq

import (
	"testing"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/pqerror"
)

// Called for the side-effect of setting the environment.
func init() { pqtest.DSN("") }

const cancelErrorCode pqerror.Code = "57014"

// pqError converts an error to *pq.Error, calling t.Fatal() if the error is nil
// or if this fails.
//
// This should probably be in pqtest, but can't right now due to import cycles,
// and using pq_test package requires some refactoring as it refers to
// unexported symbols.
func pqError(t *testing.T, err error) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("pqError: error is nil")
	}
	pqErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("wrong error %T: %[1]s", err)
	}
	return pqErr
}
