package pq

import (
	"slices"
	"testing"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/pqerror"
)

// Called for the side-effect of setting the environment.
func init() { pqtest.DSN("") }

// mustAs calls As(), calling t.Fatal() if the error is nil or if this fails.
//
// This should probably be in pqtest, but can't right now due to import cycles,
// and using pq_test package requires some refactoring as it refers to
// unexported symbols.
func mustAs(t *testing.T, err error, codes ...pqerror.Code) *Error {
	t.Helper()
	pqErr := As(err)
	if pqErr == nil {
		t.Fatalf("mustAs: not *pq.Error: %T", err)
	}
	if len(codes) > 0 && !slices.Contains(codes, pqErr.Code) {
		t.Fatalf("mustAs: wrong error %q (code not one of %s)", pqErr.Error(), codes)
	}
	return pqErr
}
