package pq

// findPQError is the type-specific equivalent of errors.As for *Error. It
// avoids reflection and target allocations on ordinary single- and multi-error
// unwrap chains while preserving custom As method ordering.
func findPQError(err error) (*Error, bool) {
	for err != nil {
		if pqErr, ok := err.(*Error); ok {
			return pqErr, true
		}
		if custom, ok := err.(interface{ As(any) bool }); ok {
			var pqErr *Error
			if custom.As(&pqErr) {
				return pqErr, true
			}
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				if child == nil {
					continue
				}
				if pqErr, ok := findPQError(child); ok {
					return pqErr, true
				}
			}
			return nil, false
		default:
			return nil, false
		}
	}
	return nil, false
}
