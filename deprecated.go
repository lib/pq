package pq

// PGError is an interface used by previous versions of pq.
//
// Deprecated: use the Error type. This is never used.
type PGError interface {
	Error() string
	Fatal() bool
	Get(k byte) (v string)
}

// Get implements the legacy PGError interface.
//
// Deprecated: new code should use the fields of the Error struct directly.
func (e *Error) Get(k byte) (v string) {
	switch k {
	case 'S':
		return e.Severity
	case 'C':
		return string(e.Code)
	case 'M':
		return e.Message
	case 'D':
		return e.Detail
	case 'H':
		return e.Hint
	case 'P':
		return e.Position
	case 'p':
		return e.InternalPosition
	case 'q':
		return e.InternalQuery
	case 'W':
		return e.Where
	case 's':
		return e.Schema
	case 't':
		return e.Table
	case 'c':
		return e.Column
	case 'd':
		return e.DataTypeName
	case 'n':
		return e.Constraint
	case 'F':
		return e.File
	case 'L':
		return e.Line
	case 'R':
		return e.Routine
	}
	return ""
}

// ParseURL converts a url to a connection string for driver.Open.
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }
