package pqerror

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
