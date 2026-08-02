package pq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lib/pq/pqerror"
)

// Error returned by the PostgreSQL server.
//
// The [Error] method returns the error message and error code:
//
//	pq: invalid input syntax for type json (22P02)
//
// The [ErrorWithDetail] method also includes the error Detail, Hint, and
// location context (if any):
//
//	ERROR:   invalid input syntax for type json (22P02)
//	DETAIL:  Token "asd" is invalid.
//	CONTEXT: line 5, column 8:
//
//	 3 | 'def',
//	 4 | 123,
//	 5 | 'foo', 'asd'::jsonb
//	            ^
type Error struct {
	// [Efatal], [Epanic], [Ewarning], [Enotice], [Edebug], [Einfo], or [Elog].
	// Always present.
	Severity string

	// SQLSTATE code. Always present.
	Code pqerror.Code

	// Primary human-readable error message. This should be accurate but terse
	// (typically one line). Always present.
	Message string

	// Optional secondary error message carrying more detail about the problem.
	// Might run to multiple lines.
	Detail string

	// Optional suggestion what to do about the problem. This is intended to
	// differ from Detail in that it offers advice (potentially inappropriate)
	// rather than hard facts. Might run to multiple lines.
	Hint string

	// error position as an index into the original query string, as decimal
	// ASCII integer. The first character has index 1, and positions are
	// measured in characters not bytes.
	Position string

	// This is defined the same as the Position field, but it is used when the
	// cursor position refers to an internally generated command rather than the
	// one submitted by the client. The InternalQuery field will always appear
	// when this field appears.
	InternalPosition string

	// Text of a failed internally-generated command. This could be, for
	// example, an SQL query issued by a PL/pgSQL function.
	InternalQuery string

	// An indication of the context in which the error occurred. Presently this
	// includes a call stack traceback of active procedural language functions
	// and internally-generated queries. The trace is one entry per line, most
	// recent first.
	Where string

	// If the error was associated with a specific database object, the name of
	// the schema containing that object, if any.
	Schema string

	// If the error was associated with a specific table, the name of the table.
	// (Refer to the schema name field for the name of the table's schema.)
	Table string

	// If the error was associated with a specific table column, the name of the
	// column. (Refer to the schema and table name fields to identify the
	// table.)
	Column string

	// If the error was associated with a specific data type, the name of the
	// data type. (Refer to the schema name field for the name of the data
	// type's schema.)
	DataTypeName string

	// If the error was associated with a specific constraint, the name of the
	// constraint. Refer to fields listed above for the associated table or
	// domain. (For this purpose, indexes are treated as constraints, even if
	// they weren't created with constraint syntax.)
	Constraint string

	// File name of the source-code location where the error was reported.
	File string

	// Line number of the source-code location where the error was reported.
	Line string

	// Name of the source-code routine reporting the error.
	Routine string

	query string
}

type (
	// ErrorCode is a five-character error code.
	//
	// Deprecated: use pqerror.Code
	//
	//go:fix inline
	ErrorCode = pqerror.Code

	// ErrorClass is only the class part of an error code.
	//
	// Deprecated: use pqerror.Class
	//
	//go:fix inline
	ErrorClass = pqerror.Class
)

func parseError(r *readBuf, q string) *Error {
	payload := *r
	// The connection reuses payload storage for subsequent messages. Copy all
	// fields once and let Error's strings slice that immutable backing store.
	fields := string(payload)
	err := &Error{query: q}
	position := 0
	for {
		if position >= len(fields) {
			panic(errors.New("pq: invalid message format; expected error terminator"))
		}
		t := fields[position]
		position++
		if t == 0 {
			break
		}
		end := strings.IndexByte(fields[position:], 0)
		if end < 0 {
			panic(errors.New("pq: invalid message format; expected string terminator"))
		}
		end += position
		msg := fields[position:end]
		position = end + 1
		switch t {
		case 'S':
			err.Severity = msg
		case 'C':
			err.Code = pqerror.Code(msg)
		case 'M':
			err.Message = msg
		case 'D':
			err.Detail = msg
		case 'H':
			err.Hint = msg
		case 'P':
			err.Position = msg
		case 'p':
			err.InternalPosition = msg
		case 'q':
			err.InternalQuery = msg
		case 'W':
			err.Where = msg
		case 's':
			err.Schema = msg
		case 't':
			err.Table = msg
		case 'c':
			err.Column = msg
		case 'd':
			err.DataTypeName = msg
		case 'n':
			err.Constraint = msg
		case 'F':
			err.File = msg
		case 'L':
			err.Line = msg
		case 'R':
			err.Routine = msg
		}
	}
	*r = payload[position:]
	return err
}

// Fatal returns true if the Error Severity is fatal.
func (e *Error) Fatal() bool { return e.Severity == pqerror.SeverityFatal }

// SQLState returns the SQLState of the error.
func (e *Error) SQLState() string { return string(e.Code) }

func (e *Error) Error() string {
	line, col, hasPosition := e.position()

	var b strings.Builder
	b.Grow(len(e.Message) + len(e.Code) + 32)
	b.WriteString("pq: ")
	b.WriteString(e.Message)
	if e.query != "" && e.Position != "" {
		if hasPosition {
			if strings.IndexByte(e.query, '\n') < 0 {
				b.WriteString(" at column ")
				writeErrorInt(&b, col)
			} else {
				b.WriteString(" at position ")
				writeErrorInt(&b, line)
				b.WriteByte(':')
				writeErrorInt(&b, col)
			}
		}
	}

	if e.Code != "" {
		b.WriteString(" (")
		b.WriteString(string(e.Code))
		b.WriteByte(')')
	}
	return b.String()
}

// ErrorWithDetail returns the error message with detailed information and
// location context (if any).
//
// See the documentation on [Error].
func (e *Error) ErrorWithDetail() string {
	line, col, hasPosition := e.position()
	var previous2, previous1, current string
	if hasPosition {
		previous2, previous1, current = errorContextLines(e.query, line)
	}

	capacity := len(e.Message) + len(e.Detail) + len(e.Hint) + 30
	if hasPosition {
		capacity += expandedTabLen(previous2) + expandedTabLen(previous1) + expandedTabLen(current) + col + 96
	}
	var b strings.Builder
	b.Grow(capacity)
	b.WriteString("ERROR:   ")
	b.WriteString(e.Message)
	if e.Code != "" {
		b.WriteString(" (")
		b.WriteString(string(e.Code))
		b.WriteByte(')')
	}
	if e.Detail != "" {
		b.WriteString("\nDETAIL:  ")
		b.WriteString(e.Detail)
	}
	if e.Hint != "" {
		b.WriteString("\nHINT:    ")
		b.WriteString(e.Hint)
	}

	if hasPosition {
		b.WriteString("\nCONTEXT: line ")
		writeErrorInt(&b, line)
		b.WriteString(", column ")
		writeErrorInt(&b, col)
		b.WriteString(":\n\n")
		if line > 2 {
			writeErrorSourceLine(&b, line-2, previous2)
		}
		if line > 1 {
			writeErrorSourceLine(&b, line-1, previous1)
		}
		/// Expand tabs, so that the ^ is at at the correct position, but leave
		/// "column 10-13" intact. Adjusting this to the visual column would be
		/// better, but we don't know the tabsize of the user in their editor,
		/// which can be 8, 4, 2, or something else. We can't know. So leaving
		/// it as the character index is probably the "most correct".
		expandedLength := expandedTabLen(current)
		writeErrorSourceLine(&b, line, current)
		writeErrorSpaces(&b, 10+col-1+expandedLength-len(current))
		b.WriteString("^\n")
	}

	return b.String()
}

func (e *Error) position() (line, col int, ok bool) {
	if e.query == "" || e.Position == "" {
		return 0, 0, false
	}
	pos, err := strconv.Atoi(e.Position)
	if err != nil {
		return 0, 0, false
	}
	return queryPosition(e.query, pos)
}

func queryPosition(query string, pos int) (line, col int, ok bool) {
	if pos < 1 {
		return 0, 0, false
	}
	read := 0
	line = 1
	for {
		newline := strings.IndexByte(query, '\n')
		text := query
		if newline >= 0 {
			text = query[:newline]
		}
		lineLength := utf8.RuneCountInString(text) + 1
		if read+lineLength >= pos {
			return line, pos - read, true
		}
		if newline < 0 {
			return 0, 0, false
		}
		read += lineLength
		line++
		query = query[newline+1:]
	}
}

func errorContextLines(query string, target int) (previous2, previous1, current string) {
	line := 1
	for {
		newline := strings.IndexByte(query, '\n')
		text := query
		if newline >= 0 {
			text = query[:newline]
		}
		switch line {
		case target - 2:
			previous2 = text
		case target - 1:
			previous1 = text
		case target:
			return previous2, previous1, text
		}
		if newline < 0 {
			return previous2, previous1, current
		}
		line++
		query = query[newline+1:]
	}
}

func writeErrorSourceLine(b *strings.Builder, line int, source string) {
	writeErrorPaddedInt(b, line, 7)
	b.WriteString(" | ")
	writeExpandedTabs(b, source)
	b.WriteByte('\n')
}

func writeErrorPaddedInt(b *strings.Builder, value, width int) {
	var scratch [20]byte
	digits := strconv.AppendInt(scratch[:0], int64(value), 10)
	padding := width - len(digits)
	if padding < 1 {
		// The previous % 7d formatting always reserved a leading sign space
		// for positive values, even when the digits exceeded the field width.
		padding = 1
	}
	writeErrorSpaces(b, padding)
	b.Write(digits)
}

func writeErrorInt(b *strings.Builder, value int) {
	var scratch [20]byte
	b.Write(strconv.AppendInt(scratch[:0], int64(value), 10))
}

func writeErrorSpaces(b *strings.Builder, count int) {
	const spaces = "                                                                "
	for count > len(spaces) {
		b.WriteString(spaces)
		count -= len(spaces)
	}
	if count > 0 {
		b.WriteString(spaces[:count])
	}
}

func writeExpandedTabs(b *strings.Builder, s string) {
	l := 0
	for _, r := range s {
		switch r {
		case '\t':
			tw := 8 - l%8
			writeErrorSpaces(b, tw)
			l += tw
		default:
			b.WriteRune(r)
			l += 1
		}
	}
}

func expandedTabLen(s string) int {
	length, columns := 0, 0
	for _, r := range s {
		if r == '\t' {
			width := 8 - columns%8
			length += width
			columns += width
		} else {
			length += utf8.RuneLen(r)
			columns++
		}
	}
	return length
}

func (cn *conn) handleError(reported error, query ...string) error {
	switch err := reported.(type) {
	case nil:
		return nil
	case runtime.Error, *net.OpError:
		cn.err.set(driver.ErrBadConn)
	case *safeRetryError:
		cn.err.set(driver.ErrBadConn)
		reported = driver.ErrBadConn
	case *Error:
		if len(query) > 0 && query[0] != "" {
			err.query = query[0]
			reported = err
		}
		if err.Fatal() {
			reported = driver.ErrBadConn
		}
	case error:
		if err == io.EOF || err == io.ErrUnexpectedEOF || err.Error() == "remote error: handshake failure" {
			reported = driver.ErrBadConn
		}
	default:
		cn.err.set(driver.ErrBadConn)
		reported = fmt.Errorf("pq: unknown error %T: %[1]s", err)
	}

	// Any time we return ErrBadConn, we need to remember it since *Tx doesn't
	// mark the connection bad in database/sql.
	if reported == driver.ErrBadConn {
		cn.err.set(driver.ErrBadConn)
	}
	return reported
}
