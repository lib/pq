package pq

import (
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"runtime"

	"github.com/lib/pq/pqerror"
)

// [pq.Error.Severity] values.
const (
	Efatal   = pqerror.SeverityFatal
	Epanic   = pqerror.SeverityPanic
	Ewarning = pqerror.SeverityWarning
	Enotice  = pqerror.SeverityNotice
	Edebug   = pqerror.SeverityDebug
	Einfo    = pqerror.SeverityInfo
	Elog     = pqerror.SeverityLog
)

// Error represents an error communicating with the server.
//
// The [Error] method only returns the error message and error code:
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
//
// See http://www.postgresql.org/docs/current/static/protocol-error-fields.html for details of the fields
type Error = pqerror.Error

// ErrorCode is a five-character error code.
type ErrorCode = pqerror.ErrorCode

// ErrorClass is only the class part of an error code.
type ErrorClass = pqerror.ErrorClass

func parseError(r *readBuf, q string) *Error {
	err := &Error{Query: q}
	for t := r.byte(); t != 0; t = r.byte() {
		msg := r.string()
		switch t {
		case 'S':
			err.Severity = msg
		case 'C':
			err.Code = ErrorCode(msg)
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
	return err
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
			err.Query = query[0]
			reported = err
		}
		if err.Fatal() {
			reported = driver.ErrBadConn
		}
	case error:
		if err == io.EOF || err.Error() == "remote error: handshake failure" {
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
