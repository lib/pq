package pq

import (
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"runtime"
)

const (
	Efatal   = "FATAL"
	Epanic   = "PANIC"
	Ewarning = "WARNING"
	Enotice  = "NOTICE"
	Edebug   = "DEBUG"
	Einfo    = "INFO"
	Elog     = "LOG"
)

// Error represents an error communicating with the server.
//
// See http://www.postgresql.org/docs/current/static/protocol-error-fields.html for details of the fields
type Error struct {
	Severity         string
	Code             ErrorCode
	Message          string
	Detail           string
	Hint             string
	Position         string
	InternalPosition string
	InternalQuery    string
	Where            string
	Schema           string
	Table            string
	Column           string
	DataTypeName     string
	Constraint       string
	File             string
	Line             string
	Routine          string
}

func parseError(r *readBuf) *Error {
	err := new(Error)
	for t := r.byte(); t != 0; t = r.byte() {
		msg := r.string()
		switch t {
		case 'S':
			err.Severity = msg
		case 'C':
			err.Code = msg
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

func (err *Error) Fatal() bool {
	return err.Severity == Efatal
}

func (err Error) Error() string {
	return "pq: " + err.Message
}

func errorf(s string, args ...interface{}) {
	panic(fmt.Errorf("pq: %s", fmt.Sprintf(s, args...)))
}

func errRecover(err *error) {
	e := recover()
	switch v := e.(type) {
	case nil:
		// Do nothing
	case runtime.Error:
		panic(v)
	case *Error:
		if v.Fatal() {
			*err = driver.ErrBadConn
		} else {
			*err = v
		}
	case *net.OpError:
		*err = driver.ErrBadConn
	case error:
		if v == io.EOF || v.(error).Error() == "remote error: handshake failure" {
			*err = driver.ErrBadConn
		} else {
			*err = v
		}

	default:
		panic(fmt.Sprintf("unknown error: %#v", e))
	}
}
