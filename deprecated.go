package pq

import (
	"net"
	"path/filepath"
	"strings"
)

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
// Example:
//
//	"postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
//
// converts to:
//
//	"user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }

type values map[string]string

func (o values) network() (string, string) {
	host := o["host"]
	// UNIX domain sockets are either represented by an (absolute) file system
	// path or they live in the abstract name space (starting with an @).
	if filepath.IsAbs(host) || strings.HasPrefix(host, "@") {
		sockPath := filepath.Join(host, ".s.PGSQL."+o["port"])
		return "unix", sockPath
	}
	return "tcp", net.JoinHostPort(host, o["port"])
}
