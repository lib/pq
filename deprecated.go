package pq

import (
	"bytes"
	"database/sql"
)

// PGError is an interface used by previous versions of pq.
//
// Deprecated: use the Error type. This is never used.
type PGError interface {
	Error() string
	Fatal() bool
	Get(k byte) (v string)
}

// ParseURL converts a url to a connection string for driver.Open.
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }

// NullTime represents a [time.Time] that may be null.
//
// Deprecated: this is an alias for [sql.NullTime].
//
//go:fix inline
type NullTime = sql.NullTime

// CopyIn creates a COPY FROM statement which can be prepared with Tx.Prepare().
// The target table should be visible in search_path.
//
// It copies all columns if the list of columns is empty.
//
// Deprecated: there is no need to use this query builder, you can use:
//
//	tx.Prepare("copy tbl (col1, col2) from stdin")
func CopyIn(table string, columns ...string) string {
	b := bytes.NewBufferString("COPY ")
	BufferQuoteIdentifier(table, b)
	makeStmt(b, columns...)
	return b.String()
}

// CopyInSchema creates a COPY FROM statement which can be prepared with
// Tx.Prepare().
//
// Deprecated: there is no need to use this query builder, you can use:
//
//	tx.Prepare("copy schema.tbl (col1, col2) from stdin")
func CopyInSchema(schema, table string, columns ...string) string {
	b := bytes.NewBufferString("COPY ")
	BufferQuoteIdentifier(schema, b)
	b.WriteRune('.')
	BufferQuoteIdentifier(table, b)
	makeStmt(b, columns...)
	return b.String()
}

func makeStmt(b *bytes.Buffer, columns ...string) {
	if len(columns) == 0 {
		b.WriteString(" FROM STDIN")
		return
	}
	b.WriteString(" (")
	for i, col := range columns {
		if i != 0 {
			b.WriteString(", ")
		}
		BufferQuoteIdentifier(col, b)
	}
	b.WriteString(") FROM STDIN")
}
