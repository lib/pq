package pq

import (
	"bytes"
	"strings"
)

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement. For example:
//
//	tblname := "my_table"
//	data := "my_data"
//	quoted := pq.QuoteIdentifier(tblname)
//	err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped. The quoted identifier will be case
// sensitive when used in a query. If the input string contains a zero byte, the
// result will be truncated immediately before it.
func QuoteIdentifier(name string) string {
	end := strings.IndexByte(name, 0)
	if end > -1 {
		name = name[:end]
	}
	quoteCount := strings.Count(name, `"`)
	if quoteCount == 0 {
		return `"` + name + `"`
	}

	var buffer strings.Builder
	buffer.Grow(len(name) + quoteCount + 2)
	buffer.WriteByte('"')
	writeQuotedIdentifier(name, &buffer)
	buffer.WriteByte('"')
	return buffer.String()
}

// BufferQuoteIdentifier satisfies the same purpose as QuoteIdentifier, but backed by a
// byte buffer.
func BufferQuoteIdentifier(name string, buffer *bytes.Buffer) {
	// TODO(v2): this should have accepted an io.Writer, not *bytes.Buffer.
	end := strings.IndexByte(name, 0)
	if end > -1 {
		name = name[:end]
	}
	buffer.WriteByte('"')
	for {
		quote := strings.IndexByte(name, '"')
		if quote < 0 {
			buffer.WriteString(name)
			break
		}
		buffer.WriteString(name[:quote+1])
		buffer.WriteByte('"')
		name = name[quote+1:]
	}
	buffer.WriteByte('"')
}

func writeQuotedIdentifier(name string, buffer *strings.Builder) {
	for {
		quote := strings.IndexByte(name, '"')
		if quote < 0 {
			buffer.WriteString(name)
			return
		}
		buffer.WriteString(name[:quote+1])
		buffer.WriteByte('"')
		name = name[quote+1:]
	}
}

// QuoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement. For example:
//
//	exp_date := pq.QuoteLiteral("2023-01-05 15:00:00Z")
//	err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
func QuoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	quoteCount := strings.Count(literal, `'`)
	backslashCount := strings.Count(literal, `\`)
	if quoteCount == 0 && backslashCount == 0 {
		return `'` + literal + `'`
	}

	var buffer strings.Builder
	if backslashCount == 0 {
		buffer.Grow(len(literal) + quoteCount + 2)
		buffer.WriteByte('\'')
	} else {
		buffer.Grow(len(literal) + quoteCount + backslashCount + 4)
		buffer.WriteString(` E'`)
	}
	for i := range len(literal) {
		switch literal[i] {
		case '\'', '\\':
			buffer.WriteByte(literal[i])
			buffer.WriteByte(literal[i])
		default:
			buffer.WriteByte(literal[i])
		}
	}
	buffer.WriteByte('\'')
	return buffer.String()
}
