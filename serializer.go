package pq

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"time"
)

// Dump formats a SQL-dump compatible representation of the given rows.
func Dump(driverRows driver.Rows) ([]byte, error) {
	var err error
	var builder bytes.Buffer
	if rs, ok := driverRows.(*rows); ok {
		defer func() {
			err := rs.Close()
			if err != nil {
				panic(err)
			}
		}()
		values := make([]driver.Value, len(rs.colNames))
		first := true
		for err = rs.Next(values); err == nil; err = rs.Next(values) {
			if !first {
				builder.Write([]byte(",\n"))
			} else {
				first = false
			}
			builder.Write([]byte("("))
			last := len(values) - 1
			for i, v := range values {
				if v != nil {
					var needsQuote bool
					switch v.(type) {
					case string:
						needsQuote = true
					case []byte:
						needsQuote = true
					case time.Time:
						needsQuote = true
					}
					if needsQuote {
						builder.Write([]byte("'"))
					}
					ve := encode(&rs.cn.parameterStatus, v, rs.colTyps[i].OID)
					_, err = builder.Write(escapeSingleQuotes(ve))
					if err != nil {
						return nil, err
					}
					if needsQuote {
						builder.Write([]byte("'"))
					}
				} else {
					builder.Write([]byte("NULL"))
				}
				if i != last {
					builder.Write([]byte(", "))
				}
			}
			builder.Write([]byte(")"))
		}
		if err != io.EOF {
			return nil, err
		}
		return builder.Bytes(), nil
	}
	return nil, errors.New("not a postgres rows struct")
}

const singleQuote = '\''

func escapeSingleQuotes(s []byte) []byte {
	out := make([]byte, 0, 2*len(s))
	for i := range s {
		switch c := s[i]; c {
		case singleQuote:
			out = append(out, singleQuote, singleQuote)
		default:
			out = append(out, c)
		}
	}
	return out
}
