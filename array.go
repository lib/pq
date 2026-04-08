package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	_ "unsafe"
)

var (
	_ sql.Scanner   = (*ArrayOf[any])(nil)
	_ driver.Valuer = (*ArrayOf[any])(nil)
)

// TODO: hopefully will be exported in the future?
// https://github.com/golang/go/issues/62146#issuecomment-3921700836
//
//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

// ArrayOf wraps a slice with Scan() and Value() methods that use PostgreSQL's
// array syntax.
//
// Types may optionally implement the "ArrayDelimiter() string" method to use a
// different array delimiter.
type ArrayOf[T any] []T

func (a *ArrayOf[T]) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		return a.scan(src)
	case string:
		return a.scan([]byte(src))
	case nil:
		*a = nil
		return nil
	}
	return fmt.Errorf("pq: cannot convert %T to %T", src, a)
}

func (a *ArrayOf[T]) scan(src []byte) error {
	var zero T
	dims, elems, err := parseArray(src, arrayDelimiter(any(zero)))
	if err != nil {
		return err
	}
	if len(dims) > 1 {
		return fmt.Errorf("pq: cannot convert ARRAY%s to %T", strings.Replace(fmt.Sprint(dims), " ", "][", -1), a)
	}

	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
		return nil
	}

	switch any(zero).(type) {
	case time.Time, *time.Time: // convertAssign only scans time.Time if src (v here) is a time.Time.
		_, ptr := any(zero).(*time.Time)
		b := make([]T, len(elems))
		for i, v := range elems {
			if v == nil { // NULL
				if ptr {
					continue // Use nil zero value.
				}
				return fmt.Errorf("pq: array index %d: cannot convert NULL to time.Time", i)
			}
			t, err := ParseTimestamp(nil, string(v))
			if err != nil {
				return fmt.Errorf("array index %d: %w", i, err)
			}
			x := any(t)
			if ptr {
				x = any(&t)
			}
			b[i] = x.(T)
		}
		*a = b
	case []byte:
		b := make([]T, len(elems))
		for i, v := range elems {
			x, err := parseBytea(v)
			if err != nil {
				return fmt.Errorf("pq: array index %d: %w", i, err)
			}
			b[i] = any(x).(T)
		}
		*a = b
	case nil: // Treat any as string, rather than []byte
		b := make([]T, len(elems))
		for i, v := range elems {
			b[i] = any(string(v)).(T)
		}
		*a = b
	default:
		b := make([]T, len(elems))
		for i, v := range elems {
			if v == nil { // NULL
				if !isPointer(zero) {
					return fmt.Errorf("pq: array index %d: cannot convert NULL to %T", i, zero)
				}
				continue // Just use zero value
			}

			err := convertAssign(&b[i], v)
			if err != nil {
				return fmt.Errorf("pq: array index %d: %s", i, strings.TrimPrefix(err.Error(), "sql/driver: "))
			}
		}
		*a = b
	}

	return nil
}

func isPointer(t any) bool {
	switch t.(type) {
	case *string, *bool, *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64:
		return true
	}
	return reflect.ValueOf(t).Kind() == reflect.Ptr
}

func arrayDelimiter(v any) []byte {
	if d, ok := v.(interface{ ArrayDelimiter() string }); ok {
		return []byte(d.ArrayDelimiter())
	}
	return []byte{','}
}

const hextable = "0123456789abcdef"

func (a ArrayOf[T]) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	if len(a) == 0 {
		return "{}", nil
	}

	var zero T
	del := arrayDelimiter(any(zero))

	// Pick a reasonable initial buffer length.
	sz := 2 + (len(a)-1)*len(del) // Start/end {} and n-1 delimiters.
	switch any(zero).(type) {
	case bool:
		sz += len(a) // Always 1 byte.
	case string:
		sz += len(a) * 4 // Start/end quote, assume 2 bytes per entry.
	case []byte:
		sz += len(a) * 3 // Start \\x
		for _, aa := range a {
			sz += len(any(aa).([]byte)) * 2
		}
	case float32, float64:
		sz += len(a) * 3 // Assume 3 bytes per entry.
	case time.Time:
		sz += len(a) * 22 // 2 quotes and assumed 20 bytes per entry (timestamp w/o subseconds but with "Z")
	default:
		sz += len(a) * 2 // Assume 2 bytes per entry.
	}
	b := make([]byte, 0, sz)

	b = append(b, '{')
	for i, aa := range a {
		if i > 0 {
			b = append(b, del...)
		}

		swval := any(aa)
		if v, ok := swval.(driver.Valuer); ok {
			var err error
			swval, err = v.Value()
			if err != nil {
				return nil, fmt.Errorf("pq: %w", err)
			}
		}

	restart:
		switch v := swval.(type) {
		default:
			rv := reflect.ValueOf(aa)
			switch rv.Kind() {
			case reflect.String:
				b = appendArrayQuotedText(b, []byte(rv.String()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				b = strconv.AppendInt(b, rv.Int(), 10)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				b = strconv.AppendUint(b, rv.Uint(), 10)
			case reflect.Float32, reflect.Float64:
				b = strconv.AppendFloat(b, rv.Float(), 'f', -1, 32)
			case reflect.Ptr:
				if rv.IsNil() {
					b = append(b, "NULL"...)
					continue
				}
				swval = rv.Elem().Interface()
				goto restart
			default:
				return nil, fmt.Errorf("pq: unsupported array type %T", zero)
			}
		case []byte:
			b = append(b, `"\\x`...)
			for _, c := range v {
				b = append(b, hextable[c>>4], hextable[c&0x0f])
			}
			b = append(b, `"`...)
		case string:
			b = appendArrayQuotedText(b, []byte(v))
		case int:
			b = strconv.AppendInt(b, int64(v), 10)
		case int8:
			b = strconv.AppendInt(b, int64(v), 10)
		case int16:
			b = strconv.AppendInt(b, int64(v), 10)
		case int32:
			b = strconv.AppendInt(b, int64(v), 10)
		case int64:
			b = strconv.AppendInt(b, v, 10)
		case uint:
			b = strconv.AppendUint(b, uint64(v), 10)
		case uint8:
			b = strconv.AppendUint(b, uint64(v), 10)
		case uint16:
			b = strconv.AppendUint(b, uint64(v), 10)
		case uint32:
			b = strconv.AppendUint(b, uint64(v), 10)
		case uint64:
			b = strconv.AppendUint(b, v, 10)
		case float32:
			b = strconv.AppendFloat(b, float64(v), 'f', -1, 32)
		case float64:
			b = strconv.AppendFloat(b, v, 'f', -1, 64)
		case bool:
			if any(aa).(bool) {
				b = append(b, 't')
			} else {
				b = append(b, 'f')
			}
		case time.Time:
			b = append(b, '"')
			b = append(b, FormatTimestamp(v)...)
			b = append(b, '"')
		}
	}
	b = append(b, '}')

	return string(b), nil
}

func appendArrayQuotedText(b, v []byte) []byte {
	b = append(b, '"')
	for {
		i := bytes.IndexAny(v, `"\`)
		if i < 0 {
			b = append(b, v...)
			break
		}
		if i > 0 {
			b = append(b, v[:i]...)
		}
		b = append(b, '\\', v[i])
		v = v[i+1:]
	}
	return append(b, '"')
}

// parseArray extracts the dimensions and elements of an array represented in
// text format. Only representations emitted by the backend are supported.
// Notably, whitespace around brackets and delimiters is significant, and NULL
// is case-sensitive.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func parseArray(src, del []byte) (dims []int, elems [][]byte, err error) {
	if len(src) < 1 || src[0] != '{' {
		return nil, nil, fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '{', 0)
	}

	var depth, i int
Open:
	for i < len(src) {
		switch src[i] {
		case '{':
			depth++
			i++
		case '}':
			elems = make([][]byte, 0)
			goto Close
		default:
			break Open
		}
	}
	dims = make([]int, i)

Element:
	for i < len(src) {
		switch src[i] {
		case '{':
			if depth == len(dims) {
				break Element
			}
			depth++
			dims[depth-1] = 0
			i++
		case '"':
			var elem = []byte{}
			var escape bool
			for i++; i < len(src); i++ {
				if escape {
					elem = append(elem, src[i])
					escape = false
				} else {
					switch src[i] {
					default:
						elem = append(elem, src[i])
					case '\\':
						escape = true
					case '"':
						elems = append(elems, elem)
						i++
						break Element
					}
				}
			}
		default:
			for start := i; i < len(src); i++ {
				if bytes.HasPrefix(src[i:], del) || src[i] == '}' {
					elem := src[start:i]
					if len(elem) == 0 {
						return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
					}
					if bytes.Equal(elem, []byte("NULL")) {
						elem = nil
					}
					elems = append(elems, elem)
					break Element
				}
			}
		}
	}

	for i < len(src) {
		if bytes.HasPrefix(src[i:], del) && depth > 0 {
			dims[depth-1]++
			i += len(del)
			goto Element
		} else if src[i] == '}' && depth > 0 {
			dims[depth-1]++
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}

Close:
	for i < len(src) {
		if src[i] == '}' && depth > 0 {
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}
	if depth > 0 {
		err = fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '}', i)
	}
	if err == nil {
		for _, d := range dims {
			if (len(elems) % d) != 0 {
				err = fmt.Errorf("pq: multidimensional arrays must have elements with matching dimensions")
			}
		}
	}
	return
}
