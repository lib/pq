package pq

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

// Array implements the driver.Valuer interface for an array or slice.
//
// For example:
//  db.Query("SELECT * FROM t WHERE id = ANY($1)", pq.Array{[]int{235, 401}})
type Array struct{ A interface{} }

// ArrayValuer is an interface used by Array.
type ArrayValuer interface {

	// ArrayDelimiter returns the delimiter character(s) for this element's type.
	ArrayDelimiter() string

	// ArrayValue returns the value of this array element. []byte and string
	// values are double-quoted by Array.
	ArrayValue() (interface{}, error)
}

// Value implements the driver.Valuer interface.
func (a Array) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(a.A)

	if k := rv.Kind(); k != reflect.Array && k != reflect.Slice {
		return nil, fmt.Errorf("pq: Unable to convert %T to array", a.A)
	}

	if n := rv.Len(); n > 0 {
		// TODO b = make([]byte, 0, /* best guess */)

		b, _, err := appendArray(nil, rv, n)
		return b, err
	}

	return []byte{'{', '}'}, nil
}

// appendArray appends rv to the buffer, returning the extended buffer and
// the delimiter used between elements.
//
// It panics when n <= 0 or rv's Kind is not reflect.Array nor reflect.Slice.
func appendArray(b []byte, rv reflect.Value, n int) ([]byte, string, error) {
	b = append(b, '{')

	var del string
	var err error
	if b, del, err = appendArrayElement(b, rv.Index(0)); err != nil {
		return b, del, err
	}

	for i := 1; i < n; i++ {
		b = append(b, del...)
		if b, del, err = appendArrayElement(b, rv.Index(i)); err != nil {
			return b, del, err
		}
	}

	return append(b, '}'), del, nil
}

// appendArrayElement appends rv to the buffer, returning the extended buffer
// and the delimiter to use before the next element.
//
// When rv's Kind is neither reflect.Array nor reflect.Slice, it is converted
// using driver.DefaultParameterConverter and the resulting []byte or string
// is double-quoted.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func appendArrayElement(b []byte, rv reflect.Value) ([]byte, string, error) {
	if k := rv.Kind(); k == reflect.Array ||
		(k == reflect.Slice && rv.Type() != reflect.TypeOf([]byte{})) {
		if n := rv.Len(); n > 0 {
			return appendArray(b, rv, n)
		}

		return b, "", nil
	}

	var del string = ","
	var err error
	var iv interface{} = rv.Interface()

	if av, ok := iv.(ArrayValuer); ok {
		del = av.ArrayDelimiter()
		if iv, err = av.ArrayValue(); err != nil {
			return b, del, err
		}
	}

	if iv, err = driver.DefaultParameterConverter.ConvertValue(iv); err != nil {
		return b, del, err
	}

	switch v := iv.(type) {
	case nil:
		return append(b, "NULL"...), del, nil

	case []byte:
		b = append(b, '"')
		for {
			i := bytes.IndexAny(v, `"\`)
			if i < 0 {
				b = append(b, v...)
				break
			}
			b = append(b, v[:i]...)
			b = append(b, '\\', v[i])
			v = v[i+1:]
		}
		return append(b, '"'), del, nil

	case string:
		b = append(b, '"')
		for {
			i := strings.IndexAny(v, `"\`)
			if i < 0 {
				b = append(b, v...)
				break
			}
			b = append(b, v[:i]...)
			b = append(b, '\\', v[i])
			v = v[i+1:]
		}
		return append(b, '"'), del, nil
	}

	b, err = appendValue(b, iv)
	return b, del, err
}

func appendValue(b []byte, v driver.Value) ([]byte, error) {
	return append(b, encode(nil, v, 0)...), nil
}
