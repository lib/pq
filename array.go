package pq

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var typeByteSlice = reflect.TypeOf([]byte{})

// Array returns the optimal driver.Valuer for an array or slice of any
// dimension.
//
// For example:
//  db.Query(`SELECT * FROM t WHERE id = ANY($1)`, pq.Array([]int{235, 401}))
func Array(a interface{}) driver.Valuer {
	switch a := a.(type) {
	case []bool:
		return BoolArray(a)
	case []float64:
		return Float64Array(a)
	case []int64:
		return Int64Array(a)
	case []string:
		return StringArray(a)
	}

	return GenericArray{a}
}

// ArrayValuer is used by GenericArray.
type ArrayValuer interface {

	// ArrayDelimiter returns the delimiter character(s) for this element's type.
	ArrayDelimiter() string
}

type BoolArray []bool

// Value implements the driver.Valuer interface.
func (a BoolArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be exactly two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1+2*n)

		for i := 0; i < n; i++ {
			b[2*i] = ','
			if a[i] {
				b[1+2*i] = 't'
			} else {
				b[1+2*i] = 'f'
			}
		}

		b[0] = '{'
		b[2*n] = '}'

		return b, nil
	}

	return []byte{'{', '}'}, nil
}

type ByteaArray [][]byte

// Value implements the driver.Valuer interface. It uses the "hex" format which
// is only supported on PostgreSQL 9.0 or newer.
func (a ByteaArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// 3*N bytes of hex formatting, and N-1 bytes of delimiters.
		size := 1 + 6*n
		for _, x := range a {
			size += hex.EncodedLen(len(x))
		}

		b := make([]byte, size)

		for i, s := 0, b; i < n; i++ {
			o := copy(s, `,"\\x`)
			o += hex.Encode(s[o:], a[i])
			s[o] = '"'
			s = s[o+1:]
		}

		b[0] = '{'
		b[size-1] = '}'

		return b, nil
	}

	return []byte{'{', '}'}, nil
}

type Float64Array []float64

// Value implements the driver.Valuer interface.
func (a Float64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendFloat(b, a[0], 'f', -1, 64)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendFloat(b, a[i], 'f', -1, 64)
		}

		return append(b, '}'), nil
	}

	return []byte{'{', '}'}, nil
}

// GenericArray implements the driver.Valuer interface for an array or slice
// of any dimension.
type GenericArray struct{ A interface{} }

// Value implements the driver.Valuer interface.
func (a GenericArray) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(a.A)

	if k := rv.Kind(); k != reflect.Array && k != reflect.Slice {
		return nil, fmt.Errorf("pq: Unable to convert %T to array", a.A)
	}

	if n := rv.Len(); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 0, 1+2*n)

		b, _, err := appendArray(b, rv, n)
		return b, err
	}

	return []byte{'{', '}'}, nil
}

type Int64Array []int64

// Value implements the driver.Valuer interface.
func (a Int64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendInt(b, a[0], 10)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendInt(b, a[i], 10)
		}

		return append(b, '}'), nil
	}

	return []byte{'{', '}'}, nil
}

type StringArray []string

// Value implements the driver.Valuer interface.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+3*n)
		b[0] = '{'

		b = appendArrayQuotedString(b, a[0])
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = appendArrayQuotedString(b, a[i])
		}

		return append(b, '}'), nil
	}

	return []byte{'{', '}'}, nil
}

// appendArray appends rv to the buffer, returning the extended buffer and
// the delimiter used between elements.
//
// It panics when n <= 0 or rv's Kind is not reflect.Array nor reflect.Slice.
func appendArray(b []byte, rv reflect.Value, n int) ([]byte, string, error) {
	var del string
	var err error

	b = append(b, '{')

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
	if k := rv.Kind(); k == reflect.Array || (k == reflect.Slice && rv.Type() != typeByteSlice) {
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
	}

	if iv, err = driver.DefaultParameterConverter.ConvertValue(iv); err != nil {
		return b, del, err
	}

	switch v := iv.(type) {
	case nil:
		return append(b, "NULL"...), del, nil
	case []byte:
		return appendArrayQuotedBytes(b, v), del, nil
	case string:
		return appendArrayQuotedString(b, v), del, nil
	}

	b, err = appendValue(b, iv)
	return b, del, err
}

func appendArrayQuotedBytes(b, v []byte) []byte {
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

func appendArrayQuotedString(b []byte, v string) []byte {
	b = append(b, '"')
	for {
		i := strings.IndexAny(v, `"\`)
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

func appendValue(b []byte, v driver.Value) ([]byte, error) {
	return append(b, encode(nil, v, 0)...), nil
}
