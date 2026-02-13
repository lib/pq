//go:build go1.18
// +build go1.18

package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
	_ "unsafe"
)

// Range returns the optimal driver.Valuer and sql.Scanner for a range.
// Check https://www.postgresql.org/docs/current/rangetypes.html for details.
//
// For example:
//
//	min := time.Now()
//	max := time.Now().AddDate(0,-1,0)
//	db.Query(`SELECT * FROM reservation WHERE during && $1`, pq.NewRange(&min, &max))
//
//	var x pq.Range[int]
//	db.QueryRow(`SELECT '[1, 10)'`).Scan(&x)
//
// Scanning multi-dimensional range is supported using [MultiRange]
type Range[T any] struct {
	// When nil it will be infinite value
	Lower, Upper *T
	LowerBound   RangeLowerBound
	UpperBound   RangeUpperBound
}

type RangeLowerBound byte
type RangeUpperBound byte

const (
	RangeLowerBoundInclusive RangeLowerBound = '['
	RangeLowerBoundExclusive RangeLowerBound = '('
	RangeUpperBoundInclusive RangeUpperBound = ']'
	RangeUpperBoundExclusive RangeUpperBound = ')'

	RangeLowerBoundDefault = RangeLowerBoundInclusive
	RangeUpperBoundDefault = RangeUpperBoundExclusive

	RangeEmpty = "empty"
)

// NewRange create a [Range] with default bounds [RangeLowerBoundDefault] and [RangeUpperBoundDefault].
func NewRange[T any](lower, upper *T) Range[T] {
	return Range[T]{
		Lower:      lower,
		Upper:      upper,
		LowerBound: RangeLowerBoundDefault,
		UpperBound: RangeUpperBoundDefault,
	}
}

var (
	_ sql.Scanner   = (*Range[any])(nil)
	_ driver.Valuer = (*Range[any])(nil)
)

func (r *Range[T]) Scan(anySrc any) error {
	var src []byte
	switch s := anySrc.(type) {
	case string:
		src = []byte(s)
	case []byte:
		src = s
	default:
		return fmt.Errorf("pq: cannot convert %T to Range", anySrc)
	}

	src = bytes.TrimSpace(src)
	if len(src) == 0 {
		return fmt.Errorf("pq: could not parse range: range is empty")
	}

	if string(src) == RangeEmpty {
		return nil
	}

	// read bounds
	r.LowerBound = RangeLowerBound(src[0])
	r.UpperBound = RangeUpperBound(src[len(src)-1])
	src = src[1 : len(src)-1]
	if len(src) == 0 {
		return fmt.Errorf("pq: could not parse range: range is empty")
	}

	// read range
	l, u, ok := bytes.Cut(src, []byte(","))
	if !ok {
		return fmt.Errorf("pq: could not parse range: missing comma")
	}

	convertBound := func(dest any, src []byte) error {
		src = bytes.Trim(src, "\"")
		switch d := dest.(type) {
		case sql.Scanner:
			if err := d.Scan(src); err != nil {
				return err
			}
			return nil
		case *time.Time:
			var err error
			*d, err = ParseTimestamp(nil, string(src))
			if err != nil {
				return err
			}
			return nil
		}
		if err := convertAssign(dest, string(src)); err != nil {
			return err
		}
		return nil
	}

	if len(l) != 0 {
		r.Lower = new(T)
		if err := convertBound(r.Lower, l); err != nil {
			return err
		}
	}
	if len(u) != 0 {
		r.Upper = new(T)
		if err := convertBound(r.Upper, u); err != nil {
			return err
		}
	}

	return nil
}

// IsEmpty return true when bounds are inclusive and range value are equal
func (r Range[T]) IsEmpty() bool {
	if r.LowerBound == 0 && r.UpperBound == 0 {
		return true
	}
	if r.Lower == nil || r.Upper == nil {
		return false
	}
	if r.LowerBound == RangeLowerBoundInclusive && r.UpperBound == RangeUpperBoundInclusive {
		return false
	}
	return reflect.DeepEqual(*r.Lower, *r.Upper)
}

// IsZero return true when empty, used for IsZeroer interface
func (r Range[T]) IsZero() bool {
	return r.IsEmpty()
}

func (r Range[T]) Value() (driver.Value, error) {
	if r.IsEmpty() {
		return RangeEmpty, nil
	}

	convertBound := func(src any) (string, error) {
		if reflect.ValueOf(src).IsNil() {
			return "", nil
		}

		switch s := src.(type) {
		case *time.Time:
			return "\"" + string(FormatTimestamp(*s)) + "\"", nil
		case driver.Valuer:
			v, err := s.Value()
			if err != nil {
				return "", err
			}
			var out string
			if err := convertAssign(&out, v); err != nil {
				return "", err
			}
			return "\"" + out + "\"", nil
		default:
			var out string
			if err := convertAssign(&out, reflect.ValueOf(src).Elem().Interface()); err != nil {
				return "", err
			}
			return "\"" + out + "\"", nil
		}
	}

	b, err := convertBound(r.Lower)
	if err != nil {
		return nil, err
	}

	var v string
	v += string(r.LowerBound) + b + ","

	b, err = convertBound(r.Upper)
	if err != nil {
		return nil, err
	}

	v += b + string(r.UpperBound)

	return v, nil
}

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

// MultiRange returns the optimal driver.Valuer and sql.Scanner for a multirange.
// Check https://www.postgresql.org/docs/current/rangetypes.html for details.
// Scanning one-dimensional range is supported using [MultiRange]
type MultiRange[T any] []Range[T]

var (
	_ sql.Scanner   = (*MultiRange[any])(nil)
	_ driver.Valuer = (*MultiRange[any])(nil)
)

func (m *MultiRange[T]) Scan(anySrc any) error {
	var src []byte
	switch s := anySrc.(type) {
	case string:
		src = []byte(s)
	case []byte:
		src = s
	default:
		return fmt.Errorf("pq: cannot convert %T to MultiRange", anySrc)
	}

	src = bytes.TrimSpace(src)
	if len(src) == 0 {
		return fmt.Errorf("pq: could not parse multirange: multirange is empty")
	}

	if src[0] != '{' || src[len(src)-1] != '}' {
		return fmt.Errorf("pq: invalid multirange format: missing braces")
	}
	src = src[1 : len(src)-1]

	blockPos := 0
	boundDepth := 0
	inQuote := false
	isEscaping := false
	for i, c := range src {
		if isEscaping {
			isEscaping = false
			continue
		}

		switch c {
		case '\\':
			isEscaping = true
		case '"':
			inQuote = !inQuote
		case byte(RangeLowerBoundInclusive), byte(RangeLowerBoundExclusive):
			if !inQuote {
				boundDepth++
			}
		case byte(RangeUpperBoundInclusive), byte(RangeUpperBoundExclusive):
			if !inQuote {
				boundDepth--
			}
		case ',':
			if !inQuote && boundDepth == 0 {
				var r Range[T]
				if err := r.Scan(bytes.TrimSpace(src[blockPos:i])); err != nil {
					return err
				}
				*m = append(*m, r)
				blockPos = i + 1
			}
		}
	}

	// parse last range if any
	if blockPos < len(src) {
		rBytes := bytes.TrimSpace(src[blockPos:])
		var r Range[T]
		if err := r.Scan(rBytes); err != nil {
			return err
		}
		*m = append(*m, r)
	}

	return nil
}

func (m MultiRange[T]) Value() (driver.Value, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	var b []byte
	var err error
	b = append(b, '{')

	for _, r := range m {
		iv, err := driver.DefaultParameterConverter.ConvertValue(r)
		if err != nil {
			return nil, err
		}

		b, err = appendValue(b, iv)
		if err != nil {
			return nil, err
		}
		b = append(b, ',')
	}
	b = append(b[:len(b)-1], '}')
	return b, err
}
