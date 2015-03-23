package pq

import (
	"database/sql/driver"
	"reflect"
)

// Array wraps slices with a driver.Valuer interface.
// For example:
//  ids := []int{235, 401}
//  db.Query("SELECT * FROM t WHERE id=ANY($1)", Array{ids})
type Array struct {
	A interface{} // A slice or array
}

// Value implements the driver Valuer interface.
func (a Array) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}
	v := reflect.ValueOf(a.A)

	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, ErrInvalidArray
	}

	return arrayValue(v)
}

func arrayValue(v reflect.Value) ([]byte, error) {
	_, isBytes := v.Interface().([]byte)

	if !isBytes && (v.Kind() == reflect.Slice || v.Kind() == reflect.Array) {
		result := []byte{'{'}

		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				result = append(result, ',')
			}

			item, err := arrayValue(v.Index(i))
			if err != nil {
				return nil, err
			}

			result = append(result, item...)
		}

		return append(result, '}'), nil
	}

	val := v.Interface()

	// encode can handle float32 in addition to drive.Value types
	// so we don't want to convert it to a float64
	if _, ok := val.(float32); !ok {
		var err error
		val, err = driver.DefaultParameterConverter.ConvertValue(val)
		if err != nil {
			return nil, err
		}
	}

	if val == nil {
		return []byte("NULL"), nil
	}

	encoded := encode(nil, val, 0)

	switch val.(type) {
	case []byte, string:
		return escapedArrayText(encoded), nil
	default:
		return encoded, nil
	}
}

func escapedArrayText(text []byte) []byte {
	result := []byte{'"'}
	for _, c := range text {
		switch c {
		case '"', '\\':
			result = append(result, '\\', c)
		default:
			result = append(result, c)
		}
	}

	return append(result, '"')
}
