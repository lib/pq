package ranges

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

// Float64Range represents a range between two float64 values
type Float64Range struct {
	Min          float64
	MinInclusive bool
	Max          float64
	MaxInclusive bool
}

// Scan implements the sql.Scanner interface
func (r *Float64Range) Scan(val interface{}) error {
	if val == nil {
		r.Min = 0
		r.MinInclusive = false
		r.Max = 0
		r.MaxInclusive = false
		return nil
	}
	minIn, maxIn, min, max, err := readRange(val.([]byte))
	if err != nil {
		return err
	}
	r.Min, err = strconv.ParseFloat(string(min), 64)
	if err != nil {
		return err
	}
	r.Max, err = strconv.ParseFloat(string(max), 64)
	if err != nil {
		return err
	}
	r.MinInclusive = minIn
	r.MaxInclusive = maxIn
	return nil
}

// Value implements the driver.Valuer interface
func (r Float64Range) Value() (driver.Value, error) {
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Float64Range) String() string {
	var (
		open  = "("
		close = ")"
	)
	if r.MinInclusive {
		open = "["
	}
	if r.MaxInclusive {
		close = "]"
	}
	return fmt.Sprintf("%s%f,%f%s", open, r.Min, r.Max, close)
}
