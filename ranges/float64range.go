package ranges

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

// Float64Range represents a range between two float64 values
type Float64Range struct {
	Lower          float64
	LowerInclusive bool
	Upper          float64
	UpperInclusive bool
}

// Scan implements the sql.Scanner interface
func (r *Float64Range) Scan(val interface{}) error {
	if val == nil {
		r.Lower = 0
		r.LowerInclusive = false
		r.Upper = 0
		r.UpperInclusive = false
		return nil
	}
	lowerIn, upperIn, lower, upper, err := readRange(val.([]byte))
	if err != nil {
		return err
	}
	r.Lower, err = strconv.ParseFloat(string(lower), 64)
	if err != nil {
		return err
	}
	r.Upper, err = strconv.ParseFloat(string(upper), 64)
	if err != nil {
		return err
	}
	r.LowerInclusive = lowerIn
	r.UpperInclusive = upperIn
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
	if r.LowerInclusive {
		open = "["
	}
	if r.UpperInclusive {
		close = "]"
	}
	return fmt.Sprintf("%s%f,%f%s", open, r.Lower, r.Upper, close)
}
