package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
)

// Int32Range represents a range between two int32 values. The minimum value is
// inclusive and the maximum is exclusive.
type Int32Range struct {
	Min int32
	Max int32
}

// Scan implements the sql.Scanner interface
func (r *Int32Range) Scan(val interface{}) error {
	if val == nil {
		return errors.New("cannot scan NULL into *Int32Range")
	}
	minb, maxb, err := readDiscreteRange(val.([]byte))
	if err != nil {
		return err
	}
	min, err := strconv.Atoi(string(minb))
	if err != nil {
		return err
	}
	max, err := strconv.Atoi(string(maxb))
	if err != nil {
		return err
	}
	r.Min = int32(min)
	r.Max = int32(max)
	return nil
}

// Value implements the driver.Valuer interface
func (r Int32Range) Value() (driver.Value, error) {
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Int32Range) String() string {
	return fmt.Sprintf("[%d,%d)", r.Min, r.Max)
}
