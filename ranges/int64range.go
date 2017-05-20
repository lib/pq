package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
)

// Int64Range represents a range between two int64 values. The minimum value is
// inclusive and the maximum is exclusive.
type Int64Range struct {
	Min int64
	Max int64
}

// Scan implements the sql.Scanner interface
func (r *Int64Range) Scan(val interface{}) error {
	if val == nil {
		return errors.New("cannot scan NULL into *Int64Range")
	}
	var (
		err      error
		min, max []byte
	)
	min, max, err = readDiscreteRange(val.([]byte))
	if err != nil {
		return err
	}
	r.Min, err = strconv.ParseInt(string(min), 10, 64)
	if err != nil {
		return err
	}
	r.Max, err = strconv.ParseInt(string(max), 10, 64)
	if err != nil {
		return err
	}
	return nil
}

// Value implements the driver.Valuer interface
func (r Int64Range) Value() (driver.Value, error) {
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Int64Range) String() string {
	return fmt.Sprintf("[%d,%d)", r.Min, r.Max)
}
