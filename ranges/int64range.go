package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
)

// Int64Range represents a range between two int64 values. The lower value is
// inclusive and the upper is exclusive.
type Int64Range struct {
	Lower int64
	Upper int64
}

// Scan implements the sql.Scanner interface
func (r *Int64Range) Scan(val interface{}) error {
	if val == nil {
		return errors.New("cannot scan NULL into *Int64Range")
	}
	var (
		err          error
		lower, upper []byte
	)
	lower, upper, err = readDiscreteRange(val.([]byte))
	if err != nil {
		return err
	}
	r.Lower, err = strconv.ParseInt(string(lower), 10, 64)
	if err != nil {
		return err
	}
	r.Upper, err = strconv.ParseInt(string(upper), 10, 64)
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
	return fmt.Sprintf("[%d,%d)", r.Lower, r.Upper)
}
