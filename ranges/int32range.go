package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
)

// Int32Range represents a range between two int32 values. The lower value is
// inclusive and the upper is exclusive.
type Int32Range struct {
	Lower int32
	Upper int32
}

// Scan implements the sql.Scanner interface
func (r *Int32Range) Scan(val interface{}) error {
	if val == nil {
		return errors.New("cannot scan NULL into *Int32Range")
	}
	lowerb, upperb, err := readDiscreteRange(val.([]byte))
	if err != nil {
		return err
	}
	lower, err := strconv.Atoi(string(lowerb))
	if err != nil {
		return err
	}
	upper, err := strconv.Atoi(string(upperb))
	if err != nil {
		return err
	}
	r.Lower = int32(lower)
	r.Upper = int32(upper)
	return nil
}

// Value implements the driver.Valuer interface
func (r Int32Range) Value() (driver.Value, error) {
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Int32Range) String() string {
	return fmt.Sprintf("[%d,%d)", r.Lower, r.Upper)
}
