package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
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
	l, u, err := parseIntRange(val.([]byte), 32)
	if err != nil {
		return err
	}
	r.Lower = int32(l)
	r.Upper = int32(u)
	return nil
}

// Value implements the driver.Valuer interface
func (r Int32Range) Value() (driver.Value, error) {
	if r.Lower > r.Upper {
		return nil, errors.New("lower value is greater than the upper value")
	}
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Int32Range) String() string {
	return fmt.Sprintf("[%d,%d)", r.Lower, r.Upper)
}
