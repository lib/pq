package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
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
	l, u, err := parseIntRange(val.([]byte), 64)
	if err != nil {
		return err
	}
	r.Lower = l
	r.Upper = u
	return nil
}

// Value implements the driver.Valuer interface
func (r Int64Range) Value() (driver.Value, error) {
	if r.Lower > r.Upper {
		return nil, errors.New("lower value is greater than the upper value")
	}
	return []byte(r.String()), nil
}

// String returns a string representation of this range
func (r Int64Range) String() string {
	return fmt.Sprintf("[%d,%d)", r.Lower, r.Upper)
}
