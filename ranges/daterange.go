package ranges

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

func isTimeZero(t time.Time) bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

// DateRange represents a range between two dates where the lower is inclusive
// and the upper exclusive.
type DateRange struct {
	Lower time.Time
	Upper time.Time
}

// Scan implements the sql.Scanner interface
func (r *DateRange) Scan(val interface{}) error {
	var (
		err        error
		minb, maxb []byte
	)

	if val == nil {
		return errors.New("cannot scan NULL into *DateRange")
	}
	minb, maxb, err = readDiscreteTimeRange(val.([]byte))
	if err != nil {
		return errors.New("could not scan date range: " + err.Error())
	}

	if len(minb) == 0 {
		r.Lower = time.Time{}
	} else {
		r.Lower, err = pq.ParseTimestamp(nil, string(minb))
		if err != nil {
			return errors.New("could not parse lower date:" + err.Error())
		}
		if !isTimeZero(r.Lower) {
			return errors.New("time component of lower date is not zero")
		}
	}

	if len(maxb) == 0 {
		r.Upper = time.Time{}
	} else {
		r.Upper, err = pq.ParseTimestamp(nil, string(maxb))
		if err != nil {
			return errors.New("could not parse upper date:" + err.Error())
		}
		if !isTimeZero(r.Upper) {
			return errors.New("time component of upper date is not zero")
		}
	}

	return nil
}

// IsLowerInfinity returns whether the lower value is negative infinity
func (r DateRange) IsLowerInfinity() bool {
	return r.Lower.IsZero()
}

// IsUpperInfinity returns whether the upper value is positive infinity
func (r DateRange) IsUpperInfinity() bool {
	return r.Upper.IsZero()
}

// Value implements the driver.Value interface
func (r DateRange) Value() (driver.Value, error) {
	if !isTimeZero(r.Lower) {
		return nil, errors.New("time component of lower date is not zero")
	}
	if !isTimeZero(r.Upper) {
		return nil, errors.New("time component of upper date is not zero")
	}
	if r.Lower.After(r.Upper) {
		return nil, errors.New("lower date is after upper date")
	}
	return []byte(r.String()), nil
}

// Returns the date range as a string where the dates are formatted according
// to ISO8601
func (r DateRange) String() string {
	var (
		open         = '('
		lower, upper string
	)
	if !r.Lower.IsZero() {
		lower = r.Lower.Format("2006-01-02")
		open = '['
	}
	if !r.Upper.IsZero() {
		upper = r.Upper.Format("2006-01-02")
	}
	return fmt.Sprintf("%c%s,%s)", open, lower, upper)
}
