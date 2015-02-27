package pq

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// Clock represents a value of the PostgreSQL `time without time zone` type.
// It implements the sql.Scanner interface so it can be used as a scan
// destination.
type Clock struct {
	Hour, Minute, Second, Nanosecond int
}

// Scan implements the sql.Scanner interface.
func (c *Clock) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return c.scanString(string(src))

	case string:
		return c.scanString(src)

	case time.Time:
		return c.scanTime(src)
	}

	return fmt.Errorf("pq: cannot convert %T to Clock", src)
}

func (c *Clock) scanString(src string) (err error) {
	t, err := time.Parse("15:04:05", src)
	if err == nil {
		err = c.scanTime(t)
	}

	return
}

func (c *Clock) scanTime(src time.Time) error {
	hour, min, sec := src.Clock()
	nsec := src.Nanosecond()

	*c = Clock{Hour: hour, Minute: min, Second: sec, Nanosecond: nsec}
	return nil
}

// Value implements the driver.Valuer interface.
func (c Clock) Value() (driver.Value, error) {
	return fmt.Sprintf("%02d:%02d:%02d.%09d", c.Hour, c.Minute, c.Second, c.Nanosecond), nil
}
