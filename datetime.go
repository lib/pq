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

// Date represents a value of the PostgreSQL `date` type. It supports the
// special values "infinity" and "-infinity" and implements the sql.Scanner
// interface so it can be used as a scan destination.
//
// A positive or negative value in Infinity represents the special value
// "infinity" or "-infinity", respectively.
type Date struct {
	Infinity int
	Year     int
	Month    time.Month
	Day      int
}

// Scan implements the sql.Scanner interface.
func (d *Date) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return d.scanString(string(src))

	case string:
		return d.scanString(src)

	case time.Time:
		return d.scanTime(src)
	}

	return fmt.Errorf("pq: cannot convert %T to Date", src)
}

func (d *Date) scanString(src string) (err error) {
	switch src {
	case "-infinity":
		*d = Date{Infinity: -1}

	case "infinity":
		*d = Date{Infinity: 1}

	default:
		t, err := time.Parse("2006-01-02", src)
		if err == nil {
			err = d.scanTime(t)
		}
	}

	return
}

func (d *Date) scanTime(src time.Time) error {
	year, month, day := src.Date()
	*d = Date{Year: year, Month: month, Day: day}
	return nil
}

// Value implements the driver.Valuer interface.
func (d Date) Value() (driver.Value, error) {
	switch {
	case d.Infinity < 0:
		return "-infinity", nil

	case d.Infinity > 0:
		return "infinity", nil

	default:
		return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day), nil
	}
}

// Timestamp represents a value of the PostgreSQL `timestamp without time zone`
// type. It supports the special values "infinity" and "-infinity" and
// implements the sql.Scanner interface so it can be used as a scan destination.
type Timestamp struct {
	Date
	Clock
}

// TimestampTZ represents a value of the PostgreSQL `timestamp with time zone`
// type. It supports the special values "infinity" and "-infinity" and
// implements the sql.Scanner interface so it can be used as a scan destination.
//
// A positive or negative value in Infinity represents the special value
// "infinity" or "-infinity", respectively.
type TimestampTZ struct {
	Infinity int
	Time     time.Time
}
