package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"time"
)

func encode(x interface{}) []byte {
	const timeFormat = "2006-01-02 15:04:05.0000-07"

	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
	case []byte:
		return []byte(fmt.Sprintf("\\x%x", v))
	case string:
		return []byte(v)
	case bool:
		return []byte(fmt.Sprintf("%t", v))
	case time.Time:
		return []byte(v.Format(timeFormat))
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

func decode(s []byte, typ int) interface{} {
	switch typ {
	case t_bytea:
		s = s[2:] // trim off "\\x"
		d := make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(d, s)
		if err != nil {
			errorf("%s", err)
		}
		return d
	case t_timestamptz:
		return mustParse("2006-01-02 15:04:05-07", s)
	case t_timestamp:
		return mustParse("2006-01-02 15:04:05", s)
	case t_time:
		return mustParse("15:04:05", s)
	case t_timetz:
		return mustParse("15:04:05-07", s)
	case t_date:
		return mustParse("2006-01-02", s)
	}

	return s
}

func mustParse(f string, s []byte) time.Time {
	t, err := time.Parse(f, string(s))
	if err != nil {
		errorf("decode: %s", err)
	}
	return t
}

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}
