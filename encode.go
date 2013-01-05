package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func encode(x interface{}, pgtypoid oid) []byte {
	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
	case []byte:
		if pgtypoid == t_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}

		return v
	case string:
		if pgtypoid == t_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}

		return []byte(v)
	case bool:
		return []byte(fmt.Sprintf("%t", v))
	case time.Time:
		return []byte(v.Format(time.RFC3339Nano))
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

func decode(s []byte, typ oid) interface{} {
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
		return mustParse("2006-01-02 15:04:05-07", typ, s)
	case t_timestamp:
		return mustParse("2006-01-02 15:04:05", typ, s)
	case t_time:
		return mustParse("15:04:05", typ, s)
	case t_timetz:
		return mustParse("15:04:05-07", typ, s)
	case t_date:
		return mustParse("2006-01-02", typ, s)
	case t_bool:
		return s[0] == 't'
	case t_int8, t_int2, t_int4:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			errorf("%s", err)
		}
		return i
	case t_float4, t_float8:
		bits := 64
		if typ == t_float4 {
			bits = 32
		}
		f, err := strconv.ParseFloat(string(s), bits)
		if err != nil {
			errorf("%s", err)
		}
		return f
	}

	return s
}

func mustParse(f string, typ oid, s []byte) time.Time {
	str := string(s)

	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if str[len(str)-2] == '.' {
		str += "0"
	}

	// check for a 30-minute-offset timezone
	if (typ == t_timestamptz || typ == t_timetz) &&
		str[len(str)-3] == ':' {
		f += ":00"
	}
	t, err := time.Parse(f, str)
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
