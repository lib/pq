package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func encode(x interface{}, pgtypOid Oid) []byte {
	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
	case []byte:
		if pgtypOid == T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}

		return v
	case string:
		if pgtypOid == T_bytea {
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

func decode(s []byte, typ Oid) interface{} {
	switch typ {
	case T_bytea:
		s = s[2:] // trim off "\\x"
		d := make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(d, s)
		if err != nil {
			errorf("%s", err)
		}
		return d
	case T_timestamptz:
		return mustParse("2006-01-02 15:04:05-07", typ, s)
	case T_timestamp:
		return mustParse("2006-01-02 15:04:05", typ, s)
	case T_time:
		return mustParse("15:04:05", typ, s)
	case T_timetz:
		return mustParse("15:04:05-07", typ, s)
	case T_date:
		return mustParse("2006-01-02", typ, s)
	case T_bool:
		return s[0] == 't'
	case T_int8, T_int2, T_int4:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			errorf("%s", err)
		}
		return i
	case T_float4, T_float8:
		bits := 64
		if typ == T_float4 {
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

func mustParse(f string, typ Oid, s []byte) time.Time {
	str := string(s)

	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if str[len(str)-2] == '.' {
		str += "0"
	}

	// check for a 30-minute-offset timezone
	if (typ == T_timestamptz || typ == T_timetz) &&
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
