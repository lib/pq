package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"github.com/lib/pq/oid"
	"math"
	"strings"
	"strconv"
	"time"
)

func encode(x interface{}, pgtypOid oid.Oid) []byte {
	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
	case []byte:
		if pgtypOid == oid.T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}

		return v
	case string:
		if pgtypOid == oid.T_bytea {
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

func decode(s []byte, typ oid.Oid) interface{} {
	switch typ {
	case oid.T_bytea:
		s = s[2:] // trim off "\\x"
		d := make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(d, s)
		if err != nil {
			errorf("%s", err)
		}
		return d
	case oid.T_timestamptz, oid.T_timestamp, oid.T_date:
		return parseTs(string(s))
	case oid.T_time:
		return mustParse("15:04:05", typ, s)
	case oid.T_timetz:
		return mustParse("15:04:05-07", typ, s)
	case oid.T_bool:
		return s[0] == 't'
	case oid.T_int8, oid.T_int2, oid.T_int4:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			errorf("%s", err)
		}
		return i
	case oid.T_float4, oid.T_float8:
		bits := 64
		if typ == oid.T_float4 {
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

func mustParse(f string, typ oid.Oid, s []byte) time.Time {
	str := string(s)

	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if str[len(str)-2] == '.' {
		str += "0"
	}

	// check for a 30-minute-offset timezone
	if (typ == oid.T_timestamptz || typ == oid.T_timetz) &&
		str[len(str)-3] == ':' {
		f += ":00"
	}
	t, err := time.Parse(f, str)
	if err != nil {
		errorf("decode: %s", err)
	}
	return t
}

func expect(str, char string, pos int) {
	if c := str[pos:pos+1]; c != char {
		errorf("expected '%v' at position %v; got '%v'", char, pos, c)
	}
}

func mustAtoi(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		errorf("expected number; got '%v'", str)
	}
	return result
}

// This is a time function specific to the Postgres default DateStyle
// setting ("ISO, MDY"), the only one we currently support. This
// accounts for the discrepancies between the parsing available with
// time.Parse and the Postgres date formatting quirks.
func parseTs(str string) (result time.Time) {
	monSep := strings.IndexRune(str, '-')
	year := mustAtoi(str[:monSep])
	daySep := monSep + 3
	month := mustAtoi(str[monSep+1:daySep])
	expect(str, "-", daySep)
	timeSep := daySep + 3
	day := mustAtoi(str[daySep+1:timeSep])

	var hour, minute, second int
	if len(str) > monSep + len("01-01") + 1 {
		expect(str, " ", timeSep)
		minSep := timeSep + 3
		expect(str, ":", minSep)
		hour = mustAtoi(str[timeSep+1:minSep])
		secSep := minSep + 3
		expect(str, ":", secSep)
		minute = mustAtoi(str[minSep+1:secSep])
		secEnd := secSep + 3
		second = mustAtoi(str[secSep+1:secEnd])
	}
	remainderIdx := monSep + len("01-01 00:00:00") + 1
	// Three optional (but ordered) sections follow: the
	// fractional seconds, the time zone offset, and the BC
	// designation. We set them up here and adjust the other
	// offsets if the preceding sections exist.

	nanoSec := 0
	tzOff := 0
	bcSign := 1

	if remainderIdx < len(str) && str[remainderIdx:remainderIdx+1] == "." {
		fracStart := remainderIdx + 1
		fracOff := strings.IndexAny(str[fracStart:], "-+ ")
		if fracOff < 0 {
			fracOff = len(str)-fracStart
		}
		fracSec := mustAtoi(str[fracStart:fracStart + fracOff])
		nanoSec = fracSec * (1000000000 / int(math.Pow(10, float64(fracOff))))

		remainderIdx += fracOff + 1
	}
	if tzStart := remainderIdx; tzStart < len(str) && (str[tzStart:tzStart+1] == "-" || str[tzStart:tzStart+1] == "+") {
		// time zone separator is always '-' or '+' (UTC is +00)
		var tzSign int
		if c := str[tzStart:tzStart+1]; c == "-" {
			tzSign = -1
		} else if c == "+" {
			tzSign = +1
		} else {
			errorf("expected '-' or '+' at position %v; got %v", tzStart, c)
		}
		tzHours := mustAtoi(str[tzStart+1:tzStart+3])
		remainderIdx += 3
		var tzMin, tzSec int
		if tzStart + 3 < len(str) && str[tzStart+3:tzStart+4] == ":" {
			tzMin = mustAtoi(str[tzStart+4:tzStart+6])
			remainderIdx += 3
		}
		if tzStart + 6 < len(str) && str[tzStart+6:tzStart+7] == ":" {
			tzSec = mustAtoi(str[tzStart+7:tzStart+9])
			remainderIdx += 3
		}
		tzOff = (tzSign * tzHours * (60 * 60)) + (tzMin * 60) + tzSec
	}
	if remainderIdx < len(str) && str[remainderIdx:remainderIdx+3] == " BC" {
		bcSign = -1
		remainderIdx += 3
	}
	if remainderIdx < len(str) {
		errorf("expected end of input, got %v", str[remainderIdx:])
	}
	return time.Date(bcSign * year, time.Month(month), day,
		hour, minute, second, nanoSec,
		time.FixedZone("", tzOff))
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
