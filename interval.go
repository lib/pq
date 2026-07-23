package pq

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Duration is a time.Duration that implements sql.Scanner and driver.Valuer
// for PostgreSQL interval values.
//
// Scanning supports the default PostgreSQL text format produced for interval
// (days / hours / minutes / seconds). Months and years cannot be represented
// exactly as a Duration and return an error.
//
// Encoding writes a seconds-based interval PostgreSQL accepts, e.g. "1 day
// 02:03:04.000005".
type Duration time.Duration

// Scan implements the sql.Scanner interface.
func (d *Duration) Scan(src any) error {
	if src == nil {
		*d = 0
		return fmt.Errorf("pq: scanning NULL interval into Duration; use NullDuration")
	}
	dur, err := parseInterval(src)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// Value implements the driver.Valuer interface.
func (d Duration) Value() (driver.Value, error) {
	return formatInterval(time.Duration(d)), nil
}

// NullDuration represents an interval that may be null.
type NullDuration struct {
	Duration time.Duration
	Valid    bool
}

// Scan implements the sql.Scanner interface.
func (n *NullDuration) Scan(src any) error {
	if src == nil {
		n.Duration, n.Valid = 0, false
		return nil
	}
	dur, err := parseInterval(src)
	if err != nil {
		return err
	}
	n.Duration, n.Valid = dur, true
	return nil
}

// Value implements the driver.Valuer interface.
func (n NullDuration) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return formatInterval(n.Duration), nil
}

func parseInterval(src any) (time.Duration, error) {
	switch v := src.(type) {
	case string:
		return ParseInterval(v)
	case []byte:
		return ParseInterval(string(v))
	default:
		return 0, fmt.Errorf("pq: cannot scan %T into Duration", src)
	}
}

// ParseInterval parses a PostgreSQL interval in the default text style into a
// time.Duration. Units that cannot be mapped exactly (year, month) error.
//
// Accepted examples:
//
//	"00:00:01"
//	"3 days"
//	"1 day 02:03:04"
//	"1 day 02:03:04.5"
//	"-1 days +02:03:04"
//	"@ 1 day"          (postgres_verbose, leading @ ignored)
//	"P1DT2H3M4S"       (iso8601, basic subset)
func ParseInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("pq: empty interval")
	}
	if strings.HasPrefix(s, "@") {
		s = strings.TrimSpace(s[1:])
	}
	if len(s) > 0 && (s[0] == 'P' || s[0] == 'p') {
		return parseISO8601Interval(s)
	}
	return parsePostgresInterval(s)
}

func parsePostgresInterval(s string) (time.Duration, error) {
	// Tokenize on whitespace; keep sign glued to numbers when present.
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, fmt.Errorf("pq: empty interval")
	}

	var total time.Duration
	i := 0
	for i < len(fields) {
		tok := fields[i]

		// Time-of-day style: [+-]HH:MM:SS[.frac]
		if isTimeClockToken(tok) {
			d, err := parseClockDuration(tok)
			if err != nil {
				return 0, err
			}
			total += d
			i++
			continue
		}

		// number + unit (unit may be next token or glued)
		numStr, unit, ok := splitNumberUnit(tok)
		if !ok {
			return 0, fmt.Errorf("pq: invalid interval token %q", tok)
		}
		if unit == "" {
			if i+1 >= len(fields) {
				return 0, fmt.Errorf("pq: interval number %q missing unit", numStr)
			}
			unit = fields[i+1]
			i += 2
		} else {
			i++
		}

		n, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, fmt.Errorf("pq: invalid interval number %q", numStr)
		}
		d, err := durationForUnit(n, unit)
		if err != nil {
			return 0, err
		}
		if unit == "ago" || strings.EqualFold(unit, "ago") {
			total = -total
		} else {
			total += d
		}
	}
	return total, nil
}

func durationForUnit(n float64, unit string) (time.Duration, error) {
	switch strings.ToLower(unit) {
	case "microsecond", "microseconds", "us", "usec", "usecs":
		return time.Duration(n * float64(time.Microsecond)), nil
	case "millisecond", "milliseconds", "ms", "msec", "msecs":
		return time.Duration(n * float64(time.Millisecond)), nil
	case "second", "seconds", "sec", "secs":
		return time.Duration(n * float64(time.Second)), nil
	case "minute", "minutes", "min", "mins":
		return time.Duration(n * float64(time.Minute)), nil
	case "hour", "hours", "hr", "hrs":
		return time.Duration(n * float64(time.Hour)), nil
	case "day", "days":
		return time.Duration(n * float64(24*time.Hour)), nil
	case "week", "weeks":
		return time.Duration(n * float64(7*24*time.Hour)), nil
	case "mon", "mons", "month", "months":
		return 0, fmt.Errorf("pq: cannot convert interval months to time.Duration")
	case "year", "years", "yr", "yrs":
		return 0, fmt.Errorf("pq: cannot convert interval years to time.Duration")
	case "ago":
		return 0, nil
	default:
		return 0, fmt.Errorf("pq: unsupported interval unit %q", unit)
	}
}

func isTimeClockToken(tok string) bool {
	// must contain two colons or one colon with digits (HH:MM[:SS])
	if strings.Count(tok, ":") == 0 {
		return false
	}
	// strip leading sign
	t := tok
	if t[0] == '+' || t[0] == '-' {
		t = t[1:]
	}
	for _, r := range t {
		if r != ':' && r != '.' && !unicode.IsDigit(r) {
			return false
		}
	}
	return strings.Count(t, ":") >= 1
}

func parseClockDuration(tok string) (time.Duration, error) {
	sign := time.Duration(1)
	if tok[0] == '-' {
		sign = -1
		tok = tok[1:]
	} else if tok[0] == '+' {
		tok = tok[1:]
	}
	parts := strings.Split(tok, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("pq: invalid interval time %q", tok)
	}
	h, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, fmt.Errorf("pq: invalid interval hours in %q", tok)
	}
	m, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, fmt.Errorf("pq: invalid interval minutes in %q", tok)
	}
	var sec float64
	if len(parts) == 3 {
		sec, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return 0, fmt.Errorf("pq: invalid interval seconds in %q", tok)
		}
	}
	d := time.Duration(h*float64(time.Hour) + m*float64(time.Minute) + sec*float64(time.Second))
	return sign * d, nil
}

func splitNumberUnit(tok string) (num, unit string, ok bool) {
	if tok == "ago" {
		return "0", "ago", true
	}
	sign := ""
	i := 0
	if tok[0] == '+' || tok[0] == '-' {
		sign = string(tok[0])
		i = 1
	}
	start := i
	for i < len(tok) && (unicode.IsDigit(rune(tok[i])) || tok[i] == '.') {
		i++
	}
	if i == start {
		return "", "", false
	}
	num = sign + tok[start:i]
	unit = tok[i:]
	return num, unit, true
}

func parseISO8601Interval(s string) (time.Duration, error) {
	// Basic subset: PnDTnHnMnS / PT... without months/years.
	s = strings.ToUpper(strings.TrimSpace(s))
	if s[0] != 'P' {
		return 0, fmt.Errorf("pq: invalid iso8601 interval %q", s)
	}
	s = s[1:]
	neg := false
	if strings.HasPrefix(s, "-") {
		// uncommon
		neg = true
		s = s[1:]
	}
	var total time.Duration
	inTime := false
	i := 0
	for i < len(s) {
		if s[i] == 'T' {
			inTime = true
			i++
			continue
		}
		start := i
		for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.' || s[i] == '-') {
			i++
		}
		if i == start || i >= len(s) {
			return 0, fmt.Errorf("pq: invalid iso8601 interval")
		}
		n, err := strconv.ParseFloat(s[start:i], 64)
		if err != nil {
			return 0, fmt.Errorf("pq: invalid iso8601 number in %q", s)
		}
		u := s[i]
		i++
		switch u {
		case 'Y':
			return 0, fmt.Errorf("pq: cannot convert interval years to time.Duration")
		case 'M':
			if inTime {
				total += time.Duration(n * float64(time.Minute))
			} else {
				return 0, fmt.Errorf("pq: cannot convert interval months to time.Duration")
			}
		case 'W':
			total += time.Duration(n * float64(7*24*time.Hour))
		case 'D':
			total += time.Duration(n * float64(24*time.Hour))
		case 'H':
			total += time.Duration(n * float64(time.Hour))
		case 'S':
			total += time.Duration(n * float64(time.Second))
		default:
			return 0, fmt.Errorf("pq: unsupported iso8601 interval unit %q", string(u))
		}
	}
	if neg {
		total = -total
	}
	return total, nil
}

func formatInterval(d time.Duration) string {
	neg := d < 0
	if neg {
		d = -d
	}
	if d == 0 {
		return "00:00:00"
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	d -= hours * time.Hour
	mins := d / time.Minute
	d -= mins * time.Minute
	secs := d / time.Second
	d -= secs * time.Second
	usecs := d / time.Microsecond

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	if days != 0 {
		fmt.Fprintf(&b, "%d day", days)
		if days != 1 {
			b.WriteByte('s')
		}
		if hours != 0 || mins != 0 || secs != 0 || usecs != 0 {
			b.WriteByte(' ')
		}
	}
	if usecs != 0 {
		fmt.Fprintf(&b, "%02d:%02d:%02d.%06d", hours, mins, secs, usecs)
	} else if days != 0 || hours != 0 || mins != 0 || secs != 0 {
		fmt.Fprintf(&b, "%02d:%02d:%02d", hours, mins, secs)
	}
	return b.String()
}
