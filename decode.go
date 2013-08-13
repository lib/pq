package pq

import "strconv"
import "github.com/lib/pq/oid"
import "encoding/hex"

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
	case oid.T_timestamptz:
		return mustParse("2006-01-02 15:04:05-07", typ, s)
	case oid.T_timestamp:
		return mustParse("2006-01-02 15:04:05", typ, s)
	case oid.T_time:
		return mustParse("15:04:05", typ, s)
	case oid.T_timetz:
		return mustParse("15:04:05-07", typ, s)
	case oid.T_date:
		return mustParse("2006-01-02", typ, s)
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
