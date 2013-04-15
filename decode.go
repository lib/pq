package pq
import "encoding/hex"
import "strconv"

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

