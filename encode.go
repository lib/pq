package pq

import "fmt"
import "time"

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
		return []byte(v.Format("2006-01-02 15:04:05.999999999Z07:00"))
		// return []byte(v.Format(time.RFC3339Nano))
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}
