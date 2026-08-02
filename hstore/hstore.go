package hstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"math"
	"strings"
)

// Hstore is a wrapper for transferring Hstore values back and forth easily.
type Hstore struct {
	Map map[string]sql.NullString
}

// Scan implements the Scanner interface.
//
// Note h.Map is reallocated before the scan to clear existing values. If the
// hstore column's database value is NULL, then h.Map is set to nil instead.
func (h *Hstore) Scan(value any) error {
	if value == nil {
		h.Map = nil
		return nil
	}
	h.Map = make(map[string]sql.NullString)
	input := value.([]byte)
	if len(input) == 0 {
		return nil
	}
	var b byte
	var pairStorage [64]byte
	pair := [2][]byte{
		pairStorage[:0:32],
		pairStorage[32:32:64],
	}
	pi := 0
	inQuote := false
	didQuote := false
	sawSlash := false
	bindex := 0
	for bindex, b = range input {
		if sawSlash {
			pair[pi] = append(pair[pi], b)
			sawSlash = false
			continue
		}

		switch b {
		case '\\':
			sawSlash = true
			continue
		case '"':
			inQuote = !inQuote
			if !didQuote {
				didQuote = true
			}
			continue
		default:
			if !inQuote {
				switch b {
				case ' ', '\t', '\n', '\r':
					continue
				case '=':
					continue
				case '>':
					pi = 1
					didQuote = false
					continue
				case ',':
					s := string(pair[1])
					if !didQuote && len(s) == 4 && strings.EqualFold(s, "null") {
						h.Map[string(pair[0])] = sql.NullString{String: "", Valid: false}
					} else {
						h.Map[string(pair[0])] = sql.NullString{String: s, Valid: true}
					}
					pair[0] = pair[0][:0]
					pair[1] = pair[1][:0]
					pi = 0
					continue
				}
			}
		}
		pair[pi] = append(pair[pi], b)
	}
	if bindex > 0 {
		s := string(pair[1])
		if !didQuote && len(s) == 4 && strings.EqualFold(s, "null") {
			h.Map[string(pair[0])] = sql.NullString{String: "", Valid: false}
		} else {
			h.Map[string(pair[0])] = sql.NullString{String: s, Valid: true}
		}
	}
	return nil
}

func appendHstoreQuotedString(b []byte, str string) []byte {
	b = append(b, '"')
	for {
		escape := strings.IndexAny(str, `\"`)
		if escape < 0 {
			b = append(b, str...)
			break
		}
		b = append(b, str[:escape]...)
		b = append(b, '\\', str[escape])
		str = str[escape+1:]
	}
	return append(b, '"')
}

func appendHstoreNullString(b []byte, value sql.NullString) []byte {
	if !value.Valid {
		return append(b, "NULL"...)
	}
	return appendHstoreQuotedString(b, value.String)
}

// Value implements the driver Valuer interface. Note if h.Map is nil, the
// database column value will be set to NULL.
func (h Hstore) Value() (driver.Value, error) {
	if h.Map == nil {
		return nil, nil
	}
	if len(h.Map) == 0 {
		return []byte(""), nil
	}

	// Most hstores have short keys and values. Reserve enough room for the
	// common case while retaining a single encoding pass for larger entries.
	b := make([]byte, 0, len(h.Map)*32)
	for k, v := range h.Map {
		if len(b) > 0 {
			b = append(b, ',')
		}
		b = appendHstoreQuotedString(b, k)
		b = append(b, "=>"...)
		b = appendHstoreNullString(b, v)
	}
	return b, nil
}

func (h Hstore) BinaryValue() ([]byte, error) {
	if h.Map == nil {
		return nil, nil
	}

	size := 4
	for k, v := range h.Map {
		size += 8 + len(k)
		if v.Valid {
			size += len(v.String)
		}
	}
	b := make([]byte, 0, size)
	b = binary.BigEndian.AppendUint32(b, uint32(len(h.Map)))
	for k, v := range h.Map {
		b = binary.BigEndian.AppendUint32(b, uint32(len(k)))
		b = append(b, k...)
		if v.Valid {
			b = binary.BigEndian.AppendUint32(b, uint32(len(v.String)))
			b = append(b, v.String...)
		} else {
			b = binary.BigEndian.AppendUint32(b, math.MaxUint32)
		}
	}
	return b, nil
}
