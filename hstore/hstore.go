package hstore

import (
	"bytes"
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
	var b byte
	pair := [][]byte{{}, {}}
	pi := 0
	inQuote := false
	didQuote := false
	sawSlash := false
	bindex := 0
	for bindex, b = range value.([]byte) {
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
					if !didQuote && len(s) == 4 && strings.ToLower(s) == "null" {
						h.Map[string(pair[0])] = sql.NullString{String: "", Valid: false}
					} else {
						h.Map[string(pair[0])] = sql.NullString{String: string(pair[1]), Valid: true}
					}
					pair[0] = []byte{}
					pair[1] = []byte{}
					pi = 0
					continue
				}
			}
		}
		pair[pi] = append(pair[pi], b)
	}
	if bindex > 0 {
		s := string(pair[1])
		if !didQuote && len(s) == 4 && strings.ToLower(s) == "null" {
			h.Map[string(pair[0])] = sql.NullString{String: "", Valid: false}
		} else {
			h.Map[string(pair[0])] = sql.NullString{String: string(pair[1]), Valid: true}
		}
	}
	return nil
}

var hQuoteRepl = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

// Escapes and quotes hstore keys/values. s should be a sql.NullString or string
func hQuote(b *bytes.Buffer, s any) {
	var str string
	switch v := s.(type) {
	case sql.NullString:
		if !v.Valid {
			b.WriteString("NULL")
			return
		}
		str = v.String
	case string:
		str = v
	default:
		panic("not a string or sql.NullString")
	}

	b.WriteByte('"')
	b.WriteString(hQuoteRepl.Replace(str))
	b.WriteByte('"')
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

	b := new(bytes.Buffer)
	b.Grow(len(h.Map) * 8)
	for k, v := range h.Map {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		hQuote(b, k)
		b.WriteString("=>")
		hQuote(b, v)
	}
	return b.Bytes(), nil
}

func (h Hstore) BinaryValue() ([]byte, error) {
	if h.Map == nil {
		return nil, nil
	}

	b := make([]byte, 0, len(h.Map)*12)
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
