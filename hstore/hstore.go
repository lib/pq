package hstore

import (
	"database/sql"
	"database/sql/driver"
	"strings"
)

// A simple wrapper for transferring Hstore values back and forth easily.
// NULL values are converted to the string "NULL", so if you need to
// differentiate then you must use the HstoreWithNulls type.
type Hstore struct {
	Map map[string]string
}

// A wrapper for transferring Hstore values back and forth without loss of
// data. If all of your hstore values are non-null, your code will be cleaner
// if you use the Hstore type instead.
type HstoreWithNulls struct {
	Map map[string]sql.NullString
}

type hstorable interface {
	// value is either string or nil
	put(key string, value interface{})
}

func (h Hstore) put(key string, value interface{}) {
	if value == nil {
		h.Map[key] = "NULL"
	} else {
		h.Map[key] = value.(string)
	}
}

func (h HstoreWithNulls) put(key string, value interface{}) {
	if value == nil {
		h.Map[key] = sql.NullString{String: "", Valid: false}
	} else {
		h.Map[key] = sql.NullString{String: value.(string), Valid: true}
	}
}

// escapes and quotes hstore keys/values
// s should be a sql.NullString or string
func hQuote(s interface{}) string {
	var str string
	switch v := s.(type) {
	case sql.NullString:
		if !v.Valid {
			return "NULL"
		}
		str = v.String
	case string:
		str = v
	default:
		panic("not a string or sql.NullString")
	}

	str = strings.Replace(str, "\\", "\\\\", -1)
	return `"` + strings.Replace(str, "\"", "\\\"", -1) + `"`
}

func hstoreScan(h hstorable, value interface{}) error {
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
						h.put(string(pair[0]), nil)
					} else {
						h.put(string(pair[0]), s)
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
			h.put(string(pair[0]), nil)
		} else {
			h.put(string(pair[0]), s)
		}
	}
	return nil
}

// Scan implements the Scanner interface.
//
// Note h.Map is reallocated before the scan to clear existing values. If the
// hstore column's database value is NULL, then h.Map is set to nil instead.
func (h *HstoreWithNulls) Scan(value interface{}) error {
	if value == nil {
		h.Map = nil
		return nil
	}
	h.Map = make(map[string]sql.NullString)
	return hstoreScan(h, value)
}

// Value implements the driver Valuer interface.
// Note if h.Map is nil, the database value with be set to NULL also.
func (h HstoreWithNulls) Value() (driver.Value, error) {
	if h.Map == nil {
		return nil, nil
	}
	parts := []string{}
	for key, val := range h.Map {
		thispart := hQuote(key) + "=>" + hQuote(val)
		parts = append(parts, thispart)
	}
	return []byte(strings.Join(parts, ",")), nil
}

// Scan implements the Scanner interface. NULL hstore values will be converted
// to the string "NULL" automatically, if you want to differentiate use the
// HstoreWithNulls type which uses sql.NullString.
//
// Note h.Map is reallocated before the scan to clear existing values. If the
// hstore column's database value is NULL, then h.Map is set to nil instead.
func (h *Hstore) Scan(value interface{}) error {
	if value == nil {
		h.Map = nil
		return nil
	}
	h.Map = make(map[string]string)
	return hstoreScan(h, value)
}

// Value implements the driver Valuer interface. Note if h.Map is nil, the
// database column value will be set to NULL.
func (h Hstore) Value() (driver.Value, error) {
	if h.Map == nil {
		return nil, nil
	}
	parts := []string{}
	for key, val := range h.Map {
		thispart := hQuote(key) + "=>" + hQuote(val)
		parts = append(parts, thispart)
	}
	return []byte(strings.Join(parts, ",")), nil
}
