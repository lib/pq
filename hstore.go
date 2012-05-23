package pq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

// Hstore represents the contents of an hstore column. Hstore implements the Scanner
// interface so it can be used as a scan destination:
//
//  var hs Hstore
//  err := db.QueryRow("SELECT data FROM foo WHERE id=?", id).Scan(&hs)
//  ...
//  if val, ok := hs["bar"]; ok {
//      if val.Valid {
//          // use val.String
//      } else {
//          // key in hstore with NULL value
//      }
//  } else {
//      // key not in hstore
//  }
//
type Hstore map[string]sql.NullString

// Scan implements the Scanner interface
func (hs *Hstore) Scan(value interface{}) error {
	return parseHstore(string(value.([]byte)), hs)
}

// Value implements the Valuer interface
func (hs *Hstore) Value() (driver.Value, error) {
	pairs := make([]string, 0, len(*hs))
	for k, v := range *hs {
		pairs = append(pairs, fmt.Sprintf(`"%s"=>%s`, k, nullStringValue(v)))
	}
	return strings.Join(pairs, ","), nil
}

func nullStringValue(ns sql.NullString) string {
	if ns.Valid {
		return `"` + ns.String + `"`
	}

	return "NULL"
}

// Hstore parsing

// The order of the pairs is not significant (and may not be reproduced on
// output). Whitespace between pairs or around the => sign is ignored. Double-
// quote keys and values that include whitespace, commas, =s or >s. To include
// a double quote or a backslash in a key or value, escape it with a
// backslash.
// http://www.postgresql.org/docs/9.1/static/hstore.html

// "id"=>"85", "foo"=>"dfs => somf", "null"=>NULL, "quote"=>"\"fs ' "

var (
	// <hstoreChar>   := [^"\] | '\"' | '\\' 
	hstoreChar = `[^"\\]|\\"|\\\\`

	// <hstoreString> := <hstoreChar>*
	hstoreString = fmt.Sprintf("(%s)*", hstoreChar)

	// <hstoreKey> := '"' <hstoreString> '"'
	hstoreKey = fmt.Sprintf(`"(?P<key>%s)"`, hstoreString)

	// <hstoreValue> := ('"' <hstoreString> '"') | 'NULL'
	hstoreValue = fmt.Sprintf(`(?P<value>"(?P<string>%s)"|NULL)`, hstoreString)

	// <pair>       := <hstoreKey> <ws> '=>' <ws> <hstoreValue>
	pairExp = regexp.MustCompile(fmt.Sprintf(`%s\s*=>\s*%s`, hstoreKey, hstoreValue))

	subexps = make(map[string]int)
)

func init() {
	for i, subexp := range pairExp.SubexpNames() {
		if subexp != "" {
			subexps[subexp] = i
		}
	}
}

// <hstore>     := <ws> <pair> (<ws> ',' <ws> <pair>)* <ws>
func parseHstore(value string, hs *Hstore) error {
	(*hs) = make(Hstore)
	pairs := pairExp.FindAllStringSubmatch(value, -1)
	for _, pair := range pairs {
		value := sql.NullString{
			String: pair[subexps["string"]],
			Valid:  pair[subexps["value"]] != "NULL",
		}
		(*hs)[pair[subexps["key"]]] = value
	}

	return nil
}
