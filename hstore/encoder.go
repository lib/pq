package hstore

import (
	"strings"
)

// Encode formats a map as a stringified hstore value.
// cf. http://www.postgresql.org/docs/9.2/static/hstore.html
func Encode(m map[string]string) string {
	attrs := make([]string, 0, len(m))
	for k, v := range m {
		k = strings.Replace(k, `\`, `\\`, -1)
		v = strings.Replace(v, `\`, `\\`, -1)
		k = strings.Replace(k, `"`, `\"`, -1)
		v = strings.Replace(v, `"`, `\"`, -1)
		if strings.ContainsAny(k, " ,=>") {
			k = `"` + k + `"`
		}
		if strings.ContainsAny(v, " ,=>") {
			v = `"` + v + `"`
		}
		attrs = append(attrs, k+"=>"+v)
	}
	return strings.Join(attrs, ", ")
}
