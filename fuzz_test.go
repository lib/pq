package pq

import (
	"testing"
)

func FuzzNewConnector(f *testing.F) {
	f.Add(`host=example.com port=5432 user=libpq password=secret`)
	f.Fuzz(func(t *testing.T, dsn string) {
		NewConnector(dsn)
	})
}

func FuzzParseArray(f *testing.F) {
	for _, a := range []string{
		`{"hello","world"}`,
		`{1,2}`,
		`{}`,
		`{NULL}`,
		`{a}`,
		`{a,b}`,
		`{{a,b}}`,
		`{{a},{b}}`,
		`{{{a,b},{c,d},{e,f}}}`,
		`{""}`,
		`{","}`,
		`{",",","}`,
		`{{",",","}}`,
		`{{","},{","}}`,
		`{{{",",","},{",",","},{",",","}}}`,
		`{"\"}"}`,
		`{"\"","\""}`,
		`{{"\"","\""}}`,
		`{{"\""},{"\""}}`,
		`{{{"\"","\""},{"\"","\""},{"\"","\""}}}`,
	} {
		f.Add([]byte(a))
	}

	f.Fuzz(func(t *testing.T, array []byte) {
		parseArray(array, []byte{','})
	})
}
