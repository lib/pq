package hstore

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"github.com/lib/pq/internal/pqtest"
)

func TestHstore(t *testing.T) {
	pqtest.SkipCockroach(t) // "unimplemented: extension "hstore" is not yet supported (0A000)"

	tr := strings.NewReplacer("\t", "", "\n", "", `\n`, "\n", `\t`, "\t")
	tests := []struct {
		in   string
		want Hstore
	}{
		{`null`, Hstore{}},
		{`''`, Hstore{Map: map[string]sql.NullString{}}},

		{`'"key1"=>"value1"'`, Hstore{Map: map[string]sql.NullString{
			"key1": {String: "value1", Valid: true},
		}}},
		{`'"key1"=>"value1","key2"=>"value2","key3"=>"value3"'`, Hstore{Map: map[string]sql.NullString{
			"key1": {String: "value1", Valid: true},
			"key2": {String: "value2", Valid: true},
			"key3": {String: "value3", Valid: true},
		}}},
		{
			tr.Replace(`'
				"embedded2"=>"\"value2\"=>x2",
				"withnewlines"=>"\n\nvalue\t=>2",
				"nullstring"=>"NULL",
				"withbracket"=>"value>42",
				"\"withquotes1\""=>"this \"should\" be fine",
				"embedded1"=>"value1=>x1",
				"<<all sorts of crazy>>"=>"this, \"should,\\\" also, => be fine",
				"actuallynull"=>NULL,
				"NULL"=>"NULL string key",
				"withequal"=>"value=42",
				"\"withquotes\"2\""=>"this \"should\\\" also be fine"
			'`),
			Hstore{Map: map[string]sql.NullString{
				"nullstring":             {String: "NULL", Valid: true},
				"actuallynull":           {String: "", Valid: false},
				"NULL":                   {String: "NULL string key", Valid: true},
				"withbracket":            {String: "value>42", Valid: true},
				"withequal":              {String: "value=42", Valid: true},
				`"withquotes1"`:          {String: `this "should" be fine`, Valid: true},
				`"withquotes"2"`:         {String: `this "should\" also be fine`, Valid: true},
				"embedded1":              {String: "value1=>x1", Valid: true},
				"embedded2":              {String: `"value2"=>x2`, Valid: true},
				"withnewlines":           {String: "\n\nvalue\t=>2", Valid: true},
				"<<all sorts of crazy>>": {String: `this, "should,\" also, => be fine`, Valid: true},
			}}},
	}

	t.Parallel()
	db := pqtest.MustDB(t)
	pqtest.Exec(t, db, `create extension if not exists hstore`)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := pqtest.Query[Hstore](t, db, `select `+tt.in+`::hstore as hstore`)[0]["hstore"]
			if !reflect.DeepEqual(have, tt.want) {
				t.Fatalf("\nhave: %#v\nwant: %#v", have, tt.want)
			}

			have2 := pqtest.Query[Hstore](t, db, `select $1::hstore as hstore`, have)[0]["hstore"]
			if !reflect.DeepEqual(have2, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have2, tt.want)
			}
		})
	}
}

func BenchmarkHstore(b *testing.B) {
	h := Hstore{Map: map[string]sql.NullString{
		"nullstring":             {String: "NULL", Valid: true},
		"actuallynull":           {String: "", Valid: false},
		"NULL":                   {String: "NULL string key", Valid: true},
		"withbracket":            {String: "value>42", Valid: true},
		"withequal":              {String: "value=42", Valid: true},
		`"withquotes1"`:          {String: `this "should" be fine`, Valid: true},
		`"withquotes"2"`:         {String: `this "should\" also be fine`, Valid: true},
		"embedded1":              {String: "value1=>x1", Valid: true},
		"embedded2":              {String: `"value2"=>x2`, Valid: true},
		"withnewlines":           {String: "\n\nvalue\t=>2", Valid: true},
		"<<all sorts of crazy>>": {String: `this, "should,\" also, => be fine`, Valid: true},
	}}

	b.Run("Value", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = h.Value()

		}
	})
	b.Run("BinaryValue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = h.BinaryValue()
		}
	})
}
