package hstore

import (
	"database/sql"
	"database/sql/driver"
	"strconv"
	"strings"
	"testing"
)

var (
	allocationHstoreValueSink  driver.Value
	allocationHstoreBinarySink []byte
	allocationHstoreMapSink    map[string]sql.NullString
)

func allocationBenchmarkHstore() Hstore {
	return Hstore{Map: map[string]sql.NullString{
		"plain":              {String: "value", Valid: true},
		`quote"key`:          {String: `value with "quotes"`, Valid: true},
		`slash\\key`:         {String: `value\\with\\slashes`, Valid: true},
		"unicode-αβγ":        {String: "δεζ", Valid: true},
		"comma,value":        {String: "arrow=>value", Valid: true},
		"null-value":         {Valid: false},
		"another-null-value": {Valid: false},
	}}
}

func BenchmarkAllocationHstoreValue(b *testing.B) {
	h := allocationBenchmarkHstore()
	b.Run("Text", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			value, err := h.Value()
			if err != nil {
				b.Fatal(err)
			}
			allocationHstoreValueSink = value
		}
	})
	b.Run("Binary", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			value, err := h.BinaryValue()
			if err != nil {
				b.Fatal(err)
			}
			allocationHstoreBinarySink = value
		}
	})
}

func BenchmarkAllocationHstoreScan(b *testing.B) {
	input := []byte(`"plain"=>"value","quote\"key"=>"value with \"quotes\"","slash\\key"=>"value\\with\\slashes","unicode-αβγ"=>"δεζ","comma,value"=>"arrow=>value","null-value"=>NULL,"another-null-value"=>NULL`)
	var h Hstore

	b.ReportAllocs()
	for range b.N {
		if err := h.Scan(input); err != nil {
			b.Fatal(err)
		}
		allocationHstoreMapSink = h.Map
	}
}

func BenchmarkAllocationHstoreScanSmall(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		input []byte
	}{
		{"Empty", []byte{}},
		{"Single", []byte(`"key"=>"value"`)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			var h Hstore
			b.ReportAllocs()
			for range b.N {
				if err := h.Scan(benchmark.input); err != nil {
					b.Fatal(err)
				}
				allocationHstoreMapSink = h.Map
			}
		})
	}
}

func BenchmarkAllocationHstoreScanWide(b *testing.B) {
	var input strings.Builder
	for i := range 32 {
		if i > 0 {
			input.WriteByte(',')
		}
		input.WriteString(`"key`)
		input.WriteString(strconv.Itoa(i))
		input.WriteString(`"=>"value`)
		input.WriteString(strconv.Itoa(i))
		input.WriteByte('"')
	}
	encoded := []byte(input.String())
	var h Hstore

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := h.Scan(encoded); err != nil {
			b.Fatal(err)
		}
		allocationHstoreMapSink = h.Map
	}
}
