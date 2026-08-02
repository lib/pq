package pq

import (
	"database/sql/driver"
	"strings"
	"testing"
	"time"
)

var allocationArrayValueSink driver.Value
var allocationArrayElementsSink [][]byte

func BenchmarkAllocationSpecializedArrayValue(b *testing.B) {
	benchmarks := []struct {
		name  string
		value driver.Valuer
	}{
		{"Bool", BoolArray{true, false, true, true, false, false, true, false, true, false}},
		{"Bytea", ByteaArray{
			[]byte("0123456789abcdef"),
			[]byte("payload with more bytes"),
			{},
		}},
		{"Int64", Int64Array{0, 1, -1, 42, -42, 123456789, -987654321, 1 << 40, -(1 << 40), 99}},
		{"Int32", Int32Array{0, 1, -1, 42, -42, 123456789, -987654321, 99}},
		{"Float64", Float64Array{0, 1.25, -2.5, 3.1415926535, 1e20, 1e-9, -42.125}},
		{"Float32", Float32Array{0, 1.25, -2.5, 3.14159, 1e20, 1e-9, -42.125}},
		{"String", StringArray{"plain", `quote"`, `slash\\`, "comma,value", "unicode-αβγ", ""}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				value, err := benchmark.value.Value()
				if err != nil {
					b.Fatal(err)
				}
				allocationArrayValueSink = value
			}
		})
	}
}

func BenchmarkAllocationSpecializedArrayScan(b *testing.B) {
	benchmarks := []struct {
		name  string
		scan  interface{ Scan(any) error }
		value []byte
	}{
		{"Bool", new(BoolArray), []byte(`{t,f,t,t,f,f,t,f,t,f}`)},
		{"Int64", new(Int64Array), []byte(`{0,1,-1,42,-42,123456789,-987654321,1099511627776,-1099511627776,99}`)},
		{"String", new(StringArray), []byte(`{"plain","quote\"","slash\\\\","comma,value","unicode-αβγ",""}`)},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if err := benchmark.scan.Scan(benchmark.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAllocationParseArrayQuotedScratch(b *testing.B) {
	inputs := []struct {
		name  string
		value []byte
	}{
		{"SparseQuotedLarge", []byte(`{"",` + strings.Repeat("x", 64<<10) + `}`)},
		{"LargeQuoted", []byte(`{"` + strings.Repeat("x", 64<<10) + `"}`)},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, elems, err := parseArray(input.value, []byte{','})
				if err != nil {
					b.Fatal(err)
				}
				allocationArrayElementsSink = elems
			}
		})
	}
}

func BenchmarkAllocationGenericArrayValue(b *testing.B) {
	benchmarks := []struct {
		name  string
		value driver.Valuer
	}{
		{"Bool", GenericArray{[]bool{true, false, true, true, false, false, true, false, true, false}}},
		{"Float64", GenericArray{[]float64{0, 1.25, -2.5, 3.1415926535, 1e20, 1e-9, -42.125, 99, -100.5, 7.75}}},
		{"Int64", GenericArray{[]int64{0, 1, -1, 42, -42, 123456789, -987654321, 1 << 40, -(1 << 40), 99}}},
		{"Bytea", GenericArray{[][]byte{
			[]byte(`abc"def\ghi`), []byte(`longer byte slice with "quotes"`), nil,
			[]byte(`abc"def\ghi`), []byte(`longer byte slice with "quotes"`),
		}}},
		{"String", GenericArray{[]string{
			`abc"def\ghi`, `longer string with "quotes"`, "", "comma,value", "unicode-αβγ",
			`abc"def\ghi`, `longer string with "quotes"`, "", "comma,value", "unicode-αβγ",
		}}},
		{"Timestamp", GenericArray{[]time.Time{
			time.Date(2001, time.January, 2, 3, 4, 5, 6, time.UTC),
			time.Date(2024, time.December, 31, 23, 59, 59, 123456789, time.FixedZone("", 4*60*60+32)),
		}}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				value, err := benchmark.value.Value()
				if err != nil {
					b.Fatal(err)
				}
				allocationArrayValueSink = value
			}
		})
	}
}
