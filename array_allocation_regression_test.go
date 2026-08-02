package pq

import (
	"bytes"
	"database/sql/driver"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type allocationNamedInt64 int64

type allocationCustomArrayValue struct {
	value driver.Value
}

func (v allocationCustomArrayValue) Value() (driver.Value, error) {
	return v.value, nil
}

func (allocationCustomArrayValue) ArrayDelimiter() string {
	return "~"
}

func TestAllocationSpecializedArrayValueExactOutput(t *testing.T) {
	tests := []struct {
		name  string
		value driver.Valuer
		want  driver.Value
	}{
		{"nil bool", BoolArray(nil), nil},
		{"empty bool", BoolArray{}, "{}"},
		{"bool", BoolArray{true, false, true}, "{t,f,t}"},
		{"nil bytea", ByteaArray(nil), nil},
		{"empty bytea", ByteaArray{}, "{}"},
		{"bytea", ByteaArray{nil, {}, {0, 1, 0xfe, 0xff}}, `{"\\x","\\x","\\x0001feff"}`},
		{"int64", Int64Array{0, -1, 42}, "{0,-1,42}"},
		{"int32", Int32Array{0, -1, 42}, "{0,-1,42}"},
		{"float64", Float64Array{0, -1.25, 42}, "{0,-1.25,42}"},
		{"float32", Float32Array{0, -1.25, 42}, "{0,-1.25,42}"},
		{
			"string escaping and unicode",
			StringArray{"plain", `quote"slash\`, "comma,value", "brace{value}", "NULL", "", "δοκιμή"},
			`{"plain","quote\"slash\\","comma,value","brace{value}","NULL","","δοκιμή"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.value.Value()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Value() = %q; want %q", got, tt.want)
			}
		})
	}
}

func TestAllocationSpecializedArrayExtremeValues(t *testing.T) {
	int64Value, err := (Int64Array{math.MinInt64, math.MaxInt64}).Value()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := int64Value, `{-9223372036854775808,9223372036854775807}`; got != want {
		t.Fatalf("Int64Array.Value() = %q; want %q", got, want)
	}

	int32Value, err := (Int32Array{math.MinInt32, math.MaxInt32}).Value()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := int32Value, `{-2147483648,2147483647}`; got != want {
		t.Fatalf("Int32Array.Value() = %q; want %q", got, want)
	}

	float64Values := Float64Array{math.MaxFloat64, math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), math.NaN()}
	float64Value, err := float64Values.Value()
	if err != nil {
		t.Fatal(err)
	}
	float64Want := "{"
	for i, value := range float64Values {
		if i > 0 {
			float64Want += ","
		}
		float64Want += strconv.FormatFloat(value, 'f', -1, 64)
	}
	float64Want += "}"
	if float64Value != float64Want {
		t.Fatalf("Float64Array.Value() = %q; want %q", float64Value, float64Want)
	}

	float32Values := Float32Array{math.MaxFloat32, math.SmallestNonzeroFloat32}
	float32Value, err := float32Values.Value()
	if err != nil {
		t.Fatal(err)
	}
	float32Want := "{" + strconv.FormatFloat(float64(float32Values[0]), 'f', -1, 32) + "," +
		strconv.FormatFloat(float64(float32Values[1]), 'f', -1, 32) + "}"
	if float32Value != float32Want {
		t.Fatalf("Float32Array.Value() = %q; want %q", float32Value, float32Want)
	}
}

func TestAllocationInt32ArrayStackBoundary(t *testing.T) {
	for _, size := range []int{10, 11} {
		values := make(Int32Array, size)
		parts := make([]string, size)
		for i := range values {
			if i&1 == 0 {
				values[i] = math.MinInt32
			} else {
				values[i] = math.MaxInt32
			}
			parts[i] = strconv.FormatInt(int64(values[i]), 10)
		}

		value, err := values.Value()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := value, "{"+strings.Join(parts, ",")+"}"; got != want {
			t.Fatalf("Int32Array.Value() with %d values = %q; want %q", size, got, want)
		}
	}
}

func TestAllocationGenericArrayBuiltinValues(t *testing.T) {
	tests := []struct {
		name  string
		value GenericArray
		want  string
	}{
		{"bool", GenericArray{[]bool{true, false}}, `{true,false}`},
		{"float64", GenericArray{[]float64{1.25, -2.5}}, `{1.25,-2.5}`},
		{"int64", GenericArray{[]int64{math.MinInt64, math.MaxInt64}}, `{-9223372036854775808,9223372036854775807}`},
		{"string", GenericArray{[]string{`quote"slash\`, "comma,value", ""}}, `{"quote\"slash\\","comma,value",""}`},
		{"byte slice", GenericArray{[][]byte{[]byte(`quote"slash\`), nil}}, `{"quote\"slash\\",""}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.value.Value()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Value() = %q; want %q", got, tt.want)
			}
		})
	}
}

func TestAllocationGenericArrayNamedAndCustomFallback(t *testing.T) {
	named, err := (GenericArray{[]allocationNamedInt64{-1, 0, 42}}).Value()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := named, driver.Value(`{-1,0,42}`); got != want {
		t.Fatalf("named values = %q; want %q", got, want)
	}

	custom, err := (GenericArray{[]allocationCustomArrayValue{
		{value: `quote"`},
		{value: nil},
		{value: int64(42)},
	}}).Value()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := custom, driver.Value(`{"quote\""~NULL~42}`); got != want {
		t.Fatalf("custom Valuer/ArrayDelimiter values = %q; want %q", got, want)
	}
}

func TestAllocationGenericArrayTimestampValue(t *testing.T) {
	value, err := (GenericArray{[]time.Time{
		time.Date(2001, time.January, 2, 3, 4, 5, 6, time.UTC),
		time.Date(2024, time.December, 31, 23, 59, 59, 123456789, time.FixedZone("", 4*60*60+32)),
	}}).Value()
	if err != nil {
		t.Fatal(err)
	}
	const want = `{2001-01-02 03:04:05.000000006Z,2024-12-31 23:59:59.123456789+04:00:32}`
	if value != want {
		t.Fatalf("timestamp values = %q; want %q", value, want)
	}
}

func TestAllocationByteaArrayStackBoundary(t *testing.T) {
	for _, size := range []int{60, 61} {
		value := bytes.Repeat([]byte{0xab}, size)
		got, err := (ByteaArray{value}).Value()
		if err != nil {
			t.Fatal(err)
		}
		want := `{"\\x` + strings.Repeat("ab", size) + `"}`
		if got != want {
			t.Fatalf("ByteaArray.Value() with %d bytes = %q; want %q", size, got, want)
		}
	}
}

func TestAllocationStringArrayStackBoundary(t *testing.T) {
	for _, size := range []int{62, 63} {
		value := strings.Repeat(`\`, size)
		got, err := (StringArray{value}).Value()
		if err != nil {
			t.Fatal(err)
		}
		want := `{"` + strings.Repeat(`\\`, size) + `"}`
		if got != want {
			t.Fatalf("StringArray.Value() with %d escaped bytes = %q; want %q", size, got, want)
		}
	}
}

func TestAllocationParseArrayQuotedScratchOwnership(t *testing.T) {
	src := []byte(`{"first","sec\\ond","commas,,,",""}`)
	_, elems, err := parseArray(src, []byte{','})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{[]byte("first"), []byte(`sec\ond`), []byte("commas,,,"), {}}
	if len(elems) != len(want) {
		t.Fatalf("parsed %d elements; want %d", len(elems), len(want))
	}
	for i := range elems {
		if !bytes.Equal(elems[i], want[i]) {
			t.Fatalf("element %d = %q; want %q", i, elems[i], want[i])
		}
		if cap(elems[i]) != len(elems[i]) {
			t.Fatalf("element %d capacity = %d; want isolated capacity %d", i, cap(elems[i]), len(elems[i]))
		}
	}

	for i := range src {
		src[i] = 'x'
	}
	for i := range elems {
		if !bytes.Equal(elems[i], want[i]) {
			t.Fatalf("element %d retained the source buffer: got %q, want %q", i, elems[i], want[i])
		}
	}

	first := append(elems[0], 'x')
	if !bytes.Equal(first, []byte("firstx")) || !bytes.Equal(elems[1], want[1]) {
		t.Fatalf("appending one element affected shared scratch: first=%q second=%q", first, elems[1])
	}
}

func TestAllocationParseArrayQuotedScratchGrowth(t *testing.T) {
	want := [][]byte{
		bytes.Repeat([]byte("a"), 80),
		bytes.Repeat([]byte("b"), 80),
	}
	src := []byte(`{"` + string(want[0]) + `","` + string(want[1]) + `"}`)
	_, elems, err := parseArray(src, []byte{','})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(elems, want) {
		t.Fatalf("elements = %q; want %q", elems, want)
	}
	for i := range src {
		src[i] = 'x'
	}
	if !reflect.DeepEqual(elems, want) {
		t.Fatalf("elements retained source storage after scratch growth: got %q; want %q", elems, want)
	}
}

func TestAllocationParseArrayElementCapacityHintIsBounded(t *testing.T) {
	src := []byte(`{"` + strings.Repeat(",", 128) + `"}`)
	_, elems, err := parseArray(src, []byte{','})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cap(elems), 16; got != want {
		t.Fatalf("element capacity = %d; want bounded hint %d", got, want)
	}
}
