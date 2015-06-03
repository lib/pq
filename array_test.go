package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestBoolArrayValue(t *testing.T) {
	result, err := BoolArray(nil).Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	result, err = BoolArray([]bool{}).Value()

	if err != nil {
		t.Fatalf("Expected no error for empty, got %v", err)
	}
	if expected := `{}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected empty, got %q", result)
	}

	result, err = BoolArray([]bool{false, true, false}).Value()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if expected := `{f,t,f}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func BenchmarkBoolArrayValue(b *testing.B) {
	rand.Seed(1)
	x := make([]bool, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.Intn(2) == 0
	}
	a := BoolArray(x)

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestByteaArrayValue(t *testing.T) {
	result, err := ByteaArray(nil).Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	result, err = ByteaArray([][]byte{}).Value()

	if err != nil {
		t.Fatalf("Expected no error for empty, got %v", err)
	}
	if expected := `{}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected empty, got %q", result)
	}

	result, err = ByteaArray([][]byte{{'\xDE', '\xAD', '\xBE', '\xEF'}, {'\xFE', '\xFF'}, {}}).Value()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if expected := `{"\\xdeadbeef","\\xfeff","\\x"}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func BenchmarkByteaArrayValue(b *testing.B) {
	rand.Seed(1)
	x := make([][]byte, 10)
	for i := 0; i < len(x); i++ {
		x[i] = make([]byte, len(x))
		for j := 0; j < len(x); j++ {
			x[i][j] = byte(rand.Int())
		}
	}
	a := ByteaArray(x)

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestFloat64ArrayValue(t *testing.T) {
	result, err := Float64Array(nil).Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	result, err = Float64Array([]float64{}).Value()

	if err != nil {
		t.Fatalf("Expected no error for empty, got %v", err)
	}
	if expected := `{}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected empty, got %q", result)
	}

	result, err = Float64Array([]float64{1.2, 3.4, 5.6}).Value()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if expected := `{1.2,3.4,5.6}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func BenchmarkFloat64ArrayValue(b *testing.B) {
	rand.Seed(1)
	x := make([]float64, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.NormFloat64()
	}
	a := Float64Array(x)

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestInt64ArrayValue(t *testing.T) {
	result, err := Int64Array(nil).Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	result, err = Int64Array([]int64{}).Value()

	if err != nil {
		t.Fatalf("Expected no error for empty, got %v", err)
	}
	if expected := `{}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected empty, got %q", result)
	}

	result, err = Int64Array([]int64{1, 2, 3}).Value()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if expected := `{1,2,3}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func BenchmarkInt64ArrayValue(b *testing.B) {
	rand.Seed(1)
	x := make([]int64, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.Int63()
	}
	a := Int64Array(x)

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestStringArrayValue(t *testing.T) {
	result, err := StringArray(nil).Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	result, err = StringArray([]string{}).Value()

	if err != nil {
		t.Fatalf("Expected no error for empty, got %v", err)
	}
	if expected := `{}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected empty, got %q", result)
	}

	result, err = StringArray([]string{`a`, `\b`, `c"`, `d,e`}).Value()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if expected := `{"a","\\b","c\"","d,e"}`; !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func BenchmarkStringArrayValue(b *testing.B) {
	x := make([]string, 10)
	for i := 0; i < len(x); i++ {
		x[i] = strings.Repeat(`abc"def\ghi`, 5)
	}
	a := StringArray(x)

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestGenericArrayValueUnsupported(t *testing.T) {
	_, err := GenericArray{true}.Value()

	if err == nil {
		t.Fatal("Expected error for bool")
	}
	if !strings.Contains(err.Error(), "bool to array") {
		t.Errorf("Expected type to be mentioned, got %q", err)
	}
}

type FuncArrayValuer struct {
	delimiter func() string
	value     func() (driver.Value, error)
}

func (f FuncArrayValuer) ArrayDelimiter() string       { return f.delimiter() }
func (f FuncArrayValuer) Value() (driver.Value, error) { return f.value() }

func TestGenericArrayValue(t *testing.T) {
	result, err := GenericArray{nil}.Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	Tilde := func(v driver.Value) FuncArrayValuer {
		return FuncArrayValuer{
			func() string { return "~" },
			func() (driver.Value, error) { return v, nil }}
	}

	for _, tt := range []struct {
		result string
		input  interface{}
	}{
		{`{}`, []bool{}},
		{`{true}`, []bool{true}},
		{`{true,false}`, []bool{true, false}},
		{`{true,false}`, [2]bool{true, false}},

		{`{}`, [][]int{{}}},
		{`{}`, [][]int{{}, {}}},
		{`{{1}}`, [][]int{{1}}},
		{`{{1},{2}}`, [][]int{{1}, {2}}},
		{`{{1,2},{3,4}}`, [][]int{{1, 2}, {3, 4}}},
		{`{{1,2},{3,4}}`, [2][2]int{{1, 2}, {3, 4}}},

		{`{"a","\\b","c\"","d,e"}`, []string{`a`, `\b`, `c"`, `d,e`}},
		{`{"a","\\b","c\"","d,e"}`, [][]byte{{'a'}, {'\\', 'b'}, {'c', '"'}, {'d', ',', 'e'}}},

		{`{NULL}`, []*int{nil}},
		{`{0,NULL}`, []*int{new(int), nil}},

		{`{NULL}`, []sql.NullString{{}}},
		{`{"\"",NULL}`, []sql.NullString{{String: `"`, Valid: true}, {}}},

		{`{1~2}`, []FuncArrayValuer{Tilde(int64(1)), Tilde(int64(2))}},
		{`{{1~2}~{3~4}}`, [][]FuncArrayValuer{{Tilde(int64(1)), Tilde(int64(2))}, {Tilde(int64(3)), Tilde(int64(4))}}},
	} {
		result, err := GenericArray{tt.input}.Value()

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.input, err)
		}
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("Expected %q for %q, got %q", tt.result, tt.input, result)
		}
	}
}

func TestGenericArrayValueErrors(t *testing.T) {
	var v []interface{}

	v = []interface{}{func() {}}
	if _, err := (GenericArray{v}).Value(); err == nil {
		t.Errorf("Expected error for %q, got nil", v)
	}

	v = []interface{}{nil, func() {}}
	if _, err := (GenericArray{v}).Value(); err == nil {
		t.Errorf("Expected error for %q, got nil", v)
	}
}

func BenchmarkGenericArrayValueBools(b *testing.B) {
	rand.Seed(1)
	x := make([]bool, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.Intn(2) == 0
	}
	a := GenericArray{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkGenericArrayValueFloat64s(b *testing.B) {
	rand.Seed(1)
	x := make([]float64, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.NormFloat64()
	}
	a := GenericArray{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkGenericArrayValueInt64s(b *testing.B) {
	rand.Seed(1)
	x := make([]int64, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.Int63()
	}
	a := GenericArray{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkGenericArrayValueByteSlices(b *testing.B) {
	x := make([][]byte, 10)
	for i := 0; i < len(x); i++ {
		x[i] = bytes.Repeat([]byte(`abc"def\ghi`), 5)
	}
	a := GenericArray{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkGenericArrayValueStrings(b *testing.B) {
	x := make([]string, 10)
	for i := 0; i < len(x); i++ {
		x[i] = strings.Repeat(`abc"def\ghi`, 5)
	}
	a := GenericArray{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func TestArrayValueBackend(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	for _, tt := range []struct {
		s string
		v driver.Valuer
	}{
		{`ARRAY[true, false]`, BoolArray{true, false}},
		{`ARRAY[E'\\xdead', E'\\xbeef']`, ByteaArray{{'\xDE', '\xAD'}, {'\xBE', '\xEF'}}},
		{`ARRAY[1.2, 3.4]`, Float64Array{1.2, 3.4}},
		{`ARRAY[1, 2, 3]`, Int64Array{1, 2, 3}},
		{`ARRAY['a', E'\\b', 'c"', 'd,e']`, StringArray{`a`, `\b`, `c"`, `d,e`}},
	} {
		var x int
		err := db.QueryRow(`SELECT 1 WHERE `+tt.s+` <> $1`, tt.v).Scan(&x)
		if err != sql.ErrNoRows {
			t.Errorf("Expected %v to equal %s, got %v", tt.v, tt.s, err)
		}
	}
}
