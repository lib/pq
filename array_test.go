package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestArrayParse(t *testing.T) {
	tests := []struct {
		in    string
		delim string
		dims  []int
		elems [][]byte
	}{
		{`{}`, `,`, nil, [][]byte{}},
		{`{NULL}`, `,`, []int{1}, [][]byte{nil}},
		{`{a}`, `,`, []int{1}, [][]byte{{'a'}}},
		{`{a,b}`, `,`, []int{2}, [][]byte{{'a'}, {'b'}}},
		{`{{a,b}}`, `,`, []int{1, 2}, [][]byte{{'a'}, {'b'}}},
		{`{{a},{b}}`, `,`, []int{2, 1}, [][]byte{{'a'}, {'b'}}},
		{`{{{a,b},{c,d},{e,f}}}`, `,`, []int{1, 3, 2}, [][]byte{
			{'a'}, {'b'}, {'c'}, {'d'}, {'e'}, {'f'},
		}},
		{`{""}`, `,`, []int{1}, [][]byte{{}}},
		{`{","}`, `,`, []int{1}, [][]byte{{','}}},
		{`{",",","}`, `,`, []int{2}, [][]byte{{','}, {','}}},
		{`{{",",","}}`, `,`, []int{1, 2}, [][]byte{{','}, {','}}},
		{`{{","},{","}}`, `,`, []int{2, 1}, [][]byte{{','}, {','}}},
		{`{{{",",","},{",",","},{",",","}}}`, `,`, []int{1, 3, 2}, [][]byte{
			{','}, {','}, {','}, {','}, {','}, {','},
		}},
		{`{"\"}"}`, `,`, []int{1}, [][]byte{{'"', '}'}}},
		{`{"\"","\""}`, `,`, []int{2}, [][]byte{{'"'}, {'"'}}},
		{`{{"\"","\""}}`, `,`, []int{1, 2}, [][]byte{{'"'}, {'"'}}},
		{`{{"\""},{"\""}}`, `,`, []int{2, 1}, [][]byte{{'"'}, {'"'}}},
		{`{{{"\"","\""},{"\"","\""},{"\"","\""}}}`, `,`, []int{1, 3, 2}, [][]byte{
			{'"'}, {'"'}, {'"'}, {'"'}, {'"'}, {'"'},
		}},
		{`{axyzb}`, `xyz`, []int{2}, [][]byte{{'a'}, {'b'}}},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			dims, elems, err := parseArray([]byte(tt.in), []byte(tt.delim))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(dims, tt.dims) {
				t.Errorf("dims wrong\nhave: %#v\nwant: %#v", dims, tt.dims)
			}
			if !reflect.DeepEqual(elems, tt.elems) {
				t.Errorf("elems wrong\nhave: %#v\nwant: %#v", elems, tt.elems)
			}
		})
	}
}

func TestArrayParseError(t *testing.T) {
	tests := []struct {
		in, wantErr string
	}{
		{``, "expected '{' at offset 0"},
		{`x`, "expected '{' at offset 0"},
		{`}`, "expected '{' at offset 0"},
		{`{`, "expected '}' at offset 1"},
		{`{{}`, "expected '}' at offset 3"},
		{`{}}`, "unexpected '}' at offset 2"},
		{`{,}`, "unexpected ',' at offset 1"},
		{`{,x}`, "unexpected ',' at offset 1"},
		{`{x,}`, "unexpected '}' at offset 3"},
		{`{x,{`, "unexpected '{' at offset 3"},
		{`{x},`, "unexpected ',' at offset 3"},
		{`{x}}`, "unexpected '}' at offset 3"},
		{`{{x}`, "expected '}' at offset 4"},
		{`{""x}`, "unexpected 'x' at offset 3"},
		{`{{a},{b,c}}`, "multidimensional arrays must have elements with matching dimensions"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, _, err := parseArray([]byte(tt.in), []byte{','})
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestArrayFunc(t *testing.T) {
	tests := []struct {
		in   any
		want any
	}{
		{[]bool{}, &BoolArray{}},
		{[]float64{}, &Float64Array{}},
		{[]int64{}, &Int64Array{}},
		{[]float32{}, &Float32Array{}},
		{[]int32{}, &Int32Array{}},
		{[]string{}, &StringArray{}},
		{[][]byte{}, &ByteaArray{}},
		{nil, GenericArray{nil}},
		{[]driver.Value{}, GenericArray{[]driver.Value{}}},
		{[][]bool{}, GenericArray{[][]bool{}}},
		{[][]float64{}, GenericArray{[][]float64{}}},
		{[][]int64{}, GenericArray{[][]int64{}}},
		{[][]float32{}, GenericArray{[][]float32{}}},
		{[][]int32{}, GenericArray{[][]int32{}}},
		{[][]string{}, GenericArray{[][]string{}}},

		{&[]bool{}, &BoolArray{}},
		{&[]float64{}, &Float64Array{}},
		{&[]int64{}, &Int64Array{}},
		{&[]float32{}, &Float32Array{}},
		{&[]int32{}, &Int32Array{}},
		{&[]string{}, &StringArray{}},
		{&[][]byte{}, &ByteaArray{}},
		{&[]sql.Scanner{}, GenericArray{&[]sql.Scanner{}}},
		{&[][]bool{}, GenericArray{&[][]bool{}}},
		{&[][]float64{}, GenericArray{&[][]float64{}}},
		{&[][]int64{}, GenericArray{&[][]int64{}}},
		{&[][]float32{}, GenericArray{&[][]float32{}}},
		{&[][]int32{}, GenericArray{&[][]int32{}}},
		{&[][]string{}, GenericArray{&[][]string{}}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := Array(tt.in)
			if !reflect.DeepEqual(have, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
			if _, ok := have.(sql.Scanner); !ok {
				t.Error("not a sql.Scanner")
			}
			if _, ok := have.(driver.Valuer); !ok {
				t.Error("not a driver.Valuer")
			}
		})
	}
}

func TestArrayParameter(t *testing.T) {
	tests := []struct {
		pgType  string
		in, out any
	}{
		{"int[]", []int{245, 231}, []int64{245, 231}},
		{"int[]", &[]int{245, 231}, []int64{245, 231}},
		{"int[]", []int64{245, 231}, nil},
		{"int[]", &[]int64{245, 231}, []int64{245, 231}},
		{"varchar[]", []string{"hello", "world"}, nil},
		{"varchar[]", &[]string{"hello", "world"}, []string{"hello", "world"}},
	}

	db := pqtest.MustDB(t)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if tt.out == nil {
				tt.out = tt.in
			}

			have := reflect.New(reflect.TypeOf(tt.out))
			err := db.QueryRow(fmt.Sprintf("select $1::%s", tt.pgType), tt.in).Scan(Array(have.Interface()))
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tt.out, have.Elem().Interface()) {
				t.Errorf("\nhave: %v\nwant %v", have, tt.out)
			}
		})
	}
}

type TildeNullInt64 struct{ sql.NullInt64 }

func (TildeNullInt64) ArrayDelimiter() string { return "~" }
func ptr[T any](t T) *T                       { return &t }

func TestArrayScan(t *testing.T) {
	var (
		newBool    = func() *BoolArray { return &BoolArray{true, true, true} }
		newBytea   = func() *ByteaArray { return &ByteaArray{{2}, {6}, {0, 0}} }
		newString  = func() *StringArray { return &StringArray{"x", "y", "z"} }
		newInt64   = func() *Int64Array { return &Int64Array{1, 2, 3} }
		newInt32   = func() *Int32Array { return &Int32Array{4, 5, 6} }
		newFloat64 = func() *Float64Array { return &Float64Array{7.1, 7.2, 7.3} }
		newFloat32 = func() *Float32Array { return &Float32Array{8.1, 8.2, 8.3} }
	)
	tests := []struct {
		array   sql.Scanner
		in      any
		want    driver.Valuer
		wantErr string
	}{
		{&BoolArray{}, nil, new(BoolArray), ``},
		{&BoolArray{}, `{}`, &BoolArray{}, ``},
		{&BoolArray{}, `{t}`, &BoolArray{true}, ``},
		{&BoolArray{true, true, true}, `{}`, &BoolArray{}, ``},
		{&BoolArray{true, true, true}, `{t}`, &BoolArray{true}, ``},
		{&BoolArray{true, true, true}, `{f,t}`, &BoolArray{false, true}, ``},

		{&BoolArray{}, 1, &BoolArray{}, `int to BoolArray`},
		{newBool(), ``, newBool(), `unable to parse array`},
		{newBool(), `{`, newBool(), `unable to parse array`},
		{newBool(), `{{t},{f}}`, newBool(), `cannot convert ARRAY[2][1] to BoolArray`},
		{newBool(), `{NULL}`, newBool(), `could not parse boolean array index 0: invalid boolean ""`},
		{newBool(), `{a}`, newBool(), `could not parse boolean array index 0: invalid boolean "a"`},
		{newBool(), `{t,b}`, newBool(), `could not parse boolean array index 1: invalid boolean "b"`},
		{newBool(), `{t,f,cd}`, newBool(), `could not parse boolean array index 2: invalid boolean "cd"`},

		{&ByteaArray{}, nil, new(ByteaArray), ``},
		{&ByteaArray{}, `{}`, &ByteaArray{}, ``},
		{newBytea(), `{}`, &ByteaArray{}, ``},
		{newBytea(), `{NULL}`, &ByteaArray{nil}, ``},
		{newBytea(), `{"\\xfeff"}`, &ByteaArray{{'\xFE', '\xFF'}}, ``},
		{newBytea(), `{"\\xdead","\\xbeef"}`, &ByteaArray{{'\xDE', '\xAD'}, {'\xBE', '\xEF'}}, ``},
		{&ByteaArray{{2}, {6}, {0, 0}}, ``, newBytea(), `unable to parse array`},
		{&ByteaArray{{2}, {6}, {0, 0}}, `{`, newBytea(), `unable to parse array`},
		{&ByteaArray{{2}, {6}, {0, 0}}, `{{"\\xfeff"},{"\\xbeef"}}`, newBytea(), `cannot convert ARRAY[2][1] to ByteaArray`},
		{&ByteaArray{{2}, {6}, {0, 0}}, `{"\\abc"}`, newBytea(), `could not parse bytea array index 0: could not parse bytea value`},

		{&StringArray{}, nil, new(StringArray), ``},
		{&StringArray{}, `{}`, &StringArray{}, ``},
		{newString(), `{}`, &StringArray{}, ``},
		{newString(), `{}`, &StringArray{}, ``},
		{newString(), `{t}`, &StringArray{"t"}, ``},
		{newString(), `{f,1}`, &StringArray{"f", "1"}, ``},
		{newString(), `{"a\\b","c d",","}`, &StringArray{"a\\b", "c d", ","}, ``},
		{newString(), true, newString(), `cannot convert bool to StringArray`},
		{newString(), ``, newString(), `unable to parse array`},
		{newString(), `{`, newString(), `unable to parse array`},
		{newString(), `{{a},{b}}`, newString(), `cannot convert ARRAY[2][1] to StringArray`},
		{newString(), `{NULL}`, newString(), `parsing array element index 0: cannot convert nil to string`},
		{newString(), `{a,NULL}`, newString(), `parsing array element index 1: cannot convert nil to string`},
		{newString(), `{a,b,NULL}`, newString(), `parsing array element index 2: cannot convert nil to string`},

		{&Int64Array{}, nil, new(Int64Array), ``},
		{&Int64Array{}, `{}`, &Int64Array{}, ``},
		{newInt64(), `{}`, &Int64Array{}, ``},
		{newInt64(), `{}`, &Int64Array{}, ``},
		{newInt64(), `{12}`, &Int64Array{12}, ``},
		{newInt64(), `{345,678}`, &Int64Array{345, 678}, ``},
		{newInt64(), true, newInt64(), `cannot convert bool to Int64Array`},
		{newInt64(), ``, newInt64(), `unable to parse array`},
		{newInt64(), `{`, newInt64(), `unable to parse array`},
		{newInt64(), `{{5},{6}}`, newInt64(), `cannot convert ARRAY[2][1] to Int64Array`},
		{newInt64(), `{NULL}`, newInt64(), `parsing array element index 0:`},
		{newInt64(), `{a}`, newInt64(), `parsing array element index 0:`},
		{newInt64(), `{5,a}`, newInt64(), `parsing array element index 1:`},
		{newInt64(), `{5,6,a}`, newInt64(), `parsing array element index 2:`},

		{&Int32Array{}, nil, new(Int32Array), ``},
		{&Int32Array{}, `{}`, &Int32Array{}, ``},
		{newInt32(), `{}`, &Int32Array{}, ``},
		{newInt32(), `{}`, &Int32Array{}, ``},
		{newInt32(), `{12}`, &Int32Array{12}, ``},
		{newInt32(), `{345,678}`, &Int32Array{345, 678}, ``},
		{newInt32(), true, newInt32(), `cannot convert bool to Int32Array`},
		{newInt32(), ``, newInt32(), `unable to parse array`},
		{newInt32(), `{`, newInt32(), `unable to parse array`},
		{newInt32(), `{{5},{6}}`, newInt32(), `cannot convert ARRAY[2][1] to Int32Array`},
		{newInt32(), `{NULL}`, newInt32(), `parsing array element index 0:`},
		{newInt32(), `{a}`, newInt32(), `parsing array element index 0:`},
		{newInt32(), `{5,a}`, newInt32(), `parsing array element index 1:`},
		{newInt32(), `{5,6,a}`, newInt32(), `parsing array element index 2:`},

		{&Float64Array{}, nil, new(Float64Array), ``},
		{&Float64Array{}, `{}`, &Float64Array{}, ``},
		{newFloat64(), `{}`, &Float64Array{}, ``},
		{newFloat64(), `{}`, &Float64Array{}, ``},
		{newFloat64(), `{1.2}`, &Float64Array{1.2}, ``},
		{newFloat64(), `{3.456,7.89}`, &Float64Array{3.456, 7.89}, ``},
		{newFloat64(), `{3,1,2}`, &Float64Array{3, 1, 2}, ``},
		{newFloat64(), true, newFloat64(), `cannot convert bool to Float64Array`},
		{newFloat64(), ``, newFloat64(), `unable to parse array`},
		{newFloat64(), `{`, newFloat64(), `unable to parse array`},
		{newFloat64(), `{{5.6},{7.8}}`, newFloat64(), `cannot convert ARRAY[2][1] to Float64Array`},
		{newFloat64(), `{NULL}`, newFloat64(), `parsing array element index 0:`},
		{newFloat64(), `{a}`, newFloat64(), `parsing array element index 0:`},
		{newFloat64(), `{5.6,a}`, newFloat64(), `parsing array element index 1:`},
		{newFloat64(), `{5.6,7.8,a}`, newFloat64(), `parsing array element index 2:`},

		{&Float32Array{}, nil, new(Float32Array), ``},
		{&Float32Array{}, `{}`, &Float32Array{}, ``},
		{newFloat32(), `{}`, &Float32Array{}, ``},
		{newFloat32(), `{}`, &Float32Array{}, ``},
		{newFloat32(), `{1.2}`, &Float32Array{1.2}, ``},
		{newFloat32(), `{3.456,7.89}`, &Float32Array{3.456, 7.89}, ``},
		{newFloat32(), `{3,1,2}`, &Float32Array{3, 1, 2}, ``},
		{newFloat32(), true, newFloat32(), `cannot convert bool to Float32Array`},
		{newFloat32(), ``, newFloat32(), `unable to parse array`},
		{newFloat32(), `{`, newFloat32(), `unable to parse array`},
		{newFloat32(), `{{5.6},{7.8}}`, newFloat32(), `cannot convert ARRAY[2][1] to Float32Array`},
		{newFloat32(), `{NULL}`, newFloat32(), `parsing array element index 0:`},
		{newFloat32(), `{a}`, newFloat32(), `parsing array element index 0:`},
		{newFloat32(), `{5.6,a}`, newFloat32(), `parsing array element index 1:`},
		{newFloat32(), `{5.6,7.8,a}`, newFloat32(), `parsing array element index 2:`},

		{
			&GenericArray{ptr([]sql.NullString{})},
			`{}`,
			&GenericArray{ptr([]sql.NullString{})},
			``,
		},
		{
			&GenericArray{ptr([]sql.NullString{{String: ``, Valid: true}, {}})},
			nil,
			&GenericArray{new([]sql.NullString)},
			``,
		},
		{
			&GenericArray{ptr([]sql.NullString{{String: ``, Valid: true}, {}, {}, {}, {}})},
			`{NULL,abc,"\""}`,
			&GenericArray{ptr([]sql.NullString{{}, {String: `abc`, Valid: true}, {String: `"`, Valid: true}})},
			``,
		},
		{
			&GenericArray{ptr([3]sql.NullString{{String: ``, Valid: true}, {}, {}})},
			`{NULL,"\"",xyz}`,
			&GenericArray{ptr([3]sql.NullString{{}, {String: `"`, Valid: true}, {String: `xyz`, Valid: true}})},
			``,
		},

		{
			&GenericArray{ptr([]TildeNullInt64{{sql.NullInt64{Int64: 0, Valid: true}}, {}})},
			`{12~NULL~76}`,
			&GenericArray{ptr([]TildeNullInt64{{sql.NullInt64{Int64: 12, Valid: true}}, {}, {sql.NullInt64{Int64: 76, Valid: true}}})},
			``,
		},
		{&GenericArray{nil}, nil, &GenericArray{}, `destination <nil> is not a pointer to array or slice`},
		{&GenericArray{true}, nil, &GenericArray{true}, `destination bool is not a pointer to array or slice`},
		{&GenericArray{ptr(``)}, nil, &GenericArray{ptr(``)}, `destination *string is not a pointer to array or slice`},
		{&GenericArray{[]string{}}, nil, &GenericArray{[]string{}}, `destination []string is not a pointer to array or slice`},
		{&GenericArray{ptr([1]sql.NullString{})}, nil, &GenericArray{ptr([1]sql.NullString{})}, `<nil> to [1]sql.NullString`},
		{&GenericArray{ptr([]string{})}, true, &GenericArray{ptr([]string{})}, `bool to []string`},
		{&GenericArray{ptr([]string{})}, `{{x}}`, &GenericArray{ptr([]string{})}, `multidimensional ARRAY[1][1] is not implemented`},
		{&GenericArray{ptr([]string{})}, `{{x},{x}}`, &GenericArray{ptr([]string{})}, `multidimensional ARRAY[2][1] is not implemented`},
		{&GenericArray{ptr([]string{})}, `{x}`, &GenericArray{ptr([]string{})}, `scanning to string is not implemented`},
		{&GenericArray{(*[]string)(nil)}, nil, &GenericArray{(*[]string)(nil)}, `destination *[]string is nil`},
		{&GenericArray{new([1]string)}, `{`, &GenericArray{new([1]string)}, `unable to parse`},
		{&GenericArray{new([1]string)}, `{}`, &GenericArray{new([1]string)}, `cannot convert ARRAY[0] to [1]string`},
		{&GenericArray{new([1]string)}, `{x,x}`, &GenericArray{new([1]string)}, `cannot convert ARRAY[2] to [1]string`},
		{&GenericArray{new([]sql.NullInt64)}, `{x}`, &GenericArray{new([]sql.NullInt64)}, `parsing array element index 0: converting`},
	}

	for _, tt := range tests {
		t.Run(strings.TrimPrefix(fmt.Sprintf("%T", tt.array), "*pq."), func(t *testing.T) {
			err := tt.array.Scan(tt.in)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.array, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", tt.array, tt.want)
			}

			// Run again but with []byte input instead of string.
			if str, ok := tt.in.(string); ok {
				err := tt.array.Scan([]byte(str))
				if !pqtest.ErrorContains(err, tt.wantErr) {
					t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
				}
				if !reflect.DeepEqual(tt.array, tt.want) {
					t.Errorf("\nhave: %#v\nwant: %#v", tt.array, tt.want)
				}
			}
		})
	}
}

func TestArrayScanBackend(t *testing.T) {
	tests := []struct {
		s    string
		scan sql.Scanner
		want any
	}{
		{`ARRAY[true, false]`, new(BoolArray), &BoolArray{true, false}},
		{`ARRAY[E'\\xdead', E'\\xbeef']`, new(ByteaArray), &ByteaArray{{'\xDE', '\xAD'}, {'\xBE', '\xEF'}}},
		{`ARRAY[1.2, 3.4]`, new(Float64Array), &Float64Array{1.2, 3.4}},
		{`ARRAY[1, 2, 3]`, new(Int64Array), &Int64Array{1, 2, 3}},
		{`ARRAY['a', E'\\b', 'c"', 'd,e']`, new(StringArray), &StringArray{`a`, `\b`, `c"`, `d,e`}},
	}

	db := pqtest.MustDB(t)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			err := db.QueryRow(`select ` + tt.s).Scan(tt.scan)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(tt.scan, tt.want) {
				t.Errorf("\nhave: %v\nwant %v", tt.scan, tt.want)
			}
		})
	}
}

type ByteArrayValuer [1]byte
type ByteSliceValuer []byte
type FuncArrayValuer struct {
	delimiter func() string
	value     func() (driver.Value, error)
}

func (a ByteArrayValuer) Value() (driver.Value, error) { return a[:], nil }
func (b ByteSliceValuer) Value() (driver.Value, error) { return []byte(b), nil }
func (f FuncArrayValuer) ArrayDelimiter() string       { return f.delimiter() }
func (f FuncArrayValuer) Value() (driver.Value, error) { return f.value() }

func TestArrayValue(t *testing.T) {
	tilde := func(v driver.Value) FuncArrayValuer {
		return FuncArrayValuer{
			func() string { return "~" },
			func() (driver.Value, error) { return v, nil }}
	}

	tests := []struct {
		array   driver.Valuer
		want    any
		wantErr string
	}{
		{new(BoolArray), nil, ``},
		{&BoolArray{}, `{}`, ``},
		{BoolArray{false, true, false}, `{f,t,f}`, ``},

		{new(ByteaArray), nil, ``},
		{&ByteaArray{}, `{}`, ``},
		{ByteaArray([][]byte{{'\xDE', '\xAD', '\xBE', '\xEF'}, {'\xFE', '\xFF'}, {}}), `{"\\xdeadbeef","\\xfeff","\\x"}`, ``},

		{new(StringArray), nil, ``},
		{&StringArray{}, `{}`, ``},
		{StringArray([]string{`a`, `\b`, `c"`, `d,e`}), `{"a","\\b","c\"","d,e"}`, ``},

		{new(Int64Array), nil, ``},
		{&Int64Array{}, `{}`, ``},
		{Int64Array([]int64{1, 2, 3}), `{1,2,3}`, ``},

		{new(Int32Array), nil, ``},
		{&Int32Array{}, `{}`, ``},
		{Int32Array([]int32{1, 2, 3}), `{1,2,3}`, ``},

		{new(Float64Array), nil, ``},
		{&Float64Array{}, `{}`, ``},
		{Float64Array([]float64{1.2, 3.4, 5.6}), `{1.2,3.4,5.6}`, ``},

		{new(Float32Array), nil, ``},
		{&Float32Array{}, `{}`, ``},
		{Float32Array([]float32{1.2, 3.4, 5.6}), `{1.2,3.4,5.6}`, ``},

		{GenericArray{true}, nil, `unable to convert bool to array`},
		{GenericArray{nil}, nil, ``},
		{GenericArray{[]bool(nil)}, nil, ``},
		{GenericArray{[][]int(nil)}, nil, ``},
		{GenericArray{[]*int(nil)}, nil, ``},
		{GenericArray{[]sql.NullString(nil)}, nil, ``},

		{GenericArray{[]bool{}}, `{}`, ``},
		{GenericArray{[]bool{true}}, `{true}`, ``},
		{GenericArray{[]bool{true, false}}, `{true,false}`, ``},
		{GenericArray{[2]bool{true, false}}, `{true,false}`, ``},
		{GenericArray{[][]int{{}}}, `{}`, ``},
		{GenericArray{[][]int{{}, {}}}, `{}`, ``},
		{GenericArray{[][]int{{1}}}, `{{1}}`, ``},
		{GenericArray{[][]int{{1}, {2}}}, `{{1},{2}}`, ``},
		{GenericArray{[][]int{{1, 2}, {3, 4}}}, `{{1,2},{3,4}}`, ``},
		{GenericArray{[2][2]int{{1, 2}, {3, 4}}}, `{{1,2},{3,4}}`, ``},
		{GenericArray{[]string{`a`, `\b`, `c"`, `d,e`}}, `{"a","\\b","c\"","d,e"}`, ``},
		{GenericArray{[][]byte{{'a'}, {'\\', 'b'}, {'c', '"'}, {'d', ',', 'e'}}}, `{"a","\\b","c\"","d,e"}`, ``},
		{GenericArray{[]*int{nil}}, `{NULL}`, ``},
		{GenericArray{[]*int{new(int), nil}}, `{0,NULL}`, ``},
		{GenericArray{[]sql.NullString{{}}}, `{NULL}`, ``},
		{GenericArray{[]sql.NullString{{String: `"`, Valid: true}, {}}}, `{"\"",NULL}`, ``},
		{GenericArray{[]ByteArrayValuer{{'a'}, {'b'}}}, `{"a","b"}`, ``},
		{GenericArray{[][]ByteArrayValuer{{{'a'}, {'b'}}, {{'c'}, {'d'}}}}, `{{"a","b"},{"c","d"}}`, ``},
		{GenericArray{[]ByteSliceValuer{{'e'}, {'f'}}}, `{"e","f"}`, ``},
		{GenericArray{[][]ByteSliceValuer{{{'e'}, {'f'}}, {{'g'}, {'h'}}}}, `{{"e","f"},{"g","h"}}`, ``},
		{GenericArray{[]FuncArrayValuer{tilde(int64(1)), tilde(int64(2))}}, `{1~2}`, ``},
		{GenericArray{[][]FuncArrayValuer{{tilde(int64(1)), tilde(int64(2))}, {tilde(int64(3)), tilde(int64(4))}}}, `{{1~2}~{3~4}}`, ``},
		// TODO: probably shouldn't return half arrays?
		{GenericArray{[]any{func() {}}}, `{`, `unsupported type func()`},
		{GenericArray{[]any{nil, func() {}}}, `{NULL,`, `unsupported type func(), a func`},
	}

	for _, tt := range tests {
		n := strings.TrimPrefix(strings.TrimPrefix(fmt.Sprintf("%T", tt.array), "*pq."), "pq.")
		t.Run(n, func(t *testing.T) {
			have, err := tt.array.Value()
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			if !reflect.DeepEqual(have, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func TestArrayValueBackend(t *testing.T) {
	tests := []struct {
		in   string
		v    driver.Valuer
		want string
	}{
		{`ARRAY[true, false]`, BoolArray{true, false}, `{t,f}`},
		{`ARRAY[E'\\xdead', E'\\xbeef']`, ByteaArray{{'\xDE', '\xAD'}, {'\xBE', '\xEF'}}, `{"\\xdead","\\xbeef"}`},
		{`ARRAY[1.2, 3.4]`, Float64Array{1.2, 3.4}, `{1.2,3.4}`},
		{`ARRAY[1, 2, 3]`, Int64Array{1, 2, 3}, `{1,2,3}`},
		{`ARRAY['a', E'\\b', 'c"', 'd,e']`, StringArray{`a`, `\b`, `c"`, `d,e`}, `{"a","\\b","c\"","d,e"}`},
	}

	db := pqtest.MustDB(t)
	t.Parallel()
	for _, tt := range tests {
		have := pqtest.QueryRow[string](t, db, `select $1::text`, tt.v)["text"]
		if !reflect.DeepEqual(have, tt.want) {
			t.Errorf("\nhave: %v\nwant: %v", have, tt.want)
		}
	}
}

func BenchmarkArray(b *testing.B) {
	tests := []struct {
		arr interface {
			driver.Valuer
			sql.Scanner
		}
		data []byte
	}{
		{&BoolArray{}, []byte(`{t,f,t,f,t,f,t,f,t,f}`)},
		{&ByteaArray{}, []byte(`{"\\xfe","\\xff","\\xdead","\\xbeef","\\xfe","\\xff","\\xdead","\\xbeef","\\xfe","\\xff"}`)},
		{&Float64Array{}, []byte(`{1.2,3.4,5.6,7.8,9.01,2.34,5.67,8.90,1.234,5.678}`)},
		{&Int64Array{}, []byte(`{1,2,3,4,5,6,7,8,9,0}`)},
		{&Float32Array{}, []byte(`{1.2,3.4,5.6,7.8,9.01,2.34,5.67,8.90,1.234,5.678}`)},
		{&Int32Array{}, []byte(`{1,2,3,4,5,6,7,8,9,0}`)},
		{&StringArray{}, []byte(`{a,b,c,d,e,f,g,h,i,j}`)},
		{&StringArray{}, []byte(`{"\a","\b","\c","\d","\e","\f","\g","\h","\i","\j"}`)},

		{&GenericArray{new([]sql.NullString)}, []byte(`{a,b,c,d,e,f,g,h,i,j}`)},
		{&GenericArray{new([]sql.NullString)}, []byte(`{"\a","\b","\c","\d","\e","\f","\g","\h","\i","\j"}`)},
	}

	for _, tt := range tests {
		b.Run(strings.TrimPrefix(fmt.Sprintf("%T", tt.arr), "*pq."), func(b *testing.B) {
			b.Run("Scan", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = tt.arr.Scan(tt.data)
				}
			})
			b.Run("Value", func(b *testing.B) {
				err := tt.arr.Scan(tt.data)
				if err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = tt.arr.Value()
				}
			})
		})
	}

	// GenericArray doesn't support Scan() on non-Scanner arrays, so construct
	// our own.
	var (
		rnd    = rand.New(rand.NewSource(1))
		bools  = make([]bool, 10)
		floats = make([]float64, 10)
		ints   = make([]int64, 10)
		byts   = make([][]byte, 10)
		strs   = make([]string, 10)
	)
	for i := range bools {
		bools[i] = rnd.Intn(2) == 0
		floats[i] = rnd.NormFloat64()
		ints[i] = rnd.Int63()
		byts[i] = bytes.Repeat([]byte(`abc"def\ghi`), 5)
		strs[i] = strings.Repeat(`abc"def\ghi`, 5)
	}
	tests2 := []struct {
		arr driver.Valuer
	}{
		{GenericArray{bools}},
		{GenericArray{floats}},
		{GenericArray{ints}},
		{GenericArray{byts}},
		{GenericArray{strs}},
	}
	for _, tt := range tests2 {
		b.Run(strings.TrimPrefix(fmt.Sprintf("%T", tt.arr), "pq."), func(b *testing.B) {
			b.Run("Value", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, _ = tt.arr.Value()
				}
			})
		})
	}
}
