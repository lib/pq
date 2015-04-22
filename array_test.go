package pq

import (
	"bytes"
	"database/sql"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestArrayUnsupported(t *testing.T) {
	_, err := Array{true}.Value()

	if err == nil {
		t.Fatal("Expected error for bool")
	}
	if !strings.Contains(err.Error(), "bool to array") {
		t.Errorf("Expected type to be mentioned, got %q", err)
	}
}

type FuncArrayValuer struct {
	delimiter func() string
	value     func() (interface{}, error)
}

func (f FuncArrayValuer) ArrayDelimiter() string           { return f.delimiter() }
func (f FuncArrayValuer) ArrayValue() (interface{}, error) { return f.value() }

func TestArrayValue(t *testing.T) {
	result, err := Array{nil}.Value()

	if err != nil {
		t.Fatalf("Expected no error for nil, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %q", result)
	}

	Tilde := func(v interface{}) FuncArrayValuer {
		return FuncArrayValuer{
			func() string { return "~" },
			func() (interface{}, error) { return v, nil }}
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
		{`{"\"",NULL}`, []sql.NullString{{`"`, true}, {}}},

		{`{1~2}`, []FuncArrayValuer{Tilde(1), Tilde(2)}},
		{`{{1~2}~{3~4}}`, [][]FuncArrayValuer{{Tilde(1), Tilde(2)}, {Tilde(3), Tilde(4)}}},
	} {
		result, err := Array{tt.input}.Value()

		if err != nil {
			t.Fatalf("Expected no error for %q, got %v", tt.input, err)
		}
		if !reflect.DeepEqual(result, []byte(tt.result)) {
			t.Errorf("Expected %q for %q, got %q", tt.result, tt.input, result)
		}
	}
}

func TestArrayValueErrors(t *testing.T) {
	var v []interface{}

	v = []interface{}{func() {}}
	if _, err := (Array{v}).Value(); err == nil {
		t.Errorf("Expected error for %q, got nil", v)
	}

	v = []interface{}{nil, func() {}}
	if _, err := (Array{v}).Value(); err == nil {
		t.Errorf("Expected error for %q, got nil", v)
	}
}

func BenchmarkArrayValueInt64s(b *testing.B) {
	rand.Seed(1)
	x := make([]int64, 10)
	for i := 0; i < len(x); i++ {
		x[i] = rand.Int63()
	}
	a := Array{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkArrayValueBytes(b *testing.B) {
	x := make([][]byte, 10)
	for i := 0; i < len(x); i++ {
		x[i] = bytes.Repeat([]byte(`abc"def\ghi`), 5)
	}
	a := Array{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}

func BenchmarkArrayValueStrings(b *testing.B) {
	x := make([]string, 10)
	for i := 0; i < len(x); i++ {
		x[i] = strings.Repeat(`abc"def\ghi`, 5)
	}
	a := Array{x}

	for i := 0; i < b.N; i++ {
		a.Value()
	}
}
