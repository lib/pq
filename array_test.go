package pq

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
)

var arrayTests = []struct {
	resp interface{}
	arr  interface{}
}{
	{[]byte(`{}`), []bool{}},
	{[]byte(`{{}}`), [][]bool{{}}},
	{[]byte(`{true,false}`), []bool{true, false}},
	{[]byte(`{"a","b","c\"","d,e"}`), []string{"a", "b", "c\"", "d,e"}},
	{[]byte(`{"a","b"}`), [][]byte{{'a'}, {'b'}}},
	{[]byte(`{{"a","b"},{"c","d"}}`), [][]string{{"a", "b"}, {"c", "d"}}},
	{[]byte(`{12345,23456,34567890}`), []int64{12345, 23456, 34567890}},
	{[]byte(`{12345,23456,34567890}`), []int{12345, 23456, 34567890}},
	{[]byte(`{"",NULL}`), []*string{new(string), nil}},
	{nil, nil},
}

func TestArrayValuer(t *testing.T) {
	array := Array{}
	for i, test := range arrayTests {
		array.A = test.arr

		val, err := array.Value()
		if err != nil {
			t.Errorf("%d: got error: %v", i, err)
		}

		if !reflect.DeepEqual(test.resp, val) {
			t.Errorf("%d: expected %q got %q", i, test.resp, val)
		}
	}
}

func BenchmarkArrayString(b *testing.B) {
	arr := []string{"a", "b", "c\"", "d", "e"}
	array := Array{arr}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		array.Value()
	}
}

func BenchmarkCustomArrayString(b *testing.B) {
	arr := []string{"a", "b", "c\"", "d", "e"}
	strArray := StringArray(arr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strArray.Value()
	}
}

func BenchmarkArrayInt64(b *testing.B) {
	arr := []int64{2455, 229, 109, 285, 982995}
	intArray := Array{arr}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intArray.Value()
	}
}

func BenchmarkCustomArrayInt64(b *testing.B) {
	arr := []int64{2455, 229, 109, 285, 982995}
	intArray := Int64Array(arr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intArray.Value()
	}
}

type StringArray []string

func (a StringArray) Value() (driver.Value, error) {
	var val []byte
	val = append(val, '{')
	for i, s := range a {
		if i > 0 {
			val = append(val, ',')
		}
		b := []byte(s)
		val = append(val, '"')
		for _, c := range b {
			if c == '"' {
				val = append(val, '\\')
			}
			val = append(val, c)
		}
		val = append(val, '"')
	}
	return append(val, '}'), nil
}

type Int64Array []int64

func (a Int64Array) Value() (driver.Value, error) {
	var val []byte
	val = append(val, '{')
	for i, s := range a {
		if i > 0 {
			val = append(val, ',')
		}
		b := []byte(fmt.Sprintf("%d", s))
		val = append(val, b...)
	}
	return append(val, '}'), nil
}
