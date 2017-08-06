// +build go1.8

package pq

import (
	"math"
	"reflect"
	"testing"

	"github.com/lib/pq/oid"
)

func TestDataTypeName(t *testing.T) {
	tts := []struct {
		typ  oid.Oid
		name string
	}{
		{oid.T_int8, "INT8"},
		{oid.T_int4, "INT4"},
		{oid.T_int2, "INT2"},
		{oid.T_varchar, "VARCHAR"},
		{oid.T_text, "TEXT"},
		{oid.T_bool, "BOOL"},
		{oid.T_numeric, "NUMERIC"},
		{oid.T_date, "DATE"},
		{oid.T_time, "TIME"},
		{oid.T_timetz, "TIMETZ"},
		{oid.T_timestamp, "TIMESTAMP"},
		{oid.T_timestamptz, "TIMESTAMPTZ"},
		{oid.T_bytea, "BYTEA"},
	}

	for i, tt := range tts {
		dt := fieldDesc{OID: tt.typ}
		if name := dt.Name(); name != tt.name {
			t.Errorf("(%d) got: %s want: %s", i, name, tt.name)
		}
	}
}

func TestDataType(t *testing.T) {
	tts := []struct {
		typ  oid.Oid
		kind reflect.Kind
	}{
		{oid.T_int8, reflect.Int64},
		{oid.T_int4, reflect.Int32},
		{oid.T_int2, reflect.Int16},
		{oid.T_varchar, reflect.String},
		{oid.T_text, reflect.String},
		{oid.T_bool, reflect.Bool},
		{oid.T_numeric, reflect.Float64},
		{oid.T_date, reflect.Struct},
		{oid.T_time, reflect.Struct},
		{oid.T_timetz, reflect.Struct},
		{oid.T_timestamp, reflect.Struct},
		{oid.T_timestamptz, reflect.Struct},
		{oid.T_bytea, reflect.Slice},
	}

	for i, tt := range tts {
		dt := fieldDesc{OID: tt.typ}
		if kind := dt.Type().Kind(); kind != tt.kind {
			t.Errorf("(%d) got: %s want: %s", i, kind, tt.kind)
		}
	}
}

func TestDataTypeLength(t *testing.T) {
	tts := []struct {
		typ    oid.Oid
		len    int
		mod    int
		length int64
		ok     bool
	}{
		{oid.T_int4, 0, -1, 0, false},
		{oid.T_varchar, 65535, 9, 5, true},
		{oid.T_text, 65535, -1, math.MaxInt64, true},
		{oid.T_bytea, 65535, -1, math.MaxInt64, true},
	}

	for i, tt := range tts {
		dt := fieldDesc{OID: tt.typ, Len: tt.len, Mod: tt.mod}
		if l, k := dt.Length(); k != tt.ok || l != tt.length {
			t.Errorf("(%d) got: %d, %t want: %d, %t", i, l, k, tt.length, tt.ok)
		}
	}
}

func TestDataTypePrecisionScale(t *testing.T) {
	tts := []struct {
		typ              oid.Oid
		mod              int
		precision, scale int64
		ok               bool
	}{
		{oid.T_int4, -1, 0, 0, false},
		{oid.T_numeric, 589830, 9, 2, true},
		{oid.T_text, -1, 0, 0, false},
	}

	for i, tt := range tts {
		dt := fieldDesc{OID: tt.typ, Mod: tt.mod}
		if p, s, k := dt.PrecisionScale(); k != tt.ok || p != tt.precision || s != tt.scale {
			t.Errorf("(%d) got: %d, %d %t want: %d, %d %t", i, p, s, k, tt.precision, tt.scale, tt.ok)
		}
	}
}
