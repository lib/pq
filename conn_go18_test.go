// +build go1.8

package pq

import (
	"reflect"
	"testing"

	"github.com/lib/pq/oid"
)

func TestColumnTypeScanType(t *testing.T) {
	testdata := []struct {
		colTyp oid.Oid
		kind   reflect.Kind
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

	for _, test := range testdata {
		rs := &rows{
			colTyps: []oid.Oid{test.colTyp},
		}
		if kind := rs.ColumnTypeScanType(0).Kind(); kind != test.kind {
			t.Errorf("got: %s want: %s", kind, test.kind)
		}
	}
}
