package pq

import (
	"math"
	"reflect"
	"testing"

	"github.com/lib/pq/internal/pqtest"
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
		{oid.T_bit, "BIT"},
		{oid.T_varbit, "VARBIT"},
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
		{oid.T_bit, reflect.String},
		{oid.T_varbit, reflect.String},
		{oid.T_bool, reflect.Bool},
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
		{oid.T_bit, 0, 10, 10, true},
		{oid.T_varbit, 0, 10, 10, true},
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
		p, s, k := dt.PrecisionScale()
		if k != tt.ok {
			t.Errorf("(%d) got: %t want: %t", i, k, tt.ok)
		}
		if p != tt.precision {
			t.Errorf("(%d) wrong precision got: %d want: %d", i, p, tt.precision)
		}
		if s != tt.scale {
			t.Errorf("(%d) wrong scale got: %d want: %d", i, s, tt.scale)
		}
	}
}

func TestRowsColumnTypes(t *testing.T) {
	type (
		length struct {
			Len int64
			OK  bool
		}
		decimalSize struct {
			Precision int64
			Scale     int64
			OK        bool
		}
	)
	tests := []struct {
		Name        string
		TypeName    string
		Length      length
		DecimalSize decimalSize
		ScanType    reflect.Type
	}{
		{
			Name:        "a",
			TypeName:    "INT4",
			Length:      length{Len: 0, OK: false},
			DecimalSize: decimalSize{Precision: 0, Scale: 0, OK: false},
			ScanType:    reflect.TypeOf(int32(0)),
		},
		{
			Name:        "bar",
			TypeName:    "TEXT",
			Length:      length{Len: math.MaxInt64, OK: true},
			DecimalSize: decimalSize{Precision: 0, Scale: 0, OK: false},
			ScanType:    reflect.TypeOf(""),
		},
		{
			Name:        "dec",
			TypeName:    "NUMERIC",
			Length:      length{Len: 0, OK: false},
			DecimalSize: decimalSize{Precision: 9, Scale: 2, OK: true},
			ScanType:    reflect.TypeOf(new(any)).Elem(),
		},
		{
			Name:        "f",
			TypeName:    "FLOAT8",
			Length:      length{Len: 0, OK: false},
			DecimalSize: decimalSize{Precision: 0, Scale: 0, OK: false},
			ScanType:    reflect.TypeOf(float64(0)),
		},
		{
			Name:        "bit4",
			TypeName:    "BIT",
			Length:      length{Len: 4, OK: true},
			DecimalSize: decimalSize{Precision: 0, Scale: 0, OK: false},
			ScanType:    reflect.TypeOf(""),
		},
		{
			Name:        "varbit10",
			TypeName:    "VARBIT",
			Length:      length{Len: 10, OK: true},
			DecimalSize: decimalSize{Precision: 0, Scale: 0, OK: false},
			ScanType:    reflect.TypeOf(""),
		},
	}

	db := pqtest.MustDB(t)

	rows, err := db.Query(`select
		1 as a,
		text 'bar' as bar,
		1.28::numeric(9, 2) as dec,
		3.1415::float8 as f,
		'1111'::bit(4) as bit4,
		'1111'::varbit(10) as varbit10
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 6 {
		t.Errorf("expected 4 columns found %d", len(columns))
	}

	for i, tt := range tests {
		t.Run("", func(t *testing.T) {
			c := columns[i]
			if c.Name() != tt.Name {
				t.Errorf("have: %s, want: %s", c.Name(), tt.Name)
			}
			if c.DatabaseTypeName() != tt.TypeName {
				t.Errorf("have: %s, want: %s", c.DatabaseTypeName(), tt.TypeName)
			}
			l, ok := c.Length()
			if l != tt.Length.Len {
				t.Errorf("have: %d, want: %d", l, tt.Length.Len)
			}
			if ok != tt.Length.OK {
				t.Errorf("have: %t, want: %t", ok, tt.Length.OK)
			}
			p, s, ok := c.DecimalSize()
			if p != tt.DecimalSize.Precision {
				t.Errorf("have: %d, want: %d", p, tt.DecimalSize.Precision)
			}
			if s != tt.DecimalSize.Scale {
				t.Errorf("have: %d, want: %d", s, tt.DecimalSize.Scale)
			}
			if ok != tt.DecimalSize.OK {
				t.Errorf("have: %t, want: %t", ok, tt.DecimalSize.OK)
			}
			if c.ScanType() != tt.ScanType {
				t.Errorf("have: %v, want: %v", c.ScanType(), tt.ScanType)
			}
		})
	}
}

func TestRowsClose(t *testing.T) {
	t.Run("CloseBeforeDone", func(t *testing.T) {
		t.Parallel()
		db := pqtest.MustDB(t)

		rows, err := db.Query("select 1")
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		if rows.Next() {
			t.Fatal("unexpected row")
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
	})

	// closing a query early allows a subsequent query to work.
	t.Run("QuickClose", func(t *testing.T) {
		t.Parallel()
		db := pqtest.MustDB(t)

		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query("select 1; select 2;")
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		var id int
		err = tx.QueryRow("select 3").Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		if id != 3 {
			t.Fatalf("unexpected %d", id)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}
