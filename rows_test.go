package pq

import (
	"database/sql"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/oid"
)

func TestDataTypeName(t *testing.T) {
	tests := []struct {
		typ      oid.Oid
		name     string
		redshift bool
	}{
		{oid.T_int8, "INT8", false},
		{oid.T_int4, "INT4", false},
		{oid.T_int2, "INT2", false},
		{oid.T_varchar, "VARCHAR", false},
		{oid.T_text, "TEXT", false},
		{oid.T_bit, "BIT", false},
		{oid.T_varbit, "VARBIT", false},
		{oid.T_bool, "BOOL", false},
		{oid.T_numeric, "NUMERIC", false},
		{oid.T_date, "DATE", false},
		{oid.T_time, "TIME", false},
		{oid.T_timetz, "TIMETZ", false},
		{oid.T_timestamp, "TIMESTAMP", false},
		{oid.T_timestamptz, "TIMESTAMPTZ", false},
		{oid.T_bytea, "BYTEA", false},

		{oid.T_int8, "INT8", true},
		{635, "_SPECTRUM_ARRAY", true},
		{636, "_SPECTRUM_MAP", true},
		{637, "_SPECTRUM_STRUCT", true},
		{4000, "SUPER", true},

		{635, "", false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := &rows{
				cn:         &conn{parameterStatus: parameterStatus{isRedshift: tt.redshift}},
				rowsHeader: rowsHeader{colTyps: []fieldDesc{{OID: tt.typ}}},
			}
			if name := have.ColumnTypeDatabaseTypeName(0); name != tt.name {
				t.Errorf("\nhave: %s\nwant: %s", name, tt.name)
			}
		})
	}
}

func TestDataType(t *testing.T) {
	tests := []struct {
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

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := fieldDesc{OID: tt.typ}
			if kind := have.Type().Kind(); kind != tt.kind {
				t.Errorf("\nhave: %s\nwant: %s", kind, tt.kind)
			}
		})
	}
}

func TestDataTypeLength(t *testing.T) {
	tests := []struct {
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

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := fieldDesc{OID: tt.typ, Len: tt.len, Mod: tt.mod}
			if l, k := have.Length(); k != tt.ok || l != tt.length {
				t.Errorf("\nhave: %d, %t\nwant: %d, %t", l, k, tt.length, tt.ok)
			}
		})
	}
}

func TestDataTypePrecisionScale(t *testing.T) {
	tests := []struct {
		typ              oid.Oid
		mod              int
		precision, scale int64
		ok               bool
	}{
		{oid.T_int4, -1, 0, 0, false},
		{oid.T_numeric, 589830, 9, 2, true},
		{oid.T_text, -1, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			dt := fieldDesc{OID: tt.typ, Mod: tt.mod}
			p, s, k := dt.PrecisionScale()
			if k != tt.ok {
				t.Errorf("\nhave: %t\nwant: %t", k, tt.ok)
			}
			if p != tt.precision {
				t.Errorf("wrong precision\nhave: %d\nwant: %d", p, tt.precision)
			}
			if s != tt.scale {
				t.Errorf("wrong scale\nhave: %d\nwant: %d", s, tt.scale)
			}
		})
	}
}

func TestRowsColumnTypes(t *testing.T) {
	rows, err := pqtest.MustDB(t).Query(`select
		1::int4 as a,
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

	type h struct {
		name        string
		typeName    string
		length      sql.Null[int64]
		decimalSize sql.Null[[2]int64]
		scanType    reflect.Type
	}

	have := make([]h, 0, len(columns))
	for _, c := range columns {
		l, lok := c.Length()
		prec, scale, dok := c.DecimalSize()
		have = append(have, h{
			name:        c.Name(),
			typeName:    c.DatabaseTypeName(),
			scanType:    c.ScanType(),
			length:      sql.Null[int64]{V: l, Valid: lok},
			decimalSize: sql.Null[[2]int64]{V: [2]int64{prec, scale}, Valid: dok},
		})
	}

	want := []h{
		{
			name:     "a",
			typeName: "INT4",
			scanType: reflect.TypeFor[int32](),
		},
		{
			name:     "bar",
			typeName: "TEXT",
			length:   sql.Null[int64]{V: math.MaxInt64, Valid: true},
			scanType: reflect.TypeFor[string](),
		},
		{
			name:        "dec",
			typeName:    "NUMERIC",
			decimalSize: sql.Null[[2]int64]{V: [2]int64{9, 2}, Valid: true},
			scanType:    reflect.TypeFor[any](),
		},
		{
			name:     "f",
			typeName: "FLOAT8",
			scanType: reflect.TypeFor[float64](),
		},
		{
			name:     "bit4",
			typeName: "BIT",
			length:   sql.Null[int64]{V: 4, Valid: true},
			scanType: reflect.TypeFor[string](),
		},
		{
			name:     "varbit10",
			typeName: "VARBIT",
			length:   sql.Null[int64]{V: 10, Valid: true},
			scanType: reflect.TypeFor[string](),
		},
	}

	if len(have) != len(want) {
		t.Fatalf("wrong column length\nhave: %d\nwant: %d", len(have), len(want))
	}

	for i := range have {
		if !reflect.DeepEqual(have[i], want[i]) {
			t.Errorf("column %d wrong\nhave: %#v\nwant: %#v", i, have[i], want[i])
		}
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
		tx := pqtest.Begin(t, pqtest.MustDB(t))

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

func TestRowsConcurrentUse(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t, "")

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx := pqtest.Begin(t, db)
			defer tx.Rollback()

			rows, err := tx.Query(`select unnest('{1,2,3}'::int[])`)
			if err != nil {
				t.Error(err)
				return
			}

			all := make([]int, 0, 3)
			for rows.Next() {
				var n int
				err := rows.Scan(&n)
				if err != nil {
					t.Error(err)
					return
				}
				all = append(all, n)

				_, err = tx.Query("select 99")
				if !errors.Is(err, errQueryInProgress) {
					t.Errorf("wrong error for query: %v", err)
				}
				_, err = tx.Exec("select pg_sleep(0.01)")
				if !errors.Is(err, errQueryInProgress) {
					t.Errorf("wrong error for exec: %v", err)
				}
			}
			if !reflect.DeepEqual(all, []int{1, 2, 3}) {
				t.Error(all)
			}

			var n int
			err = tx.QueryRow("select 42").Scan(&n)
			if err != nil {
				t.Error(err)
				return
			}
			if n != 42 {
				t.Error(n)
			}
			_, err = tx.Exec("select pg_sleep(0.01)")
			if err != nil {
				t.Error(err)
				return
			}

			// Calling Close() early rows
			rows, err = tx.Query(`select unnest('{1,2,3}'::int[])`)
			if err != nil {
				t.Error(err)
				return
			}
			rows.Close()
			err = tx.QueryRow("select 43").Scan(&n)
			if err != nil {
				t.Error(err)
			}
			if n != 43 {
				t.Error(n)
			}
		}()
	}
	wg.Wait()
}
