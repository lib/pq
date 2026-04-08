package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq/pqerror"
)

// [pq.Error.Severity] values.
//
// Deprecated: use pqerror.Severity[..] values.
//
//go:fix inline
const (
	Efatal   = pqerror.SeverityFatal
	Epanic   = pqerror.SeverityPanic
	Ewarning = pqerror.SeverityWarning
	Enotice  = pqerror.SeverityNotice
	Edebug   = pqerror.SeverityDebug
	Einfo    = pqerror.SeverityInfo
	Elog     = pqerror.SeverityLog
)

// PGError is an interface used by previous versions of pq.
//
// Deprecated: use the Error type. This is never used.
type PGError interface {
	Error() string
	Fatal() bool
	Get(k byte) (v string)
}

// Get implements the legacy PGError interface.
//
// Deprecated: new code should use the fields of the Error struct directly.
func (e *Error) Get(k byte) (v string) {
	switch k {
	case 'S':
		return e.Severity
	case 'C':
		return string(e.Code)
	case 'M':
		return e.Message
	case 'D':
		return e.Detail
	case 'H':
		return e.Hint
	case 'P':
		return e.Position
	case 'p':
		return e.InternalPosition
	case 'q':
		return e.InternalQuery
	case 'W':
		return e.Where
	case 's':
		return e.Schema
	case 't':
		return e.Table
	case 'c':
		return e.Column
	case 'd':
		return e.DataTypeName
	case 'n':
		return e.Constraint
	case 'F':
		return e.File
	case 'L':
		return e.Line
	case 'R':
		return e.Routine
	}
	return ""
}

// ParseURL converts a url to a connection string for driver.Open.
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }

// NullTime represents a [time.Time] that may be null.
//
// Deprecated: this is an alias for [sql.NullTime].
//
//go:fix inline
type NullTime = sql.NullTime

// CopyIn creates a COPY FROM statement which can be prepared with Tx.Prepare().
// The target table should be visible in search_path.
//
// It copies all columns if the list of columns is empty.
//
// Deprecated: there is no need to use this query builder, you can use:
//
//	tx.Prepare("copy tbl (col1, col2) from stdin")
func CopyIn(table string, columns ...string) string {
	b := bytes.NewBufferString("COPY ")
	BufferQuoteIdentifier(table, b)
	makeStmt(b, columns...)
	return b.String()
}

// CopyInSchema creates a COPY FROM statement which can be prepared with
// Tx.Prepare().
//
// Deprecated: there is no need to use this query builder, you can use:
//
//	tx.Prepare("copy schema.tbl (col1, col2) from stdin")
func CopyInSchema(schema, table string, columns ...string) string {
	b := bytes.NewBufferString("COPY ")
	BufferQuoteIdentifier(schema, b)
	b.WriteRune('.')
	BufferQuoteIdentifier(table, b)
	makeStmt(b, columns...)
	return b.String()
}

func makeStmt(b *bytes.Buffer, columns ...string) {
	if len(columns) == 0 {
		b.WriteString(" FROM STDIN")
		return
	}
	b.WriteString(" (")
	for i, col := range columns {
		if i != 0 {
			b.WriteString(", ")
		}
		BufferQuoteIdentifier(col, b)
	}
	b.WriteString(") FROM STDIN")
}

// ArrayDelimiter may be optionally implemented to override the array delimiter.
//
// Deprecated: this doesn't need to be exported.
type ArrayDelimiter interface{ ArrayDelimiter() string }

// Array returns the optimal driver.Valuer and sql.Scanner for an array or
// slice of any dimension.
//
// For example:
//
//	db.Query(`SELECT * FROM t WHERE id = ANY($1)`, pq.Array([]int{235, 401}))
//
//	var x []sql.NullInt64
//	db.QueryRow(`SELECT ARRAY[235, 401]`).Scan(pq.Array(&x))
//
// Scanning multi-dimensional arrays is not supported.  Arrays where the lower
// bound is not one (such as `[0:0]={1}') are not supported.
//
// Deprecated: use ArrayOf[T]
func Array(a any) interface {
	driver.Valuer
	sql.Scanner
} {
	switch a := a.(type) {
	case []bool:
		return (*ArrayOf[bool])(&a)
	case []float64:
		return (*ArrayOf[float64])(&a)
	case []float32:
		return (*ArrayOf[float32])(&a)
	case []int64:
		return (*ArrayOf[int64])(&a)
	case []int32:
		return (*ArrayOf[int32])(&a)
	case []string:
		return (*ArrayOf[string])(&a)
	case [][]byte:
		return (*ArrayOf[[]byte])(&a)
	case *[]bool:
		return (*BoolArray)(a)
	case *[]float64:
		return (*Float64Array)(a)
	case *[]float32:
		return (*Float32Array)(a)
	case *[]int64:
		return (*Int64Array)(a)
	case *[]int32:
		return (*Int32Array)(a)
	case *[]string:
		return (*StringArray)(a)
	case *[][]byte:
		return (*ByteaArray)(a)
	}
	return GenericArray{a}
}

// BoolArray represents a one-dimensional array of the PostgreSQL boolean type.
//
// Deprecated: use ArrayOf[bool]
//
//go:fix inline
type BoolArray = ArrayOf[bool]

// StringArray represents a one-dimensional array of the PostgreSQL character types.
//
// Deprecated: use ArrayOf[string]
//
//go:fix inline
type StringArray = ArrayOf[string]

// ByteaArray represents a one-dimensional array of the PostgreSQL bytea type.
//
// Deprecated: use ArrayOf[[]byte]
//
//go:fix inline
type ByteaArray = ArrayOf[[]byte]

// Float64Array represents a one-dimensional array of the PostgreSQL double precision type.
//
// Deprecated: use ArrayOf[float32]
//
//go:fix inline
type Float64Array = ArrayOf[float64]

// Float32Array represents a one-dimensional array of the PostgreSQL double precision type.
//
// Deprecated: use ArrayOf[float32]
//
//go:fix inline
type Float32Array = ArrayOf[float32]

// Int64Array represents a one-dimensional array of the PostgreSQL integer type.
//
// Deprecated: use ArrayOf[int64]
//
//go:fix inline
type Int64Array = ArrayOf[int64]

// Int32Array represents a one-dimensional array of the PostgreSQL integer type.
//
// Deprecated: use ArrayOf[int32]
//
//go:fix inline
type Int32Array = ArrayOf[int32]

// GenericArray implements the driver.Valuer and sql.Scanner interfaces for an
// array or slice of any dimension.
//
// Deprecated: use ArrayOf[myType] or ArrayOf[sql.Scanner]
type GenericArray struct{ A any }

var (
	typeByteSlice    = reflect.TypeOf([]byte{})
	typeDriverValuer = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	typeSQLScanner   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

// Value implements the driver.Valuer interface.
func (a GenericArray) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(a.A)
	switch rv.Kind() {
	default:
		return nil, fmt.Errorf("pq: unable to convert %T to array", a.A)
	case reflect.Slice:
		if rv.IsNil() {
			return nil, nil
		}
	case reflect.Array:
		// Do nothing
	}

	l := rv.Len()
	if l == 0 {
		return "{}", nil
	}

	b := make([]byte, 0, 1+2*l)
	b, _, err := appendArray(b, rv, l)
	return string(b), err
}

// Scan implements the sql.Scanner interface.
func (a GenericArray) Scan(src any) error {
	dpv := reflect.ValueOf(a.A)
	switch {
	case dpv.Kind() != reflect.Pointer:
		return fmt.Errorf("pq: destination %T is not a pointer to array or slice", a.A)
	case dpv.IsNil():
		return fmt.Errorf("pq: destination %T is nil", a.A)
	}

	dv := dpv.Elem()
	switch dv.Kind() {
	case reflect.Slice:
	case reflect.Array:
	default:
		return fmt.Errorf("pq: destination %T is not a pointer to array or slice", a.A)
	}

	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src, dv)
	case string:
		return a.scanBytes([]byte(src), dv)
	case nil:
		if dv.Kind() == reflect.Slice {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		}
	}
	return fmt.Errorf("pq: cannot convert %T to %s", src, dv.Type())
}

func (a GenericArray) scanBytes(src []byte, dv reflect.Value) error {
	dtype := dv.Type().Elem()
	dims, elems, err := parseArray(src, arrayDelimiter(reflect.Zero(dtype).Interface()))
	if err != nil {
		return err
	}
	if len(dims) > 1 {
		return fmt.Errorf("pq: scanning from multidimensional ARRAY%s is not implemented",
			strings.Replace(fmt.Sprint(dims), " ", "][", -1))
	}
	// Treat a zero-dimensional array like an array with a single dimension of zero.
	if len(dims) == 0 {
		dims = append(dims, 0)
	}

	for i, rt := 0, dv.Type(); i < len(dims); i, rt = i+1, rt.Elem() {
		switch rt.Kind() {
		case reflect.Slice:
		case reflect.Array:
			if rt.Len() != dims[i] {
				return fmt.Errorf("pq: cannot convert ARRAY%s to %s",
					strings.Replace(fmt.Sprint(dims), " ", "][", -1), dv.Type())
			}
		default:
		}
	}

	assign := func([]byte, reflect.Value) error {
		return fmt.Errorf("pq: scanning to %s is not implemented; only sql.Scanner", dtype)
	}
	if reflect.PointerTo(dtype).Implements(typeSQLScanner) {
		// dest is always addressable because it is an element of a slice.
		assign = func(src []byte, dest reflect.Value) error {
			ss := dest.Addr().Interface().(sql.Scanner)
			if src == nil {
				return ss.Scan(nil)
			}
			return ss.Scan(src)
		}
	}
	values := reflect.MakeSlice(reflect.SliceOf(dtype), len(elems), len(elems))
	for i, e := range elems {
		err := assign(e, values.Index(i))
		if err != nil {
			return fmt.Errorf("pq: parsing array element index %d: %w", i, err)
		}
	}

	switch dv.Kind() {
	case reflect.Slice:
		dv.Set(values.Slice(0, dims[0]))
	case reflect.Array:
		for i := 0; i < dims[0]; i++ {
			dv.Index(i).Set(values.Index(i))
		}
	}
	return nil
}

// appendArray appends rv to the buffer, returning the extended buffer and the
// delimiter used between elements.
//
// Returns an error when n <= 0 or rv is not a reflect.Array or reflect.Slice.
func appendArray(b []byte, rv reflect.Value, n int) ([]byte, string, error) {
	b = append(b, '{')

	b, del, err := appendArrayElement(b, rv.Index(0))
	if err != nil {
		return b, del, err
	}
	for i := 1; i < n; i++ {
		b = append(b, del...)
		b, del, err = appendArrayElement(b, rv.Index(i))
		if err != nil {
			return b, del, err
		}
	}
	return append(b, '}'), del, nil
}

// appendArrayElement appends rv to the buffer, returning the extended buffer
// and the delimiter to use before the next element.
//
// When rv's Kind is neither reflect.Array nor reflect.Slice, it is converted
// using driver.DefaultParameterConverter and the resulting []byte or string is
// double-quoted.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func appendArrayElement(b []byte, rv reflect.Value) ([]byte, string, error) {
	if k := rv.Kind(); k == reflect.Array || k == reflect.Slice {
		if t := rv.Type(); t != typeByteSlice && !t.Implements(typeDriverValuer) {
			if n := rv.Len(); n > 0 {
				return appendArray(b, rv, n)
			}
			return b, "", nil
		}
	}

	iv := rv.Interface()
	del := string(arrayDelimiter(iv))
	iv, err := driver.DefaultParameterConverter.ConvertValue(iv)
	if err != nil {
		return b, del, err
	}

	switch v := iv.(type) {
	case nil:
		return append(b, "NULL"...), del, nil
	case []byte:
		return appendArrayQuotedText(b, v), del, nil
	case string:
		return appendArrayQuotedText(b, []byte(v)), del, nil
	}

	enc, err := encode(iv, 0)
	if err != nil {
		return b, del, err
	}
	return append(b, enc...), del, err
}
