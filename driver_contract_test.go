package pq

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

type FuncConn struct {
	prepare func(string) (driver.Stmt, error)
	exec    func(string, []driver.Value) (driver.Result, error)
	query   func(string, []driver.Value) (driver.Rows, error)
}

// Open implements the driver.Driver interface.
func (c *FuncConn) Open(string) (driver.Conn, error) { return c, nil }

// Close implements the driver.Conn interface.
func (*FuncConn) Close() error { return nil }

// Begin implements the driver.Conn interface.
func (c *FuncConn) Begin() (driver.Tx, error) { return c, nil }

// Commit implements the driver.Tx interface.
func (*FuncConn) Commit() error { return nil }

// Rollback implements the driver.Tx interface.
func (*FuncConn) Rollback() error { return nil }

// Prepare implements the driver.Conn interface.
func (c *FuncConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Exec implements the driver.Execer interface.
func (c *FuncConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(query, args)
}

// Query implements the driver.Queryer interface.
func (c *FuncConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(query, args)
}

type FuncStmt struct {
	input func() int
	exec  func([]driver.Value) (driver.Result, error)
	query func([]driver.Value) (driver.Rows, error)
}

// Close implements the driver.Stmt interface.
func (*FuncStmt) Close() error { return nil }

// NumImput implements the driver.Stmt interface.
func (s *FuncStmt) NumInput() int { return s.input() }

// Exec implements the driver.Stmt interface.
func (s *FuncStmt) Exec(args []driver.Value) (driver.Result, error) { return s.exec(args) }

// Query implements the driver.Stmt interface.
func (s *FuncStmt) Query(args []driver.Value) (driver.Rows, error) { return s.query(args) }

type FuncValuer func() (driver.Value, error)

// Value implements the driver.Valuer interface.
func (v FuncValuer) Value() (driver.Value, error) { return v() }

// We expect the sql package to call Exec and Query with only the types
// described in driver.Value
func TestDriverExecQueryArguments(t *testing.T) {
	conn := new(FuncConn)
	sql.Register("TestDriverExecQueryArguments", conn)
	db, _ := sql.Open("TestDriverExecQueryArguments", "")

	valid := []struct{ input, expected driver.Value }{
		{nil, nil},
		{false, false},
		{"", ""},
		{[]byte{}, []byte{}},
		{time.Unix(0, 0), time.Unix(0, 0)},

		{[]byte(nil), []byte(nil)},

		{0, int64(0)},
		{int8(0), int64(0)},
		{int16(0), int64(0)},
		{int32(0), int64(0)},
		{int64(0), int64(0)},
		{uint8(0), int64(0)},
		{uint16(0), int64(0)},
		{uint32(0), int64(0)},
		{uint64(0), int64(0)},

		{byte(0), int64(0)},
		{rune(0), int64(0)},

		{0.0, float64(0)},
		{float32(0), float64(0)},
		{float64(0), float64(0)},

		{FuncValuer(func() (driver.Value, error) { return nil, nil }), nil},
		{FuncValuer(func() (driver.Value, error) { return false, nil }), false},
		{FuncValuer(func() (driver.Value, error) { return "", nil }), ""},
		{FuncValuer(func() (driver.Value, error) { return []byte{}, nil }), []byte{}},
		{FuncValuer(func() (driver.Value, error) { return time.Unix(0, 0), nil }), time.Unix(0, 0)},
		{FuncValuer(func() (driver.Value, error) { return int64(0), nil }), int64(0)},
		{FuncValuer(func() (driver.Value, error) { return float64(0), nil }), float64(0)},
	}

	for _, tt := range valid {
		var exec int
		var query int
		conn.exec = func(_ string, args []driver.Value) (driver.Result, error) {
			exec++
			if !reflect.DeepEqual(tt.expected, args[0]) {
				t.Errorf("Expected %T:%#v, got %T:%#v", tt.expected, tt.expected, args[0], args[0])
			}
			return nil, nil
		}
		conn.query = func(_ string, args []driver.Value) (driver.Rows, error) {
			query++
			if !reflect.DeepEqual(tt.expected, args[0]) {
				t.Errorf("Expected %T:%#v, got %T:%#v", tt.expected, tt.expected, args[0], args[0])
			}
			return nil, nil
		}

		db.Exec("", tt.input)
		db.Query("", tt.input)

		if exec != 1 {
			t.Errorf("Expected Exec to be called once for %T, got %v", tt.input, exec)
		}
		if query != 1 {
			t.Errorf("Expected Query to be called once for %T, got %v", tt.input, query)
		}
	}

	for _, tt := range valid {
		var exec int
		var query int
		conn.prepare = func(string) (driver.Stmt, error) {
			return &FuncStmt{
				input: func() int { return 1 },
				exec: func(args []driver.Value) (driver.Result, error) {
					exec++
					if !reflect.DeepEqual(tt.expected, args[0]) {
						t.Errorf("Expected %T:%#v, got %T:%#v", tt.expected, tt.expected, args[0], args[0])
					}
					return nil, nil
				},
				query: func(args []driver.Value) (driver.Rows, error) {
					query++
					if !reflect.DeepEqual(tt.expected, args[0]) {
						t.Errorf("Expected %T:%#v, got %T:%#v", tt.expected, tt.expected, args[0], args[0])
					}
					return nil, nil
				},
			}, nil
		}

		stmt, _ := db.Prepare("")
		stmt.Exec(tt.input)
		stmt.Query(tt.input)

		if exec != 1 {
			t.Errorf("Expected Exec to be called once for %T, got %v", tt.input, exec)
		}
		if query != 1 {
			t.Errorf("Expected Query to be called once for %T, got %v", tt.input, query)
		}
	}

	invalid := []driver.Value{
		[1]byte{},
		complex64(0),
		complex128(0),
		struct{}{},
		func() {},
		map[string]string{},
		make(chan bool),

		sql.RawBytes(nil),
		sql.RawBytes{},

		FuncValuer(func() (driver.Value, error) { return [1]byte{}, nil }),
		FuncValuer(func() (driver.Value, error) { return complex64(0), nil }),
		FuncValuer(func() (driver.Value, error) { return complex128(0), nil }),
		FuncValuer(func() (driver.Value, error) { return struct{}{}, nil }),
		FuncValuer(func() (driver.Value, error) { return func() {}, nil }),
		FuncValuer(func() (driver.Value, error) { return map[string]string{}, nil }),
		FuncValuer(func() (driver.Value, error) { return make(chan bool), nil }),

		FuncValuer(func() (driver.Value, error) { return int32(0), nil }),
		FuncValuer(func() (driver.Value, error) { return float32(0), nil }),
	}

	for _, tt := range invalid {
		conn.exec = func(_ string, args []driver.Value) (driver.Result, error) {
			t.Errorf("Expected Exec to not be called, got %T:%#v", args[0], args[0])
			return nil, nil
		}
		conn.query = func(_ string, args []driver.Value) (driver.Rows, error) {
			t.Errorf("Expected Query to not be called, got %T:%#v", args[0], args[0])
			return nil, nil
		}

		if _, err := db.Exec("", tt); err == nil {
			t.Errorf("Expected Exec to return an error for %T", tt)
		}
		if _, err := db.Query("", tt); err == nil {
			t.Errorf("Expected Query to return an error for %T", tt)
		}
	}

	for _, tt := range invalid {
		conn.prepare = func(string) (driver.Stmt, error) {
			return &FuncStmt{
				input: func() int { return 1 },
				exec: func(args []driver.Value) (driver.Result, error) {
					t.Errorf("Expected Exec to not be called, got %T:%#v", args[0], args[0])
					return nil, nil
				},
				query: func(args []driver.Value) (driver.Rows, error) {
					t.Errorf("Expected Query to not be called, got %T:%#v", args[0], args[0])
					return nil, nil
				},
			}, nil
		}

		stmt, _ := db.Prepare("")

		if _, err := stmt.Exec(tt); err == nil {
			t.Errorf("Expected Exec to return an error for %T", tt)
		}
		if _, err := stmt.Query(tt); err == nil {
			t.Errorf("Expected Query to return an error for %T", tt)
		}
	}
}
