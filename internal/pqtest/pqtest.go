package pqtest

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
)

func Pgbouncer() bool { return os.Getenv("PGPORT") == "6432" }
func Pgpool() bool    { return os.Getenv("PGPORT") == "7432" }

func SkipPgbouncer(t testing.TB) {
	t.Helper()
	if Pgbouncer() {
		t.Skip("skipped for pgbouncer (PGPORT=6432)")
	}
}

func SkipPgpool(t testing.TB) {
	t.Helper()
	if Pgpool() {
		t.Skip("skipped for pgpool (PGPORT=7432)")
	}
}

func ForceBinaryParameters() bool {
	v, ok := os.LookupEnv("PQTEST_BINARY_PARAMETERS")
	if !ok {
		return false
	}
	switch strings.ToLower(v) {
	case "1", "yes", "true":
		return true
	case "0", "no", "false":
		return false
	default:
		panic("unexpected value for PQTEST_BINARY_PARAMETERS")
	}
}

var envOnce sync.Once

func DSN(conninfo string) string {
	envOnce.Do(func() {
		defaultTo := func(k string, v string) {
			if _, ok := os.LookupEnv(k); !ok {
				os.Setenv(k, v)
			}
		}
		defaultTo("PGHOST", "localhost")
		defaultTo("PGDATABASE", "pqgo")
		defaultTo("PGUSER", "pqgo")
		defaultTo("PGSSLMODE", "disable")
		defaultTo("PGCONNECT_TIMEOUT", "20")
	})

	if ForceBinaryParameters() &&
		!strings.HasPrefix(conninfo, "postgres://") &&
		!strings.HasPrefix(conninfo, "postgresql://") {
		conninfo += " binary_parameters=yes"
	}
	return conninfo
}

// DB connects to the test database. The caller must call db.Close().
func DB(conninfo ...string) (*sql.DB, error) {
	return sql.Open("postgres", DSN(strings.Join(conninfo, " ")))
}

// MustDB connects to the test database, calling t.Fatal() if this fails. The
// connection is closed in t.Cleanup().
func MustDB(t testing.TB, conninfo ...string) *sql.DB {
	t.Helper()
	db, err := DB(conninfo...)
	if err != nil {
		t.Fatalf("pqtest.MustDB: %s", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Begin a new transaction, calling t.Fatal() if this fails.
func Begin(t testing.TB, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("pqtest.Begin: %s", err)
	}
	// We can't call t.Cleanup here as that will race with the t.Cleanup from
	// MustDB (it's called in "last added, first called", so the tx.Rollback
	// gets called after db.Close)
	// t.Cleanup(func() { tx.Rollback() })
	return tx
}

// Prepare a new statement, calling t.Fatal() if this fails.
func Prepare(t testing.TB, db interface {
	Prepare(string) (*sql.Stmt, error)
}, q string) *sql.Stmt {
	t.Helper()
	stmt, err := db.Prepare(q)
	if err != nil {
		t.Fatalf("pqtest.Prepare: %s", err)
	}
	return stmt
}

// Exec calls db.Exec(), calling t.Fatal if this fails.
func Exec(t testing.TB, db interface {
	Exec(string, ...any) (sql.Result, error)
}, q string, args ...any) {
	t.Helper()
	_, err := db.Exec(q, args...)
	if err != nil {
		t.Fatalf("pqtest.Exec: %s", err)
	}
}

// Query calls db.Query(), calling t.Fatal if this fails.
//
// The resulting rows are scanned to the type T.
func Query[T any](t testing.TB, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any) []map[string]T {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}

	res := make([]map[string]T, 0, 16)
	for rows.Next() {
		if rows.Err() != nil {
			t.Fatalf("pqtest.Query: %s", rows.Err())
		}

		var (
			vals = make([]T, len(cols))
			ptrs = make([]any, len(cols))
		)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		err := rows.Scan(ptrs...)
		if err != nil {
			t.Fatalf("pqtest.Query: %s", err)
		}

		r := map[string]T{}
		for i, v := range vals {
			r[cols[i]] = v
		}
		res = append(res, r)
	}
	return res
}
