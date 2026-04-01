package pqtest

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/lib/pq/internal/pqutil"
)

func Pgbouncer() bool { return os.Getenv("PGPORT") == "6432" }
func Pgpool() bool    { return os.Getenv("PGPORT") == "7432" }
func Cockroach() bool { return os.Getenv("PGPORT") == "26257" }
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
func SkipCockroach(t testing.TB) {
	t.Helper()
	if Cockroach() {
		t.Skip("skipped for cockroach (PGPORT=26257)")
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

// InvalidCertificate reports if this error is an "invalid certificate" error.
func InvalidCertificate(err error) bool {
	switch err.(type) {
	case x509.UnknownAuthorityError, x509.HostnameError, *tls.CertificateVerificationError:
		return true
	}
	return false
}

// Unsetenv unsets the environment variable and uses Cleanup to restore the
// value after the test.
//
// Because Setenv affects the whole process, it cannot be used in parallel tests
// or tests with parallel ancestors.
func Unsetenv(t *testing.T, k string) { t.Setenv(k, ""); os.Unsetenv(k) }

// Ptr gets a pointer to any value.
//
// TODO(go1.26): replace with new(..) once pq requires Go 1.26.
func Ptr[T any](t T) *T {
	return &t
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
		os.Setenv("PGAPPNAME", "pqgo")
	})

	if ForceBinaryParameters() &&
		!strings.HasPrefix(conninfo, "postgres://") &&
		!strings.HasPrefix(conninfo, "postgresql://") {
		conninfo += " binary_parameters=yes"
	}
	return conninfo
}

// Home sets the HOME directory to a temporary directly and makes sure the
// .postgresql directory exists.
func Home(t *testing.T) string {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(pqutil.Home(true), 0o777); err != nil {
		t.Fatal(err)
	}
	return pqutil.Home(true)
}

var (
	mu       sync.Mutex
	cleanups = make(map[*sql.DB][]func())
)

// DB connects to the test database and returns the Ping error. The connection
// is closed in t.Cleanup().
func DB(t testing.TB, conninfo ...string) (*sql.DB, error) {
	t.Helper()
	db, err := sql.Open("postgres", DSN(strings.Join(conninfo, " ")))
	if err != nil {
		t.Fatalf("pqtest.DB: %s", err)
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for i := len(cleanups[db]) - 1; i >= 0; i-- {
			cleanups[db][i]()
		}
		delete(cleanups, db)
		db.Close()
	})
	return db, db.Ping()
}

// MustDB connects and pings the test database, calling t.Fatal() if this fails.
// The connection is closed in t.Cleanup().
func MustDB(t testing.TB, conninfo ...string) *sql.DB {
	t.Helper()
	db, err := DB(t, conninfo...)
	if err != nil {
		t.Fatalf("pqtest.MustDB: %s", err)
	}
	return db
}

// Begin a new transaction, calling t.Fatal() if this fails.
func Begin(t testing.TB, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("pqtest.Begin: %s", err)
	}
	mu.Lock()
	defer mu.Unlock()
	cleanups[db] = append(cleanups[db], func() { tx.Rollback() })
	return tx
}

type Stmt struct{ *sql.Stmt }

// MustExec calls Stmt.Exec(), calling t.Fatal if this fails.
func (s *Stmt) MustExec(t testing.TB, args ...any) sql.Result {
	t.Helper()
	res, err := s.Stmt.Exec(args...)
	if err != nil {
		t.Fatalf("pqtest.Stmt.MustExec: %s", err)
	}
	return res
}

// MustClose calls Stmt.Close(), calling t.Fatal if this fails.
func (s *Stmt) MustClose(t testing.TB) {
	t.Helper()
	err := s.Stmt.Close()
	if err != nil {
		t.Fatalf("pqtest.Stmt.MustClose: %s", err)
	}
}

// Prepare a new statement, calling t.Fatal() if this fails.
func Prepare(t testing.TB, db interface {
	Prepare(string) (*sql.Stmt, error)
}, q string, sqldb ...*sql.DB) *Stmt {
	t.Helper()
	stmt, err := db.Prepare(q)
	if err != nil {
		t.Fatalf("pqtest.Prepare: %s", err)
	}

	if len(sqldb) == 0 {
		sqldb = make([]*sql.DB, 1)
		var ok bool
		sqldb[0], ok = db.(*sql.DB)
		if !ok {
			t.Fatalf("pqtest.Prepare: must pass sql.DB when using transaction")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	cleanups[sqldb[0]] = append(cleanups[sqldb[0]], func() { stmt.Close() })

	return &Stmt{stmt}
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
// The resulting rows are scanned to the type T. All result sets are scanned;
// columns for additional result sets are prefixed with (rs %d), starting at 1.
func Query[T any](t testing.TB, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any) []map[string]T {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}

	res := make([]map[string]T, 0, 16)
	readRows := func(rs int) {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("pqtest.Query: %s", err)
		}
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
				k := cols[i]
				if rs > 0 {
					k = fmt.Sprintf("(rs %d) %s", rs, k)
				}
				r[k] = v
			}
			res = append(res, r)
		}
	}
	readRows(0)
	for i := 1; rows.NextResultSet(); i++ {
		readRows(i)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}
	return res
}

// QueryRow scans exactly one row, calling t.Fatal if this fails.
//
// The resulting row is scanned to the type T.
func QueryRow[T any](t testing.TB, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any) map[string]T {
	t.Helper()
	rows := Query[T](t, db, q, args...)
	switch len(rows) {
	case 0:
		// Do nothing
	case 1:
		return rows[0]
	default:
		t.Fatalf("pqtest.QueryRow: query returned %d rows", len(rows))
	}
	return nil
}

// Null represents a value that may be null.
//
// TODO(go1.22): replace with sql.Null
type Null[T any] struct {
	V     T
	Valid bool
}
