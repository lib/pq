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

func DB(conninfo string) (*sql.DB, error) {
	return sql.Open("postgres", DSN(conninfo))
}

func MustDB(t testing.TB) *sql.DB {
	t.Helper()
	conn, err := DB("")
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func Exec(t testing.TB, db interface {
	Exec(string, ...any) (sql.Result, error)
}, q string, args ...any) {
	t.Helper()
	_, err := db.Exec(q, args...)
	if err != nil {
		t.Fatalf("pqtest.Exec: %s", err)
	}
}

func Query(t testing.TB, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any) []map[string]any {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("pqtest.Query: %s", err)
	}

	res := make([]map[string]any, 0, 16)
	for rows.Next() {
		if rows.Err() != nil {
			t.Fatalf("pqtest.Query: %s", rows.Err())
		}

		var (
			vals = make([]any, len(cols))
			ptrs = make([]any, len(cols))
		)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		err := rows.Scan(ptrs...)
		if err != nil {
			t.Fatalf("pqtest.Query: %s", err)
		}

		r := map[string]any{}
		for i, v := range vals {
			r[cols[i]] = v
		}
		res = append(res, r)
	}
	return res
}
