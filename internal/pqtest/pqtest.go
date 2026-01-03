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
