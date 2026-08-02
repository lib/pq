package pq

import (
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/pqerror"
)

func TestMain(m *testing.M) {
	// Allocation benchmarks are entirely in-memory. Keep their measurements
	// independent of Docker and a live PostgreSQL process when explicitly
	// requested by the benchmark runner.
	if pqtest.PureBenchmark() {
		os.Exit(m.Run())
	}
	if err := pqtest.Setup(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	e := m.Run()

	fuzzing := false
	for _, f := range os.Args {
		if strings.HasPrefix(f, "-test.fuzz") {
			fuzzing = true
			break
		}
	}
	if e == 0 && !fuzzing && !debugProto {
		if err := checkLeakedConnections(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			e = 1
		}
	}

	if err := pqtest.Teardown(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if e == 0 {
			e = 1
		}
	}
	os.Exit(e)
}

func checkLeakedConnections() error {
	db, err := sql.Open("postgres", pqtest.DSN(""))
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(`select pid, query from pg_stat_activity where application_name='pqgo' and pid != pg_backend_pid()`)
	if err != nil {
		return err
	}
	defer rows.Close()

	leaked := false
	for rows.Next() {
		var (
			pid   int64
			query string
		)
		err := rows.Scan(&pid, &query)
		if err != nil {
			return err
		}
		leaked = true
		fmt.Printf("connection still active: pid=%d; query=%q\n", pid, query)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if leaked {
		return fmt.Errorf("test connections still active")
	}
	return nil
}

// mustAs calls As(), calling t.Fatal() if the error is nil or if this fails.
//
// This should probably be in pqtest, but can't right now due to import cycles,
// and using pq_test package requires some refactoring as it refers to
// unexported symbols.
func mustAs(t *testing.T, err error, codes ...pqerror.Code) *Error {
	t.Helper()
	pqErr := As(err)
	if pqErr == nil {
		t.Fatalf("mustAs: not *pq.Error: %T", err)
	}
	if len(codes) > 0 && !slices.Contains(codes, pqErr.Code) {
		t.Fatalf("mustAs: wrong error %q (code not one of %s)", pqErr.Error(), codes)
	}
	return pqErr
}
