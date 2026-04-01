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
	pqtest.DSN("") // Called for the side-effect of setting the environment.
	e := m.Run()
	if e > 0 {
		os.Exit(e)
	}

	for _, f := range os.Args {
		if strings.HasPrefix(f, "-test.fuzz") {
			return
		}
	}
	if debugProto { // It's just confusing/annoying when running with PQGO_DEBUG=1
		return
	}

	fatal := func(msg any) {
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", pqtest.DSN(""))
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`select pid, query from pg_stat_activity where application_name='pqgo' and pid != pg_backend_pid()`)
	if err != nil {
		fatal(err)
	}
	defer rows.Close()

	e = 0
	for rows.Next() {
		var (
			pid   int64
			query string
		)
		err := rows.Scan(&pid, &query)
		if err != nil {
			fatal(err)
		}
		e = 1
		fmt.Printf("connection still active: pid=%d; query=%q\n", pid, query)
	}
	if rows.Err() != nil {
		fatal(rows.Err())
	}
	os.Exit(e)
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
