package pqtest_test

import (
	"testing"

	_ "github.com/lib/pq"
	"github.com/lib/pq/internal/pqtest"
)

// Just calling db.Close() isn't enough, as it waits for queries to finish, and
// it won't kill the BEGIN; etc.
func TestCleanup(t *testing.T) {
	t.Run("", func(t *testing.T) {
		t.Setenv("PGAPPNAME", "pqgo-cleanup")
		db := pqtest.MustDB(t)
		pqtest.Begin(t, db)
		pqtest.Query[int](t, db, `select 1`)
		stmt := pqtest.Prepare(t, db, `select 1`)

		// No helper function for these as they're not used that frequently, and
		// for stmt.Query() also difficult to do right.
		rows1, _ := db.Query(`select 1`)
		defer rows1.Close()
		rows2, _ := stmt.Query()
		defer rows2.Close()
	})

	rows := pqtest.Query[any](t, pqtest.MustDB(t),
		`select pid, query from pg_stat_activity where application_name = 'pqgo-cleanup' and pid != pg_backend_pid()`)
	for _, r := range rows {
		t.Errorf("connection still active: pid=%d; query=%q\n", r["pid"], r["query"])
	}
}
