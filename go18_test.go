// +build go1.8

package pq

import (
	"context"
	"testing"
)

func TestMultipleSimpleQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	rows, err := db.Query("select 1; set time zone default; select 2; select 3")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var i int
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expected 1, got %d", i)
		}
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expected 2, got %d", i)
		}
	}

	// Make sure that if we ignore a result we can still query.

	rows, err = db.Query("select 4; select 5")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 4 {
			t.Fatalf("expected 4, got %d", i)
		}
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 5 {
			t.Fatalf("expected 5, got %d", i)
		}
	}
	if rows.NextResultSet() {
		t.Fatal("unexpected result set")
	}
}

func TestContextCancel(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Delay execution for just a bit until db.ExecContext has begun.
	go cancel()

	// Not canceled until after the exec has started.
	if _, err := db.ExecContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "pq: canceling statement due to user request" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Context is already canceled, so error should come before execution.
	if _, err := db.QueryContext(ctx, "select pg_sleep(1)"); err == nil {
		t.Fatal("expected error")
	} else if err.Error() != "context canceled" {
		t.Fatalf("unexpected error: %s", err)
	}
}
