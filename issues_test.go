package pq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIssue494(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	query := `CREATE TEMP TABLE t (i INT PRIMARY KEY)`
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Prepare(CopyIn("t", "i")); err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT 1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestIssue1046(t *testing.T) {
	ctxTimeout := time.Second * 2

	db := openTestConn(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, `SELECT pg_sleep(10) AS id`)
	if err != nil {
		t.Fatal(err)
	}

	var d []uint8
	err = stmt.QueryRowContext(ctx).Scan(&d)
	dl, _ := ctx.Deadline()
	since := time.Since(dl)
	if since > ctxTimeout {
		t.Logf("FAIL %s: query returned after context deadline: %v\n", t.Name(), since)
		t.Fail()
	}
	if pgErr := (*Error)(nil); !(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) {
		t.Logf("ctx.Err(): [%T]%+v\n", ctx.Err(), ctx.Err())
		t.Logf("got err: [%T] %+v expected errCode: %v", err, err, cancelErrorCode)
		t.Fail()
	}
}

func TestIssue1062(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	// Ensure that cancelling a QueryRowContext does not result in an ErrBadConn.

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		row := db.QueryRowContext(ctx, "select 1")

		var v int
		err := row.Scan(&v)
		if pgErr := (*Error)(nil); err != nil &&
			err != context.Canceled &&
			!(errors.As(err, &pgErr) && pgErr.Code == cancelErrorCode) {
			t.Fatalf("Scan resulted in unexpected error %v for canceled QueryRowContext at attempt %d", err, i+1)
		}
	}
}
