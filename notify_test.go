package pq

import (
	"database/sql"
	"testing"
)

func waitForSingleEvent(db *sql.DB, t *testing.T, doneChan chan error) (string, string) {
	var err error

	defer func() { doneChan <- err }()

	rows, err := db.Query("LISTEN notify_test")

	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()

	doneChan <- nil

	if !rows.Next() {
		t.Fatal("failed")
	}

	var bePid int
	var relname string
	var extra string

	err = rows.Scan(&bePid, &relname, &extra)

	if err != nil {
		t.Fatal(err)
	}

	return relname, extra
}

func TestListen(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	doneChan := make(chan error)

	go func() {
		relname, extra := waitForSingleEvent(db, t, doneChan)

		if relname != "notify_test" {
			t.Fatal("wrong relname", relname)
		}

		if extra != "" {
			t.Fatal("wrong extra", extra)
		}
	}()

	// Wait for LISTEN to happen.
	<-doneChan

	_, err := db.Exec("NOTIFY notify_test")

	if err != nil {
		t.Fatal(err)
	}

	<-doneChan
}

func TestNotifyExtra(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	doneChan := make(chan error)

	go func() {
		relname, extra := waitForSingleEvent(db, t, doneChan)

		if relname != "notify_test" {
			t.Fatal("wrong relname", relname)
		}

		if extra != "something" {
			t.Fatal("wrong extra", extra)
		}
	}()

	// Wait for LISTEN to happen.
	<-doneChan

	_, err := db.Exec("NOTIFY notify_test, 'something'")

	if err != nil {
		t.Fatal(err)
	}

	<-doneChan
}
