package pq

import (
	"database/sql"
	"testing"
)

func mustExec(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("Failed to execute [%s]: %s", query, err)
	}
}

func mustScan(t *testing.T, rs *sql.Rows, dest ...interface{}) {
	if err := rs.Scan(dest...); err != nil {
		t.Fatalf("Failure scanning: ", err)
	}
}

func TestListen(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	// start listening
	notes, err := db.Query("LISTEN channel")
	if err != nil {
		t.Fatalf("Failed to prepare LISTEN: ", err)
	}
	defer notes.Close()

	// make sure a plain NOTIFY with no payload works
	mustExec(t, db, "NOTIFY channel")

	payload := "bogus payload"
	if !notes.Next() {
		t.Errorf("Did not receive NOTIFY")
	}
	mustScan(t, notes, &payload)
	if payload != "" {
		t.Fatalf("Received unexpected payload '%s' (expected '')", payload)
	}

	// we can also pass in a payload, and notifications arrive in order
	mustExec(t, db, "NOTIFY channel, 'the first payload'")
	mustExec(t, db, "NOTIFY channel, 'the second payload'")
	if !notes.Next() {
		t.Fatalf("Did not receive NOTIFY")
	}
	mustScan(t, notes, &payload)
	if payload != "the first payload" {
		t.Fatalf("Received unexpected payload '%s' (expected 'the first payload')", payload)
	}
	if !notes.Next() {
		t.Fatalf("Did not receive NOTIFY")
	}
	mustScan(t, notes, &payload)
	if payload != "the second payload" {
		t.Fatalf("Received unexpected payload '%s' (expected 'the second payload')", payload)
	}
}

func TestUnlisten(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	// start listening on channel1
	notes, err := db.Query("LISTEN channel1")
	if err != nil {
		t.Fatalf("Failed to prepare LISTEN: ", err)
	}

	// return that connection to sql.DB's pool, and unlisten
	if err = notes.Close(); err != nil {
		t.Fatalf("Failure closing LISTEN query: ", err)
	}

	// start a new connection listening to channel 2
	// the underlying DB connection should be the SAME as our previous one,
	// as database.sql pools them
	notes, err = db.Query("LISTEN channel2")
	if err != nil {
		t.Fatalf("Failed to prepare LISTEN: ", err)
	}

	// make sure we receive ONLY the message sent to channel2
	mustExec(t, db, "NOTIFY channel1, 'to 1'")
	mustExec(t, db, "NOTIFY channel2, 'to 2'")

	var payload string
	if !notes.Next() {
		t.Fatalf("Did not receive NOTIFY")
	}
	mustScan(t, notes, &payload)
	if payload != "to 2" {
		t.Fatalf("Received unexpected payload '%s' (expected 'to 2')", payload)
	}
}
