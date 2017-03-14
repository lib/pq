package pq

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

var replicationSlot = "pqgoreplication_test"

func openTestReplicationConn(t *testing.T) *ReplicationConn {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	r, err := NewReplicationConnection("")
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func openTestConnWithTable(t *testing.T) *sql.DB {
	db := openTestConn(t)
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS repl (a int)")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func closeTestConnWithTable(t *testing.T, db *sql.DB) {
	db.Exec("DROP TABLE repl")
	db.Close()
}

func createTestReplicationSlot(t *testing.T, r *ReplicationConn) string {
	// r.DropReplicationSlot(replicationSlot)
	pos, err := r.CreateLogicalReplicationSlot(replicationSlot, "test_decoding")
	if err != nil {
		t.Fatal(err)
	}

	return pos
}

func dropTestReplicationSlot(t *testing.T, r *ReplicationConn) {
	err := r.DropReplicationSlot(replicationSlot)

	if err != nil {
		t.Fatal(err)
	}
}

func expectReplicationSlotExists(t *testing.T, db *sql.DB, name string) {
	exists := checkSlotExists(db, name)

	if !exists {
		t.Fatal("Expected slot to exists")
	}
}

func expectReplicationSlotNotExists(t *testing.T, db *sql.DB, name string) {
	exists := checkSlotExists(db, name)

	if exists {
		t.Error("Expected slot to not exists")
	}
}

func checkSlotExists(db *sql.DB, name string) bool {
	var slotName string

	err := db.QueryRow(fmt.Sprintf("SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s' ", replicationSlot)).Scan(&slotName)

	return err == nil && slotName == name
}

func readNTestEvents(r *ReplicationConn, n int) ([]*ReplicationEvent, error) {
	doneChan := make(chan struct{})
	replicationEvents := make([]*ReplicationEvent, 0)
	var err error

	go func() {
		for i := 0; i < n; i++ {
			select {
			case event := <-r.EventsChannel():
				replicationEvents = append(replicationEvents, event)
			case <-time.After(100e8):
				close(doneChan)
				return
			}
		}

		close(doneChan)
	}()

	<-doneChan

	return replicationEvents, err
}

func TestReplicationCreateAndDestroyReplicationSlot(t *testing.T) {
	r := openTestReplicationConn(t)
	db := openTestConn(t)

	r.DropReplicationSlot(replicationSlot)
	expectReplicationSlotNotExists(t, db, replicationSlot)

	createTestReplicationSlot(t, r)
	expectReplicationSlotExists(t, db, replicationSlot)

	dropTestReplicationSlot(t, r)
	expectReplicationSlotNotExists(t, db, replicationSlot)

	r.Close()
	db.Close()
}

func TestReplicationStartLogicalStream(t *testing.T) {
	db := openTestConnWithTable(t)
	r := openTestReplicationConn(t)
	r.DropReplicationSlot("repl_test")

	defer func() {
		db.Close()
		r.Close()
		rCleanup := openTestReplicationConn(t)
		rCleanup.DropReplicationSlot("repl_test")
		rCleanup.Close()
	}()

	pos, err := r.CreateLogicalReplicationSlot("repl_test", "test_decoding")

	if err != nil {
		t.Fatal(err)
	}

	r.StartLogicalStream("repl_test", pos, 0, nil)

	db.Exec("INSERT INTO repl VALUES (0)")
	db.Exec("INSERT INTO repl VALUES (1)")
	db.Exec("INSERT INTO repl VALUES (2)")

	events, err := readNTestEvents(r, 9)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(events) != 9 {
		t.Errorf("Expected to receive 9 events")
	}

	if string(events[1].Payload) != "table public.repl: INSERT: a[integer]:0" {
		t.Errorf("Expected payload to be %s got %s", "table public.repl: INSERT: a[integer]:0", string(events[1].Payload))
	}

	if string(events[4].Payload) != "table public.repl: INSERT: a[integer]:1" {
		t.Errorf("Expected payload to be %s got %s", "table public.repl: INSERT: a[integer]:1", string(events[1].Payload))
	}

	if string(events[7].Payload) != "table public.repl: INSERT: a[integer]:2" {
		t.Errorf("Expected payload to be %s got %s", "table public.repl: INSERT: a[integer]:2", string(events[1].Payload))
	}
}

func TestReplicationManualCommit(t *testing.T) {
	db := openTestConnWithTable(t)
	r := openTestReplicationConn(t)
	r.DropReplicationSlot("repl_test")

	defer func() {
		db.Close()
		r.Close()
		rCleanup := openTestReplicationConn(t)
		rCleanup.DropReplicationSlot("repl_test")
		rCleanup.Close()
	}()

	pos, err := r.CreateLogicalReplicationSlot("repl_test", "test_decoding")

	if err != nil {
		t.Fatal(err)
	}

	r.StartLogicalStream("repl_test", pos, -1, nil)

	db.Exec("INSERT INTO repl VALUES (0)")
	db.Exec("INSERT INTO repl VALUES (1)")
	db.Exec("INSERT INTO repl VALUES (2)")

	events, err := readNTestEvents(r, 9)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(events) != 9 {
		t.Errorf("Expected to receive 9 events")
	}

	err = r.MarkFlushLogPos(events[5].LogPos)

	r.Close()

	r = openTestReplicationConn(t)

	r.StartLogicalStream("repl_test", pos, -1, nil)

	events, err = readNTestEvents(r, 3)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("Expected to receive 3 events")
	}

	if string(events[1].Payload) != "table public.repl: INSERT: a[integer]:2" {
		t.Errorf("Expected payload to be %s got %s", "table public.repl: INSERT: a[integer]:2", string(events[1].Payload))
	}
}

func TestReplicationAutocommit(t *testing.T) {
	db := openTestConnWithTable(t)
	r := openTestReplicationConn(t)
	r.DropReplicationSlot("repl_test")

	defer func() {
		db.Close()
		r.Close()
		rCleanup := openTestReplicationConn(t)
		rCleanup.DropReplicationSlot("repl_test")
		rCleanup.Close()
	}()

	pos, err := r.CreateLogicalReplicationSlot("repl_test", "test_decoding")

	if err != nil {
		t.Fatal(err)
	}

	r.StartLogicalStream("repl_test", pos, 0, nil)

	db.Exec("INSERT INTO repl VALUES (0)")
	db.Exec("INSERT INTO repl VALUES (1)")
	db.Exec("INSERT INTO repl VALUES (2)")

	events, err := readNTestEvents(r, 9)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(events) != 9 {
		t.Errorf("Expected to receive 9 events")
	}

	time.Sleep(1e8)

	r.Close()

	r = openTestReplicationConn(t)

	r.StartLogicalStream("repl_test", pos, 0, nil)

	db.Exec("INSERT INTO repl VALUES (3)")

	events, err = readNTestEvents(r, 3)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("Expected to receive 3 events")
	}

	if string(events[1].Payload) != "table public.repl: INSERT: a[integer]:3" {
		t.Errorf("Expected payload to be %s got %s", "table public.repl: INSERT: a[integer]:3", string(events[1].Payload))
	}
}
