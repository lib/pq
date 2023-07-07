//go:build go1.10
// +build go1.10

package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"testing"
)

func TestNewConnector_WorksWithOpenDB(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Connect(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Driver(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Driver().Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Environ(t *testing.T) {
	name := ""
	os.Setenv("PGPASSFILE", "/tmp/.pgpass")
	defer os.Unsetenv("PGPASSFILE")
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	for key, expected := range map[string]string{
		"passfile": "/tmp/.pgpass",
	} {
		if got := c.opts[key]; got != expected {
			t.Fatalf("Getting values from environment variables, for %v expected %s got %s", key, expected, got)
		}
	}

}
