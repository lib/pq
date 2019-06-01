//go:build go1.10

package pq_test

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

func ExampleNewConnector() {
	c, err := pq.NewConnector("postgres://")
	if err != nil {
		log.Fatalf("could not create connector: %v", err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	// Use the DB
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("could not start transaction: %v", err)
	}
	tx.Rollback()
}
