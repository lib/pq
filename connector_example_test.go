// +build go1.10

package pq_test

import (
	"database/sql"
	"flag"
	"log"

	"github.com/lib/pq"
)

func ExampleNewConnector() {
	dsn := flag.String("dsn", "postgres://", "connection data source name")
	c, err := pq.NewConnector(*dsn)
	if err != nil {
		log.Fatalf("could not create connector: %v", err)
	}
	db := sql.OpenDB(c)
	defer db.Close()

	// Use the DB
	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("could not start transaction: %v", err)
	}
	txn.Rollback()
}
