package pq_test

import (
	"database/sql"
	"fmt"
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

func ExampleConnectorWithNoticeHandler() {
	// Base connector to wrap
	base, err := pq.NewConnector("postgres://")
	if err != nil {
		log.Fatal(err)
	}

	// Wrap the connector to simply print out the message
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		fmt.Println("Notice sent: " + notice.Message)
	})
	db := sql.OpenDB(connector)
	defer db.Close()

	// Raise a notice
	sql := "DO language plpgsql $$ BEGIN RAISE NOTICE 'test notice'; END $$"
	if _, err := db.Exec(sql); err != nil {
		log.Fatal(err)
	}

	// Output:
	// Notice sent: test notice
}
