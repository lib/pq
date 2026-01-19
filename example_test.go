package pq_test

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/lib/pq"
)

func ExampleNewConnector() {
	c, err := pq.NewConnector("host=postgres dbname=pqgo")
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
	// Output:
}

func ExampleNewConfig() {
	cfg, err := pq.NewConfig("host=postgres dbname=pqgo")
	if err != nil {
		log.Fatal(err)
	}
	if cfg.Host == "localhost" {
		cfg.Host = "127.0.0.1"
	}

	c, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	// Use the DB
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("could not start transaction: %v", err)
	}
	tx.Rollback()
	// Output:
}

func ExampleConnectorWithNoticeHandler() {
	// Base connector to wrap
	dsn := ""
	base, err := pq.NewConnector(dsn)
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

func ExampleRegisterTLSConfig() {
	pem, err := os.ReadFile("testdata/init/root.crt")
	if err != nil {
		log.Fatal(err)
	}

	root := x509.NewCertPool()
	root.AppendCertsFromPEM(pem)

	certs, err := tls.LoadX509KeyPair("testdata/init/postgresql.crt", "testdata/init/postgresql.key")
	if err != nil {
		log.Fatal(err)
	}

	pq.RegisterTLSConfig("mytls", &tls.Config{
		RootCAs:      root,
		Certificates: []tls.Certificate{certs},
		ServerName:   "postgres",
	})

	db, err := sql.Open("postgres", "host=postgres dbname=pqgo sslmode=pqgo-mytls")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// Output:
}
