package pqerror_test

import (
	"database/sql"
	"log"

	"github.com/lib/pq/pqerror"
)

func ExampleAs() {
	db, err := sql.Open("postgres", "")
	if err != nil {
		log.Fatal(err)
	}

	email := "hello@example.com"

	_, err = db.Exec("insert into t (email) values ($1)", email)
	if pqErr := pqerror.As(err, pqerror.UniqueViolation); pqErr != nil {
		log.Fatalf("email %q already exsts", email)
	}
	if err != nil {
		log.Fatalf("unknown error: %s", err)
	}
}
