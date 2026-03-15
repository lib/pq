//go:build go1.26

package pq_test

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
)

func Example_error() {
	db, _ := sql.Open("postgres", "")
	_, _ = db.Exec(`create temp table users (email text)`)
	_, _ = db.Exec(`create unique index on users(lower(email))`)

	_, err := db.Exec(`insert into users values ('a@example.com'), ('a@example.com')`)
	if err != nil {
		fmt.Println("Error()")
		fmt.Println(err)

		fmt.Println("\nErrorWithDetail()")
		if pqErr, ok := errors.AsType[*pq.Error](err); ok {
			fmt.Println(pqErr.ErrorWithDetail())
		}
	}

	// Output:
	// Error()
	// pq: duplicate key value violates unique constraint "users_lower_idx" (23505)
	//
	// ErrorWithDetail()
	// ERROR:   duplicate key value violates unique constraint "users_lower_idx" (23505)
	// DETAIL:  Key (lower(email))=(a@example.com) already exists.
}
