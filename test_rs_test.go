package pq

import (
	"database/sql"
	"testing"
)

func TestRedshiftParams(t *testing.T) {
	dsn := "postgres://scantron:520OkwZT6z5G@scantron.239674069128.us-east-1.redshift-serverless.amazonaws.com:5439/dev?sslmode=require"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	t.Log("Done")
}
