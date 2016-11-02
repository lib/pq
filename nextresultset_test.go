// +build go1.8

package pq

import "testing"

func TestMultipleSimpleQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	rows, err := db.Query("select 1; set time zone default; select 2")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var i int
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expected 1, got %d", i)
		}
	}
	if !rows.NextResultSet() {
		t.Fatal("expected more result sets", rows.Err())
	}
	for rows.Next() {
		if err := rows.Scan(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expected 2, got %d", i)
		}
	}
}
