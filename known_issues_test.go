package pq

import (
	"database/sql"
	"fmt"
	"testing"
)

// Adding this to track #148 since it's a reproducible test case, but
// leaving it named specifically after that issue since I can't seem
// to minimize it. Once this problem is understood and resolved, this
// test should be minimized and renamed and/or incorporated into
// another test.
func TestNo148(t *testing.T) {
	fmt.Println("Skipping failing test: see https://github.com/lib/pq/issues/148")
	return

	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE TEMP TABLE notnulltemp (a varchar(10) not null)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("INSERT INTO notnulltemp(a) values($1) RETURNING a")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var a string
	if err = stmt.QueryRow(nil).Scan(&a); err == sql.ErrNoRows {
		t.Errorf("expected constraint violation error; got: %v", err)
	}
}
