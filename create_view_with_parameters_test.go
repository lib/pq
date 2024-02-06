package pq

import (
	"testing"
)

func TestFoo(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	//_, err := db.Exec("CREATE VIEW foo1 AS SELECT 100") // works
	//if err != nil {
	//	t.Fatal(err)
	//}
	//_, err = db.Exec("CREATE TABLE foo2 AS SELECT $1", 100) // works
	//if err != nil {
	//	t.Fatal(err)
	//}
	_, err := db.Exec("CREATE VIEW foo3 AS SELECT $1", 100) // doesn't work
	if err != nil {
		t.Fatal(err)
	}
	//			Error:      	Received unexpected error:
	//        	            	pq: got 1 parameters but the statement requires 0
	//        	Test:       	TestFoo
}
