package pq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestConnectorWithNoticeHandler_Simple(t *testing.T) {
	raise := func(c driver.Connector, t *testing.T, n string) {
		db := sql.OpenDB(c)
		defer db.Close()
		pqtest.Exec(t, db, fmt.Sprintf(`
			do language plpgsql $$ begin
				raise notice '%s';
			end $$
		`, n))
	}

	b, err := NewConnector(pqtest.DSN(""))
	if err != nil {
		t.Fatal(err)
	}
	var notice *Error

	// Make connector w/ handler to set the local var
	c := ConnectorWithNoticeHandler(b, func(n *Error) { notice = n })
	raise(c, t, "Test notice #1")
	if notice == nil || notice.Message != "Test notice #1" {
		t.Fatalf("Expected notice w/ message, got %v", notice)
	}
	// Unset the handler on the same connector
	prevC := c
	if c = ConnectorWithNoticeHandler(c, nil); c != prevC {
		t.Fatalf("Expected to not create new connector but did")
	}
	raise(c, t, "Test notice #2")
	if notice == nil || notice.Message != "Test notice #1" {
		t.Fatalf("Expected notice to not change, got %v", notice)
	}
	// Set it back on the same connector
	if c = ConnectorWithNoticeHandler(c, func(n *Error) { notice = n }); c != prevC {
		t.Fatal("Expected to not create new connector but did")
	}
	raise(c, t, "Test notice #3")
	if notice == nil || notice.Message != "Test notice #3" {
		t.Fatalf("Expected notice w/ message, got %v", notice)
	}
}
