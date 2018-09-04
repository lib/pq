package netaddr

import (
	"bytes"
	"net"
	"testing"

	_ "github.com/lib/pq"
)

func TestMacaddr(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	macaddr := Macaddr{}

	// Test scanning NULL values
	err := db.QueryRow("SELECT NULL::macaddr").Scan(&macaddr)
	if err != nil {
		t.Fatal(err)
	}
	if macaddr.Valid {
		t.Fatalf("expected null result")
	}

	// Test setting NULL values
	err = db.QueryRow("SELECT $1::macaddr", macaddr).Scan(&macaddr)
	if err != nil {
		t.Fatalf("re-query null value failed: %s", err.Error())
	}
	if macaddr.Valid {
		t.Fatalf("expected null result")
	}

	// test encoding in query params, then decoding during Scan
	testBidirectional := func(m Macaddr, label string) {
		err = db.QueryRow("SELECT $1::macaddr", m).Scan(&macaddr)
		if err != nil {
			t.Fatalf("re-query %s macaddr failed: %s", label, err.Error())
		}
		if !macaddr.Valid {
			t.Fatalf("expected non-null value, got null for %s", label)
		}
		if !bytes.Equal(m.Macaddr, macaddr.Macaddr) {
			t.Fatalf("expected MAC addresses to match, but did not for %s", label)
		}
	}

	var simpleMac = Macaddr{Macaddr: net.HardwareAddr{1, 0x23, 0x45, 0x67, 0x89, 0xab}, Valid: true}
	testBidirectional(simpleMac, "Simple MAC Address")

	// Bad argument
	macaddr = Macaddr{}
	err = macaddr.Scan(456)
	if err == nil {
		t.Fatal("Expected error for non-byte[] argument to Scan")
	}

	macaddr = Macaddr{}
	err = macaddr.Scan([]byte(""))
	if err == nil {
		t.Fatalf("Expected error for invalid Macaddr")
	}
}
