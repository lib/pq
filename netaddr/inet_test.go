package netaddr

import (
	"net"
	"testing"

	_ "github.com/lib/pq"
)

func TestInet(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	inet := Inet{}

	// Test scanning NULL values
	err := db.QueryRow("SELECT NULL::inet").Scan(&inet)
	if err != nil {
		t.Fatal(err)
	}
	if inet.Inet != nil {
		t.Fatalf("expected null result")
	}

	// Test setting NULL values
	err = db.QueryRow("SELECT $1::inet", inet).Scan(&inet)
	if err != nil {
		t.Fatalf("re-query null value failed: %s", err.Error())
	}
	if inet.Inet != nil {
		t.Fatalf("expected null result")
	}

	// test encoding in query params, then decoding during Scan
	testBidirectional := func(i Inet, label string) {
		err = db.QueryRow("SELECT $1::inet", i).Scan(&inet)
		if err != nil {
			t.Fatalf("re-query %s inet failed: %s", label, err.Error())
		}
		if inet.Inet == nil {
			t.Fatalf("expected non-null value, got null for %s", label)
		}
		if !net.IP.Equal(i.Inet, inet.Inet) {
			t.Fatalf("expected IP addresses to match, but did not for %s - %s %s", label, i.Inet.String(), inet.Inet.String())
		}
	}

	testBidirectional(Inet{Inet: net.ParseIP("192.168.0.1")}, "Simple IPv4")
	testBidirectional(Inet{Inet: net.ParseIP("::1")}, "Loopback IPv6")
	testBidirectional(Inet{Inet: net.ParseIP("abcd:2345::")}, "Loopback IPv6")

	// Bad argument
	inet = Inet{}
	err = inet.Scan(456)
	if err == nil {
		t.Fatal("Expected error for non-byte[] argument to Scan")
	}

	inet = Inet{}
	err = inet.Scan([]byte(""))
	if err != nil {
		t.Fatalf("Unexpected error for empty string - %s", err.Error())
	}
	if inet.Inet == nil {
		t.Fatalf("Unexpected not null for empty/non-IP string string")
	}
}
