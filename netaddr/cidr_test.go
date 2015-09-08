package netaddr

import (
	"bytes"
	"net"
	"testing"

	_ "github.com/lib/pq"
)

func TestCidr(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	cidr := Cidr{}

	// Test scanning NULL values
	err := db.QueryRow("SELECT NULL::cidr").Scan(&cidr)
	if err != nil {
		t.Fatal(err)
	}
	if cidr.Valid {
		t.Fatalf("expected null result")
	}

	// Test setting NULL values
	err = db.QueryRow("SELECT $1::cidr", cidr).Scan(&cidr)
	if err != nil {
		t.Fatalf("re-query null value failed: %s", err.Error())
	}
	if cidr.Valid {
		t.Fatalf("expected null result")
	}

	// test encoding in query params, then decoding during Scan
	testBidirectional := func(c Cidr, label string) {
		err = db.QueryRow("SELECT $1::cidr", c).Scan(&cidr)
		if err != nil {
			t.Fatalf("re-query %s cidr failed: %s", label, err.Error())
		}
		if !cidr.Valid {
			t.Fatalf("expected non-null value, got null for %s", label)
		}
		if bytes.Compare(c.Cidr.IP, cidr.Cidr.IP) != 0 {
			t.Fatalf("expected IP addresses to match, but did not for %s - %s %s", label, c.Cidr.IP.String(), cidr.Cidr.IP.String())
		}
		if bytes.Compare(c.Cidr.Mask, cidr.Cidr.Mask) != 0 {
			t.Fatalf("expected net masks to match, but did not for %s", label)
		}
	}

	// a few example CIDRs to test out
	_, exampleCidr, err := net.ParseCIDR("135.104.0.0/32")
	if err != nil {
		t.Fatalf("Fatal error while building simple IP example - %s", err.Error())
	}
	var simpleIP4 = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(simpleIP4, "Simple IPv4")

	_, exampleCidr, err = net.ParseCIDR("0.0.0.0/24")
	if err != nil {
		t.Fatalf("Fatal error while building Zero IP example - %s", err.Error())
	}
	var zeroIP4Subnet = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(zeroIP4Subnet, "Zero IPv4 Subnet")

	_, exampleCidr, err = net.ParseCIDR("135.104.0.0/24")
	if err != nil {
		t.Fatalf("Fatal error while building simple IPv4 subnet example - %s", err.Error())
	}
	var simpleIP4Subnet = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(simpleIP4Subnet, "Simple IPv4 Subnet")

	_, exampleCidr, err = net.ParseCIDR("::1/128")
	if err != nil {
		t.Fatalf("Fatal error while building simple IPv6 loopback example - %s", err.Error())
	}
	var ip6Loopback = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(ip6Loopback, "IPv6 Loopback")

	_, exampleCidr, err = net.ParseCIDR("abcd:2345::/65")
	if err != nil {
		t.Fatalf("Fatal error while building simple IPv6 subnet example - %s", err.Error())
	}
	var ip6Subnet = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(ip6Subnet, "IPv6 Subnet #1")

	_, exampleCidr, err = net.ParseCIDR("abcd:2300::/24")
	if err != nil {
		t.Fatalf("Fatal error while building simple IPv6 subnet #2 example - %s", err.Error())
	}
	var ip6Subnet2 = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(ip6Subnet2, "IPv6 Subnet #2")

	_, exampleCidr, err = net.ParseCIDR("2001:DB8::1/48")
	if err != nil {
		t.Fatalf("Fatal error while building simple IPv6 subnet #3 example - %s", err.Error())
	}
	var ip6Subnet3 = Cidr{Cidr: *exampleCidr, Valid: true}
	testBidirectional(ip6Subnet3, "IPv6 Subnet #3")

	// Error handling

	// Bad argument
	cidr = Cidr{}
	err = cidr.Scan(456)
	if err == nil {
		t.Fatal("Expected error for non-byte[] argument to Scan")
	}

	cidr = Cidr{}
	err = cidr.Scan([]byte(""))
	if err == nil {
		t.Fatalf("Expected error for invalid CIDR")
	}
}
