package pq

import (
	"testing"
)

func TestSimpleParseURL(t *testing.T) {
	expected := "host=hostname.remote"
	str, err := ParseURL("postgres://hostname.remote")
	if err != nil {
		t.Fatal(err)
	}

	if str != expected {
		t.Fatalf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
	}
}

func TestIPv6LoopbackParseURL(t *testing.T) {
	expected := "host=::1 port=1234"
	str, err := ParseURL("postgres://[::1]:1234")
	if err != nil {
		t.Fatal(err)
	}

	if str != expected {
		t.Fatalf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
	}
}

func TestFullParseURL(t *testing.T) {
	expected := `dbname=database host=hostname.remote password=top\ secret port=1234 user=username`
	str, err := ParseURL("postgres://username:top%20secret@hostname.remote:1234/database")
	if err != nil {
		t.Fatal(err)
	}

	if str != expected {
		t.Fatalf("unexpected result from ParseURL:\n+ %s\n- %s", str, expected)
	}
}

func TestInvalidProtocolParseURL(t *testing.T) {
	_, err := ParseURL("http://hostname.remote")
	switch err {
	case nil:
		t.Fatal("Expected an error from parsing invalid protocol")
	default:
		msg := "invalid connection protocol: http"
		if err.Error() != msg {
			t.Fatalf("Unexpected error message:\n+ %s\n- %s",
				err.Error(), msg)
		}
	}
}

func TestMinimalURL(t *testing.T) {
	cs, err := ParseURL("postgres://")
	if err != nil {
		t.Fatal(err)
	}

	if cs != "" {
		t.Fatalf("expected blank connection string, got: %q", cs)
	}
}

func TestParseURLMultipleHosts(t *testing.T) {
	expected := "host=host1,host2"
	str, err := ParseURL("postgres://host1,host2/")
	if err != nil {
		t.Fatal(err)
	}
	if str != expected {
		t.Fatalf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
	}
}
func TestParseURLMultipleHostsMultiplePorts(t *testing.T) {
	expected := "host=host1,host2 port=15432,25432"
	str, err := ParseURL("postgres://host1:15432,host2:25432/")
	if err != nil {
		t.Fatal(err)
	}
	if str != expected {
		t.Fatalf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
	}
}
func TestParseURLMultipleHostsSinglePort(t *testing.T) {
	expected := "host=host1,host2 port=5432"
	for _, input := range []string{"postgres://host1,host2:5432/", "postgres://host1:5432,host2/"} {
		str, err := ParseURL(input)
		if err != nil {
			t.Fatal(err)
		}
		if str != expected {
			t.Fatalf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
		}
	}
}
