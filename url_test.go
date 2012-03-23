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

func TestFullParseURL(t *testing.T) {
	expected := "port=1234 host=hostname.remote user=username password=secret dbname=database"
	str, err := ParseURL("postgres://username:secret@hostname.remote:1234/database")
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
			t.Fatal("Unexpected error message:\n+ %s\n- %s", err.Error(), msg)
		}
	}
}
