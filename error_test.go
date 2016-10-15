package pq

import (
	"strings"
	"testing"
)

func TestSyntaxErrorFormatting(t *testing.T) {
	for _, tt := range []struct {
		err      Error
		expected string
	}{
		// Single line
		{Error{Message: "test", Position: "8", originalQuery: "SELECT *;"},
			"pq: test\nLINE 1: SELECT *;\n               ^"},

		// Syntax error in first line
		{Error{Message: "test", Position: "1", originalQuery: "SELECT\n *;"},
			"pq: test\nLINE 1: SELECT\n        ^"},

		// Syntax error in last line
		{Error{Message: "test", Position: "9", originalQuery: "SELECT\n *;"},
			"pq: test\nLINE 2:  *;\n         ^"},

		// Bad input: position non-positive
		{Error{Message: "test", Position: "0", originalQuery: "SELECT\n *;"},
			Error{Message: "test", Position: "0", originalQuery: "SELECT\n *;"}.normalError()},

		// Bad input: position after end of string
		{Error{Message: "test", Position: "11", originalQuery: "SELECT\n *;"},
			Error{Message: "test", Position: "11", originalQuery: "SELECT\n *;"}.normalError()},
		{Error{Message: "test", Position: "not a number", originalQuery: "SELECT\n *;"},
			Error{Message: "test", Position: "not a number", originalQuery: "SELECT\n *;"}.normalError()},
	} {
		actual := tt.err.syntaxError()
		if tt.expected != actual {
			t.Errorf("bad message, expected %#v, got %#v", tt.expected, actual)
		}
	}
}

func TestSyntaxErrorHandlingWithQuery(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Query("SELECT *;")
	if err == nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(err.Error(), "   ^") {
		t.Errorf("syntax error not formatted as such. got %#v", err.Error())
	}
}

func TestSyntaxErrorHandlingWithPrepare(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Prepare("SELECT *;")
	if err == nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(err.Error(), "   ^") {
		t.Errorf("syntax error not formatted as such. got %#v", err.Error())
	}
}
