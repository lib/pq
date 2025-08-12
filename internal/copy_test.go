package internal

import (
	"strings"
	"testing"
)

func TestStartsWithCOPY(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{
			input: "COPY data;",
			valid: true,
		},
		{
			input: "   COPY",
			valid: true,
		},
		{
			input: "SELECT * FROM users;",
			valid: false,
		},
		{
			input: "-- comment only\n/* and another */COPY table",
			valid: true,
		},
		{
			input: "\n\n/* header */  COPY my_table FROM stdin;",
			valid: true,
		},
		{
			input: "  -- some comment\n /* block */  COPY table FROM stdin;",
			valid: true,
		},
		{
			input: "-- some comment not terminated on purpose (or not) COPY table FROM stdin;",
			valid: false,
		},
		{
			input: "-- COPY table FROM stdin;\nSELECT * FROM users;",
			valid: false,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			valid := StartsWithCOPY(test.input)
			if valid != test.valid {
				t.Errorf("Expected %q to be %v, got %v", test.input, test.valid, valid)
			}
		})
	}
}

func BenchmarkStartsWithCOPY(b *testing.B) {
	sql := "  -- comment\n /* block */ COPY table FROM stdin;"
	for i := 0; i < b.N; i++ {
		_ = StartsWithCOPY(sql)
	}
}

func BenchmarkEqualFold(b *testing.B) {
	sql := "COPY table FROM stdin;"
	for i := 0; i < b.N; i++ {
		_ = strings.EqualFold(sql, "COPY")
	}
}
