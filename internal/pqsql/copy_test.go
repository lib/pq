package pqsql

import (
	"testing"
)

func TestStartsWithCopy(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"COPY data;", true},
		{"copy data;", true},
		{"   COPY", true},
		{"-- comment only\nCOPY table", true},
		{"/* comment */COPY table", true},
		{"/* comment */  COPY table", true},
		{"-- comment only\n/* and another */COPY table", true},
		{"\n\n/* header */  COPY my_table FROM stdin;", true},
		{"  -- some comment\n /* block */  COPY table FROM stdin;", true},
		{"SELECT * FROM users;", false},
		{"-- some comment not terminated on purpose (or not) COPY table FROM stdin;", false},
		{"-- COPY table FROM stdin;\nSELECT * FROM users;", false},

		{"", false},
		{"c", false},
		{"co", false},
		{"cop", false},
		{"copy", true},
		{"/", false},
		{"/*", false},
		{"/* *", false},
		{"/* */", false},
		{"-", false},
		{"--", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			have := StartsWithCopy(tt.input)
			if have != tt.want {
				t.Errorf("want %v; have %v", tt.want, have)
			}
		})
	}
}

func BenchmarkStartsWithCopy(b *testing.B) {
	sql := "  -- comment\n /* block */ \n COPY table FROM stdin;"
	for i := 0; i < b.N; i++ {
		_ = StartsWithCopy(sql)
	}
}
