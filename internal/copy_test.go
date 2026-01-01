package internal

import (
	"strings"
	"testing"
)

func TestStartsWithCOPY(t *testing.T) {
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
			have := StartsWithCOPY(tt.input)
			if have != tt.want {
				t.Errorf("want %v; have %v", tt.want, have)
			}
			have = StartsWithCOPY2(tt.input)
			if have != tt.want {
				t.Errorf("(2) want %v; have %v", tt.want, have)
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

func BenchmarkStartsWithCOPY2(b *testing.B) {
	sql := "  -- comment\n /* block */ COPY table FROM stdin;"
	for i := 0; i < b.N; i++ {
		_ = StartsWithCOPY2(sql)
	}
}

func BenchmarkEqualFold(b *testing.B) {
	sql := "COPY table FROM stdin;"
	for i := 0; i < b.N; i++ {
		_ = strings.EqualFold(sql, "COPY")
	}
}
