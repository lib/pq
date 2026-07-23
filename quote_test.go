package pq

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`foo`, `"foo"`},
		{`foo bar baz`, `"foo bar baz"`},
		{`foo"bar`, `"foo""bar"`},
		{"foo\x00bar", `"foo"`},
		{"\x00foo", `""`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := QuoteIdentifier(tt.input)
			if have != tt.want {
				t.Errorf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`foo`, `'foo'`},
		{`foo bar baz`, `'foo bar baz'`},
		{`foo'bar`, `'foo''bar'`},
		{`foo\bar`, ` E'foo\\bar'`},
		{`foo\ba'r`, ` E'foo\\ba''r'`},
		{`foo"bar`, `'foo"bar'`},
		{`foo\x00bar`, ` E'foo\\x00bar'`},
		{`\x00foo`, ` E'\\x00foo'`},
		{`'`, `''''`},
		{`''`, `''''''`},
		{`\`, ` E'\\'`},
		{`'abc'; DROP TABLE users;`, `'''abc''; DROP TABLE users;'`},
		{`\'`, ` E'\\'''`},
		{`E'\''`, ` E'E''\\'''''`},
		{`e'\''`, ` E'e''\\'''''`},
		{`E'\'abc\'; DROP TABLE users;'`, ` E'E''\\''abc\\''; DROP TABLE users;'''`},
		{`e'\'abc\'; DROP TABLE users;'`, ` E'e''\\''abc\\''; DROP TABLE users;'''`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := QuoteLiteral(tt.input)
			if have != tt.want {
				t.Errorf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}


func TestQuoteLiteralNullByte(t *testing.T) {
	if got, want := QuoteLiteral("foo\x00bar"), "'foo'"; got != want {
		t.Errorf("embedded NUL: have %q want %q", got, want)
	}
	if got, want := QuoteLiteral("\x00foo"), "''"; got != want {
		t.Errorf("leading NUL: have %q want %q", got, want)
	}
	if got, want := QuoteLiteral("a\x00b'c"), "'a'"; got != want {
		t.Errorf("NUL before quote: have %q want %q", got, want)
	}
}
