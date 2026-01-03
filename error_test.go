package pq

import (
	"errors"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestErrorSQLState(t *testing.T) {
	r := readBuf([]byte{67, 52, 48, 48, 48, 49, 0, 0}) // 40001
	err := parseError(&r, "")
	var sqlErr interface{ SQLState() string }
	if !errors.As(err, &sqlErr) {
		t.Fatal("SQLState interface not satisfied")
	}
	if state := err.SQLState(); state != "40001" {
		t.Fatalf("unexpected SQL state %v", state)
	}
}

func TestError(t *testing.T) {
	tests := []struct {
		in, want, wantDetail string
	}{
		{`create schema pg_xx`, `pq: unacceptable schema name "pg_xx" (42939)`, `
			ERROR:   unacceptable schema name "pg_xx" (42939)
			DETAIL:  The prefix "pg_" is reserved for system schemas.
		`},
		{`create view x as select 1; copy x to stdout`, `pq: cannot copy from view "x" (42809)`, `
			ERROR:   cannot copy from view "x" (42809)
			HINT:    Try the COPY (SELECT ...) TO variant.
		`},
		{`select columndoesntexist`, `pq: column "columndoesntexist" does not exist (42703)`, `
			ERROR:   column "columndoesntexist" does not exist (42703)
			CONTEXT: line 1, column 8:

			      1 | select columndoesntexist
			                 ^
		`},
		{`select !@#`, "pq: syntax error at end of input (42601)", `
			ERROR:   syntax error at end of input (42601)
			CONTEXT: line 1, column 11:

			      1 | select !@#
			                    ^
		`},
		{"select 'asd',\n\t'asd'::jsonb", "pq: invalid input syntax for type json (22P02)", `
			ERROR:   invalid input syntax for type json (22P02)
			DETAIL:  Token "asd" is invalid.
			CONTEXT: line 2, column 2:

			      1 | select 'asd',
			      2 |         'asd'::jsonb
			                  ^
		`},
		{"select 'asd'\n,'zxc',\n'def',\n123,\n'foo', 'asd'::jsonb", "pq: invalid input syntax for type json (22P02)", `
			ERROR:   invalid input syntax for type json (22P02)
			DETAIL:  Token "asd" is invalid.
			CONTEXT: line 5, column 8:

			      3 | 'def',
			      4 | 123,
			      5 | 'foo', 'asd'::jsonb
			                 ^
		`},
		{"select '€€€', a", `pq: column "a" does not exist (42703)`, `
			ERROR:   column "a" does not exist (42703)
			CONTEXT: line 1, column 15:

			      1 | select '€€€', a
			                        ^
		`},
		{"select '€€€',\n'€',a", `pq: column "a" does not exist (42703)`, `
			ERROR:   column "a" does not exist (42703)
			CONTEXT: line 2, column 5:

			      1 | select '€€€',
			      2 | '€',a
			              ^
		`},
		{pqtest.NormalizeIndent(`
			create table browsers (
			    browser_id     serial,
			    name           varchar,
			    version        varchar
			);
			create unique index "browsers#name#version" on browsers(name, version);

			create table systems (
			    system_id      serial,
			    name           varchar,
			    version        varchar,
			);
			create unique index "systems#name#version"  on systems(name, version);
		`), `pq: syntax error at or near ")" (42601)`, `
			ERROR:   syntax error at or near ")" (42601)
			CONTEXT: line 12, column 1:

			     10 |     name           varchar,
			     11 |     version        varchar,
			     12 | );
			          ^
		`},

		{pqtest.NormalizeIndent(`
			create table browsers (browser_id serial, name varchar, version varchar); create unique index "browsers#name#version" on browsers(name, version);
			create table systems (system_id serial, name varchar, version varchar,); create unique index "systems#name#version"  on systems(name, version);
		`), `pq: syntax error at or near ")" (42601)`, `
			ERROR:   syntax error at or near ")" (42601)
			CONTEXT: line 2, column 71:

			      1 | create table browsers (browser_id serial, name varchar, version varchar); create unique index "browsers#name#version" on browsers(name, version);
			      2 | create table systems (system_id serial, name varchar, version varchar,); create unique index "systems#name#version"  on systems(name, version);
			                                                                                ^
		`},
	}

	db := pqtest.MustDB(t)

	for _, tt := range tests {
		_, err := db.Exec(tt.in)
		if err == nil {
			t.Fatal("no error?")
		}
		pqErr := new(Error)
		if !errors.As(err, &pqErr) {
			t.Fatalf("wrong error %T: %[1]s", err)
		}

		if err.Error() != tt.want {
			t.Errorf("\nhave: %s\nwant: %s", err.Error(), tt.want)
		}
		tt.wantDetail = pqtest.NormalizeIndent(tt.wantDetail)
		if pqErr.query != "" && pqErr.Position != "" {
			tt.wantDetail += "\n"
		}
		if pqErr.ErrorWithDetail() != tt.wantDetail {
			t.Errorf("\nhave:\n%s\nwant:\n%s", pqErr.ErrorWithDetail(), tt.wantDetail)
		}
	}
}
