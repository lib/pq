package pq

import "testing"

func TestCopyInStmt(t *testing.T) {
	tests := []struct {
		inTable string
		inCols  []string
		want    string
	}{
		{`table name`, nil, `COPY "table name" FROM STDIN`},
		{"table name", []string{"column 1", "column 2"}, `COPY "table name" ("column 1", "column 2") FROM STDIN`},
		{`table " name """`, []string{`co"lumn""`}, `COPY "table "" name """"""" ("co""lumn""""") FROM STDIN`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			have := CopyIn(tt.inTable, tt.inCols...)
			if have != tt.want {
				t.Fatalf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}

func TestCopyInSchemaStmt(t *testing.T) {
	tests := []struct {
		inSchema string
		inTable  string
		inCols   []string
		want     string
	}{
		{"schema name", "table name", nil,
			`COPY "schema name"."table name" FROM STDIN`},

		{"schema name", "table name", []string{"column 1", "column 2"},
			`COPY "schema name"."table name" ("column 1", "column 2") FROM STDIN`},

		{`schema " name """`, `table " name """`, []string{`co"lumn""`},
			`COPY "schema "" name """""""."table "" name """"""" ("co""lumn""""") FROM STDIN`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			have := CopyInSchema(tt.inSchema, tt.inTable, tt.inCols...)
			if have != tt.want {
				t.Fatalf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}
