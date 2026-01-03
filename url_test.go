package pq

import (
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		in, want, wantErr string
	}{
		{"postgres://", "", ""},
		{"postgres://hostname.remote", "host='hostname.remote'", ""},
		{"postgres://[::1]:1234", "host='::1' port='1234'", ""},
		{"postgres://username:top%20secret@hostname.remote:1234/database",
			`dbname='database' host='hostname.remote' password='top secret' port='1234' user='username'`, ""},
		{"postgres://localhost/a%2Fb", "dbname='a/b' host='localhost'", ""},

		{"", "", "invalid connection protocol:"},
		{"http://hostname.remote", "", "invalid connection protocol: http"},

		//{"postgresql://%2Fvar%2Flib%2Fpostgresql/dbname", "", ``},
		//{"postgres:// host/db", "dbname='db' host='host'", ""},
		//{"postgres://host/db ", "dbname='db' host='host'", ""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have, err := ParseURL(tt.in)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatal(err)
			}
			if have != tt.want {
				t.Errorf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}
