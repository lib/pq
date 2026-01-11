//go:build go1.10

package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"reflect"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestNewConnector_WorksWithOpenDB(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Connect(t *testing.T) {
	c, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Driver(t *testing.T) {
	c, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Driver().Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Environ(t *testing.T) {
	os.Setenv("PGPASSFILE", "/tmp/.pgpass")
	defer os.Unsetenv("PGPASSFILE")
	c, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	for key, expected := range map[string]string{
		"passfile": "/tmp/.pgpass",
	} {
		if got := c.opts[key]; got != expected {
			t.Fatalf("Getting values from environment variables, for %v expected %s got %s", key, expected, got)
		}
	}

}

func TestParseEnviron(t *testing.T) {
	tests := []struct {
		in   []string
		want map[string]string
	}{
		{[]string{"PGDATABASE=hello", "PGUSER=goodbye"},
			map[string]string{"dbname": "hello", "user": "goodbye"}},
		{[]string{"PGDATESTYLE=ISO, MDY"},
			map[string]string{"datestyle": "ISO, MDY"}},
		{[]string{"PGCONNECT_TIMEOUT=30"},
			map[string]string{"connect_timeout": "30"}},
	}

	for i, tt := range tests {
		results := parseEnviron(tt.in)
		if !reflect.DeepEqual(tt.want, results) {
			t.Errorf("%d: want: %#v Got: %#v", i, tt.want, results)
		}
	}
}

func TestIsUTF8(t *testing.T) {
	var cases = []struct {
		name string
		want bool
	}{
		{"unicode", true},
		{"utf-8", true},
		{"utf_8", true},
		{"UTF-8", true},
		{"UTF8", true},
		{"utf8", true},
		{"u n ic_ode", true},
		{"ut_f%8", true},
		{"ubf8", false},
		{"punycode", false},
	}

	for _, test := range cases {
		if g := isUTF8(test.name); g != test.want {
			t.Errorf("isUTF8(%q) = %v want %v", test.name, g, test.want)
		}
	}
}

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
