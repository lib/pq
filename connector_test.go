//go:build go1.10

package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func TestNewConnector(t *testing.T) {
	// database/sql might not call our Open at all unless we do something with
	// the connection
	useConn := func(t *testing.T, db any) {
		t.Helper()
		switch db := db.(type) {
		default:
			t.Fatalf("unknown type: %T", db)
		case *sql.DB:
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			tx.Rollback()
		case driver.Conn:
			tx, err := db.Begin() //lint:ignore SA1019 x
			if err != nil {
				t.Fatal(err)
			}
			tx.Rollback()
		}
	}

	t.Run("WorksWithOpenDB", func(t *testing.T) {
		c, err := NewConnector("")
		if err != nil {
			t.Fatal(err)
		}
		db := sql.OpenDB(c)
		defer db.Close()
		useConn(t, db)
	})
	t.Run("Connect", func(t *testing.T) {
		c, err := NewConnector("")
		if err != nil {
			t.Fatal(err)
		}
		db, err := c.Connect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		useConn(t, db)
	})
	t.Run("Driver", func(t *testing.T) {
		c, err := NewConnector("")
		if err != nil {
			t.Fatal(err)
		}
		db, err := c.Driver().Open("")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		useConn(t, db)
	})
	t.Run("Environ", func(t *testing.T) {
		os.Setenv("PGPASSFILE", "/tmp/.pgpass")
		defer os.Unsetenv("PGPASSFILE")
		c, err := NewConnector("")
		if err != nil {
			t.Fatal(err)
		}
		if have := c.opts["passfile"]; have != "/tmp/.pgpass" {
			t.Fatalf("wrong option for pgassfile: %q", have)
		}
	})

	t.Run("WithConfig", func(t *testing.T) {
		cfg, err := NewConfig("")
		if err != nil {
			t.Fatal(err)
		}
		cfg.SSLMode = SSLModeDisable
		cfg.Runtime = map[string]string{"search_path": "foo"}

		c, err := NewConnectorConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf(
			`map[client_encoding:UTF8 connect_timeout:20 datestyle:ISO, MDY dbname:pqgo host:localhost port:%d search_path:foo sslmode:disable sslsni:yes user:pqgo]`,
			cfg.Port)
		if have := fmt.Sprintf("%v", c.opts); have != want {
			t.Errorf("\nhave: %s\nwant: %s", have, want)
		}

		// pq: unsupported startup parameter: search_path (08P01)
		pqtest.SkipPgbouncer(t)

		db := sql.OpenDB(c)
		defer db.Close()
		useConn(t, db)
	})
}

func TestParseOpts(t *testing.T) {
	tests := []struct {
		in      string
		want    values
		wantErr string
	}{
		{"dbname=hello user=goodbye", values{"dbname": "hello", "user": "goodbye"}, ""},
		{"dbname=hello user=goodbye  ", values{"dbname": "hello", "user": "goodbye"}, ""},
		{"dbname = hello user=goodbye", values{"dbname": "hello", "user": "goodbye"}, ""},
		{"dbname=hello user =goodbye", values{"dbname": "hello", "user": "goodbye"}, ""},
		{"dbname=hello user= goodbye", values{"dbname": "hello", "user": "goodbye"}, ""},
		{"host=localhost password='correct horse battery staple'", values{"host": "localhost", "password": "correct horse battery staple"}, ""},
		{"dbname=データベース password=パスワード", values{"dbname": "データベース", "password": "パスワード"}, ""},
		{"dbname=hello user=''", values{"dbname": "hello", "user": ""}, ""},
		{"user='' dbname=hello", values{"dbname": "hello", "user": ""}, ""},
		// The last option value is an empty string if there's no non-whitespace after its =
		{"dbname=hello user=   ", values{"dbname": "hello", "user": ""}, ""},

		// The parser ignores spaces after = and interprets the next set of non-whitespace characters as the value.
		{"user= password=foo", values{"user": "password=foo"}, ""},

		// Backslash escapes next char
		{`user=a\ \'\\b`, values{"user": `a '\b`}, ""},
		{`user='a \'b'`, values{"user": `a 'b`}, ""},

		// Incomplete escape
		{`user=x\`, values{}, "missing character after backslash"},

		// No '=' after the key
		{"postgre://marko@internet", values{}, `missing "="`},
		{"dbname user=goodbye", values{}, `missing "="`},
		{"user=foo blah", values{}, `missing "="`},
		{"user=foo blah   ", values{}, `missing "="`},

		// Unterminated quoted value
		{"dbname=hello user='unterminated", values{}, `unterminated quoted string`},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			var cfg Config
			err := cfg.fromDSN(tt.in)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error\nhave: %v\nwant: %v", err, tt.wantErr)
			}
			if have := cfg.tomap(); !reflect.DeepEqual(have, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func TestRuntimeParameters(t *testing.T) {
	tests := []struct {
		conninfo      string
		param         string
		want          string
		wantErr       string
		skipPgbouncer bool
	}{
		{"DOESNOTEXIST=foo", "", "", "unrecognized configuration parameter", false},

		// we can only work with a specific value for these two
		{"client_encoding=SQL_ASCII", "", "", `unsupported client_encoding "SQL_ASCII": must be absent or "UTF8"`, false},
		{"datestyle='ISO, YDM'", "", "", `unsupported datestyle "ISO, YDM": must be absent or "ISO, MDY"`, false},

		// "options" should work exactly as it does in libpq
		// Skipped on pgbouncer as it errors with:
		//   pq: unsupported startup parameter in options: search_path
		{"options='-c search_path=pqgotest'", "search_path", "pqgotest", "", true},

		// pq should override client_encoding in this case
		// TODO: not set consistently with pgbouncer
		{"options='-c client_encoding=SQL_ASCII'", "client_encoding", "UTF8", "", true},

		// allow client_encoding to be set explicitly
		{"client_encoding=UTF8", "client_encoding", "UTF8", "", false},

		// test a runtime parameter not supported by libpq
		// Skipped on pgbouncer as it errors with:
		//   pq: unsupported startup parameter: work_mem
		{"work_mem='139kB'", "work_mem", "139kB", "", true},

		// test fallback_application_name
		{"application_name=foo fallback_application_name=bar", "application_name", "foo", "", false},
		{"application_name='' fallback_application_name=bar", "application_name", "", "", false},
		{"fallback_application_name=bar", "application_name", "bar", "", false},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if tt.skipPgbouncer {
				pqtest.SkipPgbouncer(t)
			}
			if pqtest.Pgbouncer() && tt.wantErr == "unrecognized configuration parameter" {
				tt.wantErr = `unsupported startup parameter`
			}

			db := pqtest.MustDB(t, tt.conninfo)

			var have string
			row := db.QueryRow("select current_setting($1)", tt.param)
			err := row.Scan(&have)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error\nhave: %v\nwant: %v", err, tt.wantErr)
			}
			if have != tt.want {
				t.Fatalf("\nhave: %v\nwant: %v", have, tt.want)
			}
		})
	}
}
func TestParseEnviron(t *testing.T) {
	tests := []struct {
		in   []string
		want values
	}{
		{[]string{"PGDATABASE=hello", "PGUSER=goodbye"},
			map[string]string{"dbname": "hello", "user": "goodbye"}},
		{[]string{"PGDATESTYLE=ISO, MDY"},
			map[string]string{"datestyle": "ISO, MDY"}},
		{[]string{"PGCONNECT_TIMEOUT=30"},
			map[string]string{"connect_timeout": "30"}},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			var cfg Config
			err := cfg.fromEnv(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			have := cfg.tomap()
			if !reflect.DeepEqual(tt.want, have) {
				t.Errorf("\nwant: %#v\nhave: %#v", tt.want, have)
			}
		})
	}
}

func TestIsUTF8(t *testing.T) {
	tests := []struct {
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

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := isUTF8(tt.name)
			if have != tt.want {
				t.Errorf("\nhave: %v\nwant: %v", have, tt.want)
			}
		})
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

		{"postgresql://%2Fvar%2Flib%2Fpostgresql/dbname", "", `invalid URL escape "%2F"`},
	}

	t.Parallel()
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

func TestNewConfig(t *testing.T) {
	tests := []struct {
		inDSN   string
		inEnv   []string
		want    string
		wantErr string
	}{
		// Override defaults
		{"", nil, "", ""},
		{"user=u port=1 host=example.com", nil,
			"host=example.com port=1 user=u", ""},
		{"", []string{"PGUSER=u", "PGPORT=1", "PGHOST=example.com"},
			"host=example.com port=1 user=u", ""},

		// Socket
		{"host=/var/run/psql", nil, "host=/var/run/psql sslmode=disable", ""},
		{"host=@/var/run/psql", nil, "host=@/var/run/psql sslmode=disable", ""},
		{"host=/var/run/psql sslmode=require", nil, "host=/var/run/psql sslmode=disable", ""},

		// Empty value, value with space, and value with escaped \'
		{"user=''", nil, "user=''", ""},
		{`user='with\' space'`, nil, `user='with\' space'`, ""},

		// Bool
		{"sslsni=0", nil, "sslsni=no", ""},
		{"sslsni=1", nil, "sslsni=yes", ""},
		{"sslinline=yes", nil, "sslinline=yes", ""},
		{"sslinline=no", nil, "sslinline=no", ""},
		{"sslinline=lol", nil, "", `pq: wrong value for "sslinline": strconv.ParseBool: parsing "lol": invalid syntax`},

		// application_name and fallback_application_name
		{"application_name=acme", nil, "application_name=acme", ""},
		{"application_name=acme fallback_application_name=roadrunner", nil, "application_name=acme fallback_application_name=roadrunner", ""},
		{"fallback_application_name=roadrunner", []string{"PGAPPNAME=acme"}, "application_name=acme fallback_application_name=roadrunner", ""},
		{"fallback_application_name=roadrunner", nil, "application_name=roadrunner fallback_application_name=roadrunner", ""},

		// Timeout and port
		{"connect_timeout=5", nil, "connect_timeout=5", ""},
		{"", []string{"PGCONNECT_TIMEOUT=5"}, "connect_timeout=5", ""},
		{"connect_timeout=5s", nil, "", `pq: wrong value for "connect_timeout": strconv.ParseInt: parsing "5s": invalid syntax`},
		{"", []string{"PGCONNECT_TIMEOUT=5s"}, "", `pq: wrong value for $PGCONNECT_TIMEOUT: strconv.ParseInt: parsing "5s": invalid syntax`},
		{"port=5s", nil, "", `pq: wrong value for "port": strconv.ParseUint: parsing "5s": invalid syntax`},
		{"", []string{"PGPORT=5s"}, "", `pq: wrong value for $PGPORT: strconv.ParseUint: parsing "5s": invalid syntax`},

		// hostaddr
		{"hostaddr=127.1.2.3", nil, "hostaddr=127.1.2.3", ""},
		{"hostaddr=::1", nil, "hostaddr=::1", ""},
		{"", []string{"PGHOSTADDR=2a01:4f9:3081:5413::2"}, "hostaddr=2a01:4f9:3081:5413::2", ""},

		// Runtime
		{"user=u search_path=abc", nil, "search_path=abc user=u", ""},
		{"database=db", nil, "dbname=db", ``},

		// URL
		{"postgres://u@example.com:1/db", nil,
			"dbname=db host=example.com port=1 user=u", ""},
		{"postgres://u:pw@example.com:1/db?opt=val&sslmode=require", nil,
			"dbname=db host=example.com opt=val password=pw port=1 sslmode=require user=u", ""},

		// Unsupported env vars
		{"", []string{"PGREALM=abc"}, "", `pq: environment variable $PGREALM is not supported`},
		{"", []string{"PGKRBSRVNAME=abc"}, "", `pq: environment variable $PGKRBSRVNAME is not supported`},

		// Unsupported enums
		{"sslmode=sslmeharder", nil, "", `pq: wrong value for "sslmode"`},
		{"postgres://u:pw@example.com:1/db?sslmode=sslmeharder", nil, "", `pq: wrong value for "sslmode"`},
		{"", []string{"PGSSLMODE=sslmeharder"}, "", `pq: wrong value for $PGSSLMODE`},
		{"sslnegotiation=sslmeharder", nil, "", `pq: wrong value for "sslnegotiation"`},
		{"postgres://u:pw@example.com:1/db?sslnegotiation=sslmeharder", nil, "", `pq: wrong value for "sslnegotiation"`},
		{"", []string{"PGSSLNEGOTIATION=sslmeharder"}, "", `pq: wrong value for $PGSSLNEGOTIATION`},
	}

	t.Parallel()
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have, err := newConfig(tt.inDSN, tt.inEnv)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error\nhave: %v\nwant: %v", err, tt.wantErr)
			}
			if have.string() != tt.want {
				t.Errorf("\nhave: %q\nwant: %q", have.string(), tt.want)
			}
		})
	}

	// Make sure connect_timeout is parsed as seconds.
	t.Run("connect_timeout", func(t *testing.T) {
		{
			have, err := newConfig("connect_timeout=3", []string{})
			if err != nil {
				t.Fatal(err)
			}
			if have.ConnectTimeout != 3*time.Second {
				t.Errorf("\nhave: %q\nwant: %q", have.ConnectTimeout, 3*time.Second)
			}
		}
		{
			have, err := newConfig("", []string{"PGCONNECT_TIMEOUT=4"})
			if err != nil {
				t.Fatal(err)
			}
			if have.ConnectTimeout != 4*time.Second {
				t.Errorf("\nhave: %q\nwant: %q", have.ConnectTimeout, 4*time.Second)
			}
		}
	})
}
