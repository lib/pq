package pq

import (
	"bytes"
	_ "crypto/sha256"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func openSSLConn(t *testing.T, conninfo ...string) (*sql.DB, error) {
	db := pqtest.MustDB(t, conninfo...)
	// Do something with the connection to see whether it's working or not.
	return db, db.Ping()
}

func startSSLTest(t *testing.T, user string) {
	t.Parallel()
	pqtest.SkipPgbouncer(t) // TODO: need to fix pgbouncer setup
	pqtest.SkipPgpool(t)    // TODO: need to fix pgpool setup

	// Environment sanity check: should fail without SSL
	_, err := openSSLConn(t, "sslmode=disable user="+user)
	pqErr := pqError(t, err)
	if pqErr.Code.Name() != "invalid_authorization_specification" {
		t.Fatalf("wrong error code %q", pqErr.Code.Name())
	}
}

func TestSSLMode(t *testing.T) {
	tests := []struct {
		connect string
		wantErr bool
	}{
		// sslmode=require: require SSL, but don't verify certificate.
		{"sslmode=require user=pqgossl", false},

		// sslmode=verify-ca: verify that the certificate was signed by a trusted CA
		{"host=postgres sslmode=verify-ca user=pqgossl", true},
		{"host=postgres sslmode=verify-ca user=pqgossl sslrootcert=''", true},

		{"sslrootcert=testdata/init/root.crt sslmode=verify-ca user=pqgossl host=127.0.0.1", false},
		{"sslrootcert=testdata/init/root.crt sslmode=verify-ca user=pqgossl host=postgres-invalid", false},
		{"sslrootcert=testdata/init/root.crt sslmode=verify-ca user=pqgossl host=postgres", false},

		// sslmode=verify-full: verify that the certification was signed by a trusted CA and the host matches
		{"sslmode=verify-full user=pqgossl host=postgres", true},
		{"sslrootcert=testdata/init/root.crt sslmode=verify-full user=pqgossl host=127.0.0.1", true},
		{"sslrootcert=testdata/init/root.crt sslmode=verify-full user=pqgossl host=postgres-invalid", true},

		{"sslrootcert=testdata/init/root.crt sslmode=verify-full user=pqgossl host=postgres", false},

		// With root cert
		{"sslrootcert=testdata/init/bogus_root.crt host=postgres sslmode=require user=pqgossl", true},

		{"sslrootcert=testdata/init/non_existent.crt host=127.0.0.1 sslmode=require user=pqgossl", false},
		{"sslrootcert=testdata/init/root.crt host=127.0.0.1 sslmode=require user=pqgossl", false},
		{"sslrootcert=testdata/init/root.crt host=postgres sslmode=require user=pqgossl", false},
		{"sslrootcert=testdata/init/root.crt host=postgres-invalid sslmode=require user=pqgossl", false},
	}

	startSSLTest(t, "pqgossl")

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			_, err := openSSLConn(t, tt.connect)
			if tt.wantErr {
				if !pqtest.InvalidCertificate(err) {
					t.Fatalf("wrong error type %T: %[1]s", err)
				}
			} else if err != nil {
				t.Errorf("\nfailed for %q\n%s", tt.connect, err)
			}
		})
	}
}

// Authenticate over SSL using client certificates
func TestSSLClientCertificates(t *testing.T) {
	startSSLTest(t, "pqgosslcert")

	// Make sure the permissions of the keyfile are correct, or it won't load.
	// TODO: will probably fail on Windows? Dunno.
	err := os.Chmod("testdata/init/postgresql.key", 0o600)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		connect string
		wantErr string
	}{
		{"sslmode=require user=pqgosslcert", "requires a valid client certificate (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=''", "requires a valid client certificate (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=/tmp/filedoesnotexist", "requires a valid client certificate (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/init/postgresql.crt", "directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/init/postgresql.crt sslkey=''", "directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/init/postgresql.crt sslkey=/tmp/filedoesnotexist", "no such file or directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/init/postgresql.crt sslkey=testdata/init/postgresql.crt", "has world access"},

		{"sslmode=require user=pqgosslcert sslcert=testdata/init/postgresql.crt sslkey=testdata/init/postgresql.key", ""},

		{fmt.Sprintf("sslmode=require user=pqgosslcert sslinline=true sslcert='%s' sslkey='%s'",
			pqtest.Read(t, "testdata/init/postgresql.crt"),
			pqtest.Read(t, "testdata/init/postgresql.key")),
			""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db, err := openSSLConn(t, tt.connect)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error\nwant: %s\nhave: %s", tt.wantErr, err)
			}

			if err == nil {
				rows, err := db.Query("select 1")
				if err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

}

// Check that clint sends SNI data when sslsni is not disabled
func TestSSLSNI(t *testing.T) {
	startSSLTest(t, "pqgosslcert")

	tests := []struct {
		name     string
		connect  string
		hostname string
		wantSNI  string
		direct   bool
	}{
		{
			name:     "SNI is set by default",
			connect:  "sslmode=require",
			hostname: "localhost",
			wantSNI:  "localhost",
		},
		{
			name:     "SNI is passed when asked for",
			connect:  "sslmode=require sslsni=1",
			hostname: "localhost",
			wantSNI:  "localhost",
		},
		{
			name:     "SNI is not passed when disabled",
			connect:  "sslmode=require sslsni=0",
			hostname: "localhost",
			wantSNI:  "",
		},
		{
			name:     "SNI is not set for IPv4",
			connect:  "sslmode=require",
			hostname: "127.0.0.1",
			wantSNI:  "",
		},
		{
			name:     "SNI is set for when CN doesn't match",
			connect:  "sslmode=require",
			hostname: "postgres-invalid",
			wantSNI:  "postgres-invalid",
		},
		{
			name:     "SNI is set for negotiated ssl",
			connect:  "sslmode=require sslnegotiation=postgres",
			hostname: "localhost",
			wantSNI:  "localhost",
		},
		{
			name:     "SNI is set for direct ssl",
			connect:  "sslmode=require sslnegotiation=direct",
			hostname: "localhost",
			wantSNI:  "localhost",
			direct:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port, nameCh, errCh := mockPostgresSSL(t, tt.direct)

			// We are okay to skip this error as we are polling errCh and we'll
			// get an error or timeout from the server side in case of problems
			// here.
			db, _ := sql.Open("postgres", fmt.Sprintf("host=%s port=%s %s", tt.hostname, port, tt.connect))
			_, _ = db.Exec("select 1")

			// Check SNI data
			select {
			case <-time.After(time.Second):
				t.Fatal("exceeded connection timeout without erroring out")
			case err := <-errCh:
				t.Fatal(err)
			case name := <-nameCh:
				if name != tt.wantSNI {
					t.Fatalf("have: %q\nwant: %q", name, tt.wantSNI)
				}
			}
		})
	}
}

func TestUnreadableHome(t *testing.T) {
	// Ignore HOME being unset or not a directory
	for _, h := range []string{"", "/dev/null"} {
		os.Setenv("HOME", h)
		err := sslClientCertificates(&tls.Config{}, values{})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Make a postgres mock server to test TLS SNI
//
// Accepts postgres StartupMessage and handles TLS clientHello, then closes a
// connection. While reading clientHello catch passed SNI data and report it to
// nameChan.
func mockPostgresSSL(t *testing.T, direct bool) (string, chan string, chan error) {
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
		return "", nil, nil
	}

	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
		return "", nil, nil
	}

	var (
		nameCh = make(chan string, 1)
		errCh  = make(chan error, 1)
	)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		t.Cleanup(func() {
			close(errCh)
			close(nameCh)
			l.Close()
		})

		err = conn.SetDeadline(time.Now().Add(time.Second))
		if err != nil {
			errCh <- err
			return
		}

		if !direct {
			// Receive StartupMessage with SSL Request
			startupMessage := make([]byte, 8)
			_, err := io.ReadFull(conn, startupMessage)
			if err != nil {
				errCh <- err
				return
			}
			// StartupMessage: first four bytes -- total len = 8, last four bytes SslRequestNumber
			if !bytes.Equal(startupMessage, []byte{0, 0, 0, 0x8, 0x4, 0xd2, 0x16, 0x2f}) {
				errCh <- fmt.Errorf("unexpected startup message: %#v", startupMessage)
				return
			}

			// Respond with SSLOk
			_, err = conn.Write([]byte("S"))
			if err != nil {
				errCh <- err
				return
			}
		}

		// Set up TLS context to catch clientHello. It will always error out during
		// handshake as no certificate is set.
		var sniHost string
		srv := tls.Server(conn, &tls.Config{
			GetConfigForClient: func(argHello *tls.ClientHelloInfo) (*tls.Config, error) {
				sniHost = argHello.ServerName
				return nil, nil
			},
		})
		defer srv.Close()

		// Do the TLS handshake ignoring errors
		_ = srv.Handshake()

		nameCh <- sniHost
	}()

	return port, nameCh, errCh
}
