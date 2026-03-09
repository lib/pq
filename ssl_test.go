package pq

import (
	"bytes"
	_ "crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/pqutil"
	"github.com/lib/pq/internal/proto"
)

// Environment sanity check: should fail without SSL.
func startSSLTest(t *testing.T, user string) {
	_, err := pqtest.DB(t, "sslmode=disable user="+user)
	if !pqtest.ErrorContains(err, `or:no encryption (28000)|login rejected (08P01)|authentication rejected by configuration (28000)`) {
		t.Fatalf("wrong error: %q", err)
	}
}

func TestSSLMode(t *testing.T) {
	t.Parallel()
	startSSLTest(t, "pqgossl")

	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		f.Startup(cn, nil)
		for {
			code, _, ok := f.ReadMsg(cn)
			if !ok {
				return
			}
			switch code {
			case proto.Query:
				f.WriteMsg(cn, proto.EmptyQueryResponse, "")
				f.WriteMsg(cn, proto.ReadyForQuery, "I")
			case proto.Terminate:
				cn.Close()
				return
			}
		}
	})

	tests := []struct {
		connect string
		wantErr string
	}{
		// Default (prefer) should work both with and without ssl.
		{"user=pqgossl", ""},
		{f.DSN(), ""},

		// sslmode=require: require SSL, but don't verify certificate.
		{"sslmode=require user=pqgossl", ""},
		{"sslmode=require " + f.DSN(), "pq: SSL is not enabled on the server"},

		// sslmode=verify-ca: verify that the certificate was signed by a trusted CA
		{"host=postgres sslmode=verify-ca user=pqgossl", "invalid-cert"},
		{"host=postgres sslmode=verify-ca user=pqgossl sslrootcert=''", "invalid-cert"},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-ca user=pqgossl host=127.0.0.1", ""},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-ca user=pqgossl host=postgres-invalid", ""},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-ca user=pqgossl host=postgres", ""},

		// sslmode=verify-full: verify that the certification was signed by a trusted CA and the host matches
		{"sslmode=verify-full user=pqgossl host=postgres", "invalid-cert"},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-full user=pqgossl host=127.0.0.1", "invalid-cert"},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-full user=pqgossl host=postgres-invalid", "invalid-cert"},
		{"sslrootcert=testdata/ssl/root.crt sslmode=verify-full user=pqgossl host=postgres", ""},

		// With root cert
		{"sslrootcert=testdata/ssl/bogus_root.crt host=postgres sslmode=require user=pqgossl", "invalid-cert"},
		{"sslrootcert=testdata/ssl/non_existent.crt host=127.0.0.1 sslmode=require user=pqgossl", ""},
		{"sslrootcert=testdata/ssl/root.crt host=127.0.0.1 sslmode=require user=pqgossl", ""},
		{"sslrootcert=testdata/ssl/root.crt host=postgres sslmode=require user=pqgossl", ""},
		{"sslrootcert=testdata/ssl/root.crt host=postgres-invalid sslmode=require user=pqgossl", ""},

		// sslmode=prefer
		{"sslmode=prefer user=pqgossl", ""},
		{"sslmode=prefer", ""},
		{"sslmode=prefer user=pqgossl " + f.DSN(), ""}, // Doesn't support SSL, so try again without.

		// sslmode=allow
		{"sslmode=allow user=pqgossl", ""}, // Requires SSL, so will try again
		{"sslmode=allow", ""},              // Doesn't need SSL, should just work.
		{"sslmode=allow " + f.DSN(), ""},   // Idem

		// sslmode=disable
		{"sslmode=disable user=pqgossl", "or:no encryption|login rejected (08P01)|authentication rejected by configuration (28000)"},

		// sslnegotiation=direct should fail if ssl isn't required, like libpq:
		// psql: error: weak sslmode "allow" may not be used with sslnegotiation=direct (use "require", "verify-ca", or "verify-full")
		{"sslmode=disable sslnegotiation=direct", "weak sslmode"},
		{"sslmode=allow sslnegotiation=direct", "weak sslmode"},
		{"sslmode=prefer sslnegotiation=direct", "weak sslmode"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			_, err := pqtest.DB(t, tt.connect)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("\nfailed for %q\n%s", tt.connect, err)
			case tt.wantErr == "invalid-cert":
				if !pqtest.InvalidCertificate(err) {
					t.Fatalf("wrong error type %T: %[1]s", err)
				}
			case !pqtest.ErrorContains(err, tt.wantErr):
				t.Fatalf("wrong error\nwant: %s\nhave: %s", tt.wantErr, err)
			}
		})
	}
}

// Authenticate over SSL using client certificates
func TestSSLClientCertificates(t *testing.T) {
	pqtest.SkipPgpool(t)    // TODO: can't get it to work.
	pqtest.SkipPgbouncer(t) // TODO: can't get it to work.
	t.Parallel()
	startSSLTest(t, "pqgosslcert")
	pqtest.Chmod(t, 0o600, "testdata/ssl/postgresql.key")

	tests := []struct {
		connect string
		wantErr string
	}{
		{"sslmode=require user=pqgosslcert", " (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=''", " (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=/tmp/filedoesnotexist", " (28000)"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/ssl/postgresql.crt", "directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/ssl/postgresql.crt sslkey=''", "directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/ssl/postgresql.crt sslkey=/tmp/filedoesnotexist", "no such file or directory"},
		{"sslmode=require user=pqgosslcert sslcert=testdata/ssl/postgresql.crt sslkey=testdata/ssl/postgresql.crt", "has world access"},

		{"sslmode=require user=pqgosslcert sslcert=testdata/ssl/postgresql.crt sslkey=testdata/ssl/postgresql.key", ""},

		{fmt.Sprintf("sslmode=require user=pqgosslcert sslinline=true sslcert='%s' sslkey='%s'",
			pqtest.Read(t, "testdata/ssl/postgresql.crt"),
			pqtest.Read(t, "testdata/ssl/postgresql.key")),
			""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db, err := pqtest.DB(t, tt.connect)
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

func TestSSLClientCertificateIntermediate(t *testing.T) {
	pqtest.SkipPgpool(t)    // TODO: can't get it to work.
	pqtest.SkipPgbouncer(t) // TODO: can't get it to work.
	t.Parallel()
	startSSLTest(t, "pqgosslcert")
	pqtest.Chmod(t, 0o600, "testdata/ssl/client_intermediate.key")

	tests := []struct {
		name    string
		connect string
		wantErr string
	}{
		{
			// Client cert signed by intermediate CA, sslrootcert has
			// root+intermediate bundle. The server's ssl_ca_file has only root.crt,
			// so sslAppendIntermediates must send the intermediate in the TLS chain.
			name: "file certs",
			connect: "sslmode=require user=pqgosslcert " +
				"sslrootcert=testdata/ssl/root+intermediate.crt " +
				"sslcert=testdata/ssl/client_intermediate.crt " +
				"sslkey=testdata/ssl/client_intermediate.key",
		},
		{
			name: "inline certs",
			connect: fmt.Sprintf(
				"sslmode=require user=pqgosslcert sslinline=true sslrootcert='%s' sslcert='%s' sslkey='%s'",
				pqtest.Read(t, "testdata/ssl/root+intermediate.crt"),
				pqtest.Read(t, "testdata/ssl/client_intermediate.crt"),
				pqtest.Read(t, "testdata/ssl/client_intermediate.key"),
			),
		},
		{
			// Without the intermediate in sslrootcert, sslAppendIntermediates has
			// nothing to append, so the server can't verify the client cert chain.
			name: "fails without intermediate in sslrootcert",
			connect: "sslmode=require user=pqgosslcert " +
				"sslrootcert=testdata/ssl/root.crt " +
				"sslcert=testdata/ssl/client_intermediate.crt " +
				"sslkey=testdata/ssl/client_intermediate.key",
			wantErr: "unknown certificate authority",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, err := pqtest.DB(t, tt.connect)
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

func TestSSLSNI(t *testing.T) {
	t.Parallel()
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

func TestSSLVersion(t *testing.T) {
	t.Parallel()
	startSSLTest(t, "pqgossl")
	RegisterTLSConfig("empty", &tls.Config{})

	tests := []struct {
		in, wantErr string
	}{
		// All the containers require 1.2
		{"sslmode=require ssl_min_protocol_version=TLSv1.3", ``},
		{"sslmode=require ssl_max_protocol_version=TLSv1.0", `tls: no supported versions`},
		{"sslmode=pqgo-empty ssl_max_protocol_version=TLSv1.0", `tls: no supported versions`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, err := pqtest.DB(t, "user=pqgossl "+tt.in)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error: %v", err)
			}
		})
	}
}

// Test that the defaults are being used by writing invalid data to them:
// they're skipped if they don't exist, but do error if they're invalid.
func TestSSLDefaults(t *testing.T) {
	pqtest.SkipPgpool(t)    // TODO: can't get it to work.
	pqtest.SkipPgbouncer(t) // TODO: can't get it to work.
	startSSLTest(t, "pqgosslcert")

	tests := []struct {
		file    string
		wantErr string
	}{
		{"root.crt", `couldn't parse pem from sslrootcert`},
		{"postgresql.crt", `failed to find any PEM data in certificate input`},
		{"postgresql.key", `failed to find any PEM data in key input`},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			pqtest.Home(t)

			pqtest.Write(t, []byte("invalid data"), pqutil.Home(true), tt.file)
			if tt.file == "postgresql.crt" {
				pqtest.Write(t, pqtest.Read(t, "testdata/ssl/postgresql.key"), pqutil.Home(true), "postgresql.key")
				pqtest.Chmod(t, 0o600, pqutil.Home(true), "postgresql.key")
			}
			if tt.file == "postgresql.key" {
				pqtest.Write(t, pqtest.Read(t, "testdata/ssl/postgresql.crt"), pqutil.Home(true), "postgresql.crt")
				pqtest.Chmod(t, 0o600, pqutil.Home(true), "postgresql.key")
			}

			_, err := pqtest.DB(t, "user=pqgossl sslmode=require")
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error:\nhave: %v\nwant: %s", err, tt.wantErr)
			}
		})
	}

	t.Run("work with default paths", func(t *testing.T) {
		pqtest.Home(t)
		pqtest.Write(t, pqtest.Read(t, "testdata/ssl/root.crt"), pqutil.Home(true), "root.crt")
		pqtest.Write(t, pqtest.Read(t, "testdata/ssl/postgresql.crt"), pqutil.Home(true), "postgresql.crt")
		pqtest.Write(t, pqtest.Read(t, "testdata/ssl/postgresql.key"), pqutil.Home(true), "postgresql.key")
		pqtest.Chmod(t, 0o600, pqutil.Home(true), "postgresql.key")
		_ = pqtest.MustDB(t, "host=postgres user=pqgosslcert sslmode=verify-ca")
	})
}

func TestSSLRootCA(t *testing.T) {
	startSSLTest(t, "pqgossl")

	t.Cleanup(func() { testSystemRoots = nil })
	testSystemRoots = x509.NewCertPool()
	if !testSystemRoots.AppendCertsFromPEM(pqtest.Read(t, "testdata/ssl/root.crt")) {
		t.Fatal()
	}

	tests := []struct {
		connect string
		wantErr string
	}{
		{"", ``},
		{"sslmode=verify-full", ``},

		{"sslmode=verify-ca", `weak sslmode`},
		{"sslmode=disable", `weak sslmode`},
		{"sslmode=allow", `weak sslmode`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, err := pqtest.DB(t, "host=postgres user=pqgossl sslrootcert=system "+tt.connect)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error:\nhave: %v\nwant: %s", err, tt.wantErr)
			}
		})
	}
}

func TestUnreadableHome(t *testing.T) {
	// Ignore HOME being unset or not a directory
	for _, h := range []string{"", "/dev/null"} {
		t.Setenv("HOME", h)
		err := sslClientCertificates(&tls.Config{}, Config{}, h)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ssl(Config{}, SSLModeRequire)
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
