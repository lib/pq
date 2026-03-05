package pq

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

type mockSSLServerOpts struct {
	serverCert tls.Certificate
	clientCAs  *x509.CertPool // nil means don't request client certs
}

// mockPostgresSSLServer creates a mock PostgreSQL server with TLS.
// If opts.clientCAs is set, the server requests and manually verifies client
// certificates against that pool (simulating PostgreSQL's ssl_ca_file).
func mockPostgresSSLServer(t *testing.T, opts mockSSLServerOpts) (port string, errCh chan error) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	errCh = make(chan error, 1)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}
		handleMockSSLConn(t, conn, opts, errCh)
	}()

	_, port, _ = net.SplitHostPort(l.Addr().String())
	return port, errCh
}

func handleMockSSLConn(t *testing.T, conn net.Conn, opts mockSSLServerOpts, errCh chan error) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Read SSL request message
	startupMessage := make([]byte, 8)
	if _, err := io.ReadFull(conn, startupMessage); err != nil {
		errCh <- fmt.Errorf("reading startup: %w", err)
		return
	}
	if !bytes.Equal(startupMessage, []byte{0, 0, 0, 0x8, 0x4, 0xd2, 0x16, 0x2f}) {
		errCh <- fmt.Errorf("unexpected startup message: %#v", startupMessage)
		return
	}

	// Respond with SSLOk
	if _, err := conn.Write([]byte("S")); err != nil {
		errCh <- fmt.Errorf("writing SSLOk: %w", err)
		return
	}

	// Configure TLS
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{opts.serverCert},
	}
	if opts.clientCAs != nil {
		// RequireAnyClientCert: request a client cert but don't let Go verify
		// it automatically. We do manual verification after the handshake to
		// simulate what PostgreSQL does.
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	}

	tlsConn := tls.Server(conn, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		errCh <- fmt.Errorf("TLS handshake: %w", err)
		return
	}
	defer tlsConn.Close()

	// Manually verify client cert chain if requested.
	if opts.clientCAs != nil {
		state := tlsConn.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			errCh <- fmt.Errorf("client did not present a certificate")
			return
		}
		intermediates := x509.NewCertPool()
		for _, cert := range state.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
		_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
			Roots:         opts.clientCAs,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		})
		if err != nil {
			errCh <- fmt.Errorf("client cert verification failed: %w", err)
			return
		}
	}

	// Read PostgreSQL startup message
	buf := make([]byte, 4)
	if _, err := io.ReadFull(tlsConn, buf); err != nil {
		errCh <- err
		return
	}
	length := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
	if length > 4 {
		rest := make([]byte, length-4)
		if _, err := io.ReadFull(tlsConn, rest); err != nil {
			errCh <- err
			return
		}
	}

	// AuthenticationOk + ReadyForQuery
	tlsConn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0})
	tlsConn.Write([]byte{'Z', 0, 0, 0, 5, 'I'})

	// Read client message (Ping sends a simple query)
	msgType := make([]byte, 1)
	if _, err := io.ReadFull(tlsConn, msgType); err != nil {
		errCh <- err
		return
	}
	if _, err := io.ReadFull(tlsConn, buf); err != nil {
		errCh <- err
		return
	}
	msgLen := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
	if msgLen > 4 {
		body := make([]byte, msgLen-4)
		if _, err := io.ReadFull(tlsConn, body); err != nil {
			errCh <- err
			return
		}
	}

	// CommandComplete + ReadyForQuery
	tlsConn.Write([]byte{'C', 0, 0, 0, 11})
	tlsConn.Write([]byte("SELECT\x00"))
	tlsConn.Write([]byte{'Z', 0, 0, 0, 5, 'I'})

	close(errCh) // success
	time.Sleep(100 * time.Millisecond)
}

func pingMockServer(t *testing.T, dsn string, port string, errCh chan error) error {
	t.Helper()

	connector, err := NewConnector(dsn)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientErr := db.PingContext(ctx)

	// Check server-side error first — it's more informative.
	select {
	case serverErr, ok := <-errCh:
		if ok && serverErr != nil {
			return fmt.Errorf("server: %s (client: %v)", serverErr, clientErr)
		}
	case <-time.After(2 * time.Second):
	}

	return clientErr
}

// loadServerCert loads a TLS server certificate, optionally including the
// intermediate CA cert in the chain.
func loadServerCert(t *testing.T, certFile, keyFile, intermediateFile string) tls.Certificate {
	t.Helper()

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if intermediateFile != "" {
		interPEM, err := os.ReadFile(intermediateFile)
		if err != nil {
			t.Fatal(err)
		}
		block, _ := pem.Decode(interPEM)
		if block != nil && block.Type == "CERTIFICATE" {
			cert.Certificate = append(cert.Certificate, block.Bytes)
		}
	}
	return cert
}

// TestSSLIntermediateCA tests various intermediate CA scenarios for both
// server certificate verification (verify-ca, verify-full) and client
// certificate authentication.
func TestSSLIntermediateCA(t *testing.T) {
	const (
		rootCert   = "testdata/init/root.crt"
		bundleCert = "testdata/init/root+intermediate.crt"
		interCert  = "testdata/init/intermediate.crt"
		serverCert = "testdata/init/server_intermediate.crt"
		serverKey  = "testdata/init/server_intermediate.key"
		clientCert = "testdata/init/client_intermediate.crt"
		clientKey  = "testdata/init/client_intermediate.key"
	)

	// Server cert with full chain [leaf, intermediate]
	serverFullChain := loadServerCert(t, serverCert, serverKey, interCert)
	// Server cert with only the leaf (no intermediate in chain)
	serverLeafOnly := loadServerCert(t, serverCert, serverKey, "")

	t.Run("server cert verification", func(t *testing.T) {
		tests := []struct {
			name       string
			sslmode    string
			rootcert   string
			serverCert tls.Certificate
			wantErr    bool
		}{
			// Server sends full chain [leaf, intermediate], sslrootcert has root only.
			{
				name:       "verify-ca full chain root only",
				sslmode:    "verify-ca",
				rootcert:   rootCert,
				serverCert: serverFullChain,
			},
			{
				name:       "verify-full full chain root only",
				sslmode:    "verify-full",
				rootcert:   rootCert,
				serverCert: serverFullChain,
			},

			// Server sends only leaf, sslrootcert has root+intermediate bundle.
			{
				name:       "verify-ca leaf only bundle rootcert",
				sslmode:    "verify-ca",
				rootcert:   bundleCert,
				serverCert: serverLeafOnly,
			},
			{
				name:       "verify-full leaf only bundle rootcert",
				sslmode:    "verify-full",
				rootcert:   bundleCert,
				serverCert: serverLeafOnly,
			},

			// Server sends only leaf, sslrootcert has root only — can't build chain.
			{
				name:       "verify-ca leaf only root only fails",
				sslmode:    "verify-ca",
				rootcert:   rootCert,
				serverCert: serverLeafOnly,
				wantErr:    true,
			},
			{
				name:       "verify-full leaf only root only fails",
				sslmode:    "verify-full",
				rootcert:   rootCert,
				serverCert: serverLeafOnly,
				wantErr:    true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				port, errCh := mockPostgresSSLServer(t, mockSSLServerOpts{
					serverCert: tt.serverCert,
				})
				dsn := fmt.Sprintf("host=127.0.0.1 port=%s sslmode=%s sslrootcert=%s user=test dbname=test connect_timeout=5",
					port, tt.sslmode, tt.rootcert)

				err := pingMockServer(t, dsn, port, errCh)
				if tt.wantErr && err == nil {
					t.Fatal("expected error but got nil")
				}
				if !tt.wantErr && err != nil {
					t.Fatalf("expected no error but got: %s", err)
				}
			})
		}
	})

	t.Run("client cert with intermediate CA", func(t *testing.T) {
		// Server's CA trust store has only the root CA. It needs the client to
		// send the intermediate cert in its TLS certificate chain.
		rootPEM, err := os.ReadFile(rootCert)
		if err != nil {
			t.Fatal(err)
		}
		serverCAs := x509.NewCertPool()
		serverCAs.AppendCertsFromPEM(rootPEM)

		if err := os.Chmod(clientKey, 0600); err != nil {
			t.Fatal(err)
		}

		port, errCh := mockPostgresSSLServer(t, mockSSLServerOpts{
			serverCert: serverFullChain,
			clientCAs:  serverCAs,
		})

		dsn := fmt.Sprintf(
			"host=127.0.0.1 port=%s sslmode=verify-ca sslrootcert=%s sslcert=%s sslkey=%s user=test dbname=test connect_timeout=5",
			port, bundleCert, clientCert, clientKey)

		err = pingMockServer(t, dsn, port, errCh)
		if err != nil {
			t.Fatalf("client cert with intermediate CA failed: %s", err)
		}
	})
}
