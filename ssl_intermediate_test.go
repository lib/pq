package pq

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	_ "crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

type certChain struct {
	rootPEM         []byte
	intermediatePEM []byte
	serverTLSCert   tls.Certificate
	clientCertPEM   []byte
	clientKeyPEM    []byte
}

// generateIntermediateCAChain creates:
//   - root CA
//   - intermediate CA (signed by root)
//   - server cert (signed by intermediate)
//   - client cert (signed by intermediate)
func generateIntermediateCAChain(t *testing.T) certChain {
	t.Helper()

	now := time.Now()

	// Root CA
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		NotBefore:             now,
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootCertDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	rootCert, err := x509.ParseCertificate(rootCertDER)
	if err != nil {
		t.Fatal(err)
	}

	// Intermediate CA signed by root
	interKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	interTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "Test Intermediate CA"},
		NotBefore:             now,
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	interCertDER, err := x509.CreateCertificate(rand.Reader, interTemplate, rootCert, &interKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	interCert, err := x509.ParseCertificate(interCertDER)
	if err != nil {
		t.Fatal(err)
	}

	// Server cert signed by intermediate
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    now,
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, interCert, &serverKey.PublicKey, interKey)
	if err != nil {
		t.Fatal(err)
	}

	// Client cert signed by intermediate
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: "testclient"},
		NotBefore:    now,
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, interCert, &clientKey.PublicKey, interKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}

	return certChain{
		rootPEM:         pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootCertDER}),
		intermediatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: interCertDER}),
		serverTLSCert: tls.Certificate{
			Certificate: [][]byte{serverCertDER, interCertDER},
			PrivateKey:  serverKey,
		},
		clientCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}),
		clientKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER}),
	}
}

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

// TestSSLIntermediateCA tests various intermediate CA scenarios for both
// server certificate verification (verify-ca, verify-full) and client
// certificate authentication.
func TestSSLIntermediateCA(t *testing.T) {
	chain := generateIntermediateCAChain(t)

	// Server cert with only the leaf (no intermediate in chain)
	serverCertLeafOnly := tls.Certificate{
		Certificate: [][]byte{chain.serverTLSCert.Certificate[0]},
		PrivateKey:  chain.serverTLSCert.PrivateKey,
	}

	rootCertFile := pqtest.TempFile(t, "root.crt", string(chain.rootPEM))
	bundleCertFile := pqtest.TempFile(t, "bundle.crt", string(chain.rootPEM)+string(chain.intermediatePEM))

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
				rootcert:   rootCertFile,
				serverCert: chain.serverTLSCert,
			},
			{
				name:       "verify-full full chain root only",
				sslmode:    "verify-full",
				rootcert:   rootCertFile,
				serverCert: chain.serverTLSCert,
			},

			// Server sends only leaf, sslrootcert has root+intermediate bundle.
			{
				name:       "verify-ca leaf only bundle rootcert",
				sslmode:    "verify-ca",
				rootcert:   bundleCertFile,
				serverCert: serverCertLeafOnly,
			},
			{
				name:       "verify-full leaf only bundle rootcert",
				sslmode:    "verify-full",
				rootcert:   bundleCertFile,
				serverCert: serverCertLeafOnly,
			},

			// Server sends only leaf, sslrootcert has root only — can't build chain.
			{
				name:       "verify-ca leaf only root only fails",
				sslmode:    "verify-ca",
				rootcert:   rootCertFile,
				serverCert: serverCertLeafOnly,
				wantErr:    true,
			},
			{
				name:       "verify-full leaf only root only fails",
				sslmode:    "verify-full",
				rootcert:   rootCertFile,
				serverCert: serverCertLeafOnly,
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
		serverCAs := x509.NewCertPool()
		serverCAs.AppendCertsFromPEM(chain.rootPEM)

		clientCertFile := pqtest.TempFile(t, "client.crt", string(chain.clientCertPEM))
		clientKeyFile := pqtest.TempFile(t, "client.key", string(chain.clientKeyPEM))
		if err := os.Chmod(clientKeyFile, 0600); err != nil {
			t.Fatal(err)
		}

		port, errCh := mockPostgresSSLServer(t, mockSSLServerOpts{
			serverCert: chain.serverTLSCert,
			clientCAs:  serverCAs,
		})

		dsn := fmt.Sprintf(
			"host=127.0.0.1 port=%s sslmode=verify-ca sslrootcert=%s sslcert=%s sslkey=%s user=test dbname=test connect_timeout=5",
			port, bundleCertFile, clientCertFile, clientKeyFile)

		err := pingMockServer(t, dsn, port, errCh)
		if err != nil {
			t.Fatalf("client cert with intermediate CA failed: %s", err)
		}
	})
}
