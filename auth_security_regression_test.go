package pq

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/proto"
)

type securityRegressionGSS struct {
	initToken  []byte
	continueFn func([]byte) (bool, []byte, error)
}

func (g *securityRegressionGSS) GetInitToken(string, string) ([]byte, error) {
	return g.initToken, nil
}

func (g *securityRegressionGSS) GetInitTokenFromSpn(string) ([]byte, error) {
	return g.initToken, nil
}

func (g *securityRegressionGSS) Continue(token []byte) (bool, []byte, error) {
	if g.continueFn != nil {
		return g.continueFn(token)
	}
	return true, nil, nil
}

func securityRegressionRegisterGSS(t *testing.T, gss GSS) {
	t.Helper()
	old := newGss
	newGss = func() (GSS, error) { return gss, nil }
	t.Cleanup(func() { newGss = old })
}

func securityRegressionWriteAuth(f pqtest.Fake, cn net.Conn, code proto.AuthCode, payload string) {
	msg := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(msg, uint32(code))
	msg = append(msg, payload...)
	f.WriteMsg(cn, proto.AuthenticationRequest, string(msg))
}

func securityRegressionConnect(t *testing.T, dsn string) error {
	t.Helper()
	c, err := NewConnector(dsn + " connect_timeout=1")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cn, err := c.Connect(ctx)
	if err == nil {
		_ = cn.Close()
	}
	return err
}

func TestSecurityRegressionGSSRequiresCryptographicCompletion(t *testing.T) {
	gss := &securityRegressionGSS{initToken: []byte("client-init-token")}
	securityRegressionRegisterGSS(t, gss)

	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		defer cn.Close()
		_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, _, ok := f.ReadStartup(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqGSS, "")
		if _, _, ok := f.ReadMsg(cn); !ok {
			return
		}

		// A peer that possesses no service key cannot skip the continuation
		// exchange and authenticate itself merely by claiming success.
		securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
		f.WriteMsg(cn, proto.ReadyForQuery, "I")
	})
	defer f.Close()

	err := securityRegressionConnect(t, f.DSN()+" sslmode=disable user=test")
	if err == nil {
		t.Fatal("connection succeeded before GSS mutual authentication completed")
	}
}

func TestSecurityRegressionGSSContinuationErrorIsFatal(t *testing.T) {
	wantErr := errors.New("mutual authentication failed")
	gss := &securityRegressionGSS{
		initToken: []byte("client-init-token"),
		continueFn: func([]byte) (bool, []byte, error) {
			return false, nil, wantErr
		},
	}
	securityRegressionRegisterGSS(t, gss)

	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		defer cn.Close()
		_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, _, ok := f.ReadStartup(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqGSS, "")
		if _, _, ok := f.ReadMsg(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqGSSCont, "server-token")
		securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
		f.WriteMsg(cn, proto.ReadyForQuery, "I")
	})
	defer f.Close()

	err := securityRegressionConnect(t, f.DSN()+" sslmode=disable user=test")
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("GSS continuation error was discarded\nhave: %v\nwant: %v", err, wantErr)
	}
}

func TestSecurityRegressionRequireAuthPolicy(t *testing.T) {
	gss := &securityRegressionGSS{initToken: []byte("client-init-token")}
	securityRegressionRegisterGSS(t, gss)

	tests := []struct {
		name      string
		auth      proto.AuthCode
		require   string
		wantError bool
	}{
		{"positive md5 rejects GSS", proto.AuthReqGSS, "md5", true},
		{"negative gss rejects GSS", proto.AuthReqGSS, "!gss", true},
		{"negative password wins over any authentication", proto.AuthReqPassword, "!none,!password", true},
		{"negative md5 permits password", proto.AuthReqPassword, "!md5", false},
		{"negative md5 permits no authentication", proto.AuthReqOk, "!md5", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
				defer cn.Close()
				_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, _, ok := f.ReadStartup(cn); !ok {
					return
				}
				switch tt.auth {
				case proto.AuthReqOk:
					securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
				case proto.AuthReqPassword:
					securityRegressionWriteAuth(f, cn, proto.AuthReqPassword, "")
					if _, _, ok := f.ReadMsg(cn); !ok {
						return
					}
					securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
				case proto.AuthReqGSS:
					securityRegressionWriteAuth(f, cn, proto.AuthReqGSS, "")
					if _, _, ok := f.ReadMsg(cn); !ok {
						return
					}
					securityRegressionWriteAuth(f, cn, proto.AuthReqGSSCont, "server-token")
					securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
				default:
					panic("unsupported test authentication method")
				}
				f.WriteMsg(cn, proto.ReadyForQuery, "I")
			})
			defer f.Close()

			err := securityRegressionConnect(t, f.DSN()+" sslmode=disable user=test password=secret require_auth="+tt.require)
			if tt.wantError && err == nil {
				t.Fatal("connection succeeded with a prohibited authentication method")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("connection failed with a permitted authentication method: %v", err)
			}
		})
	}
}

func TestSecurityRegressionHostaddrUsesRemotePgpassIdentity(t *testing.T) {
	pqtest.Unsetenv(t, "PGHOST")
	passwordCh := make(chan string, 1)
	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		defer cn.Close()
		_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, _, ok := f.ReadStartup(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqPassword, "")
		_, password, ok := f.ReadMsg(cn)
		if !ok {
			return
		}
		passwordCh <- strings.TrimSuffix(string(password), "\x00")
		securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
		f.WriteMsg(cn, proto.ReadyForQuery, "I")
	})
	defer f.Close()

	passfile := pqtest.TempFile(t, "pgpass", fmt.Sprintf(
		"localhost:%s:database:user:LOCAL_SECRET\n%s:%s:database:user:REMOTE_SECRET\n",
		f.Port(), f.Host(), f.Port()))
	pqtest.Chmod(t, 0o600, passfile)
	dsn := fmt.Sprintf("hostaddr=%s port=%s dbname=database user=user passfile='%s' sslmode=disable",
		f.Host(), f.Port(), passfile)
	if err := securityRegressionConnect(t, dsn); err != nil {
		t.Fatal(err)
	}

	select {
	case password := <-passwordCh:
		if password != "REMOTE_SECRET" {
			t.Fatalf("hostaddr-only connection used the wrong pgpass identity\nhave: %q\nwant: %q", password, "REMOTE_SECRET")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for password message")
	}
}

func TestSecurityRegressionHostaddrRequiresHostnameForTLSAndGSS(t *testing.T) {
	pqtest.Unsetenv(t, "PGHOST")
	t.Run("verify-full", func(t *testing.T) {
		f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
			defer cn.Close()
			_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
			f.ReadStartup(cn)
		})
		defer f.Close()

		dsn := fmt.Sprintf("hostaddr=%s port=%s sslmode=verify-full user=test", f.Host(), f.Port())
		err := securityRegressionConnect(t, dsn)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "host") {
			t.Fatalf("verify-full without an explicit hostname did not return a hostname error: %v", err)
		}
	})

	t.Run("GSS", func(t *testing.T) {
		gss := &securityRegressionGSS{initToken: []byte("client-init-token")}
		securityRegressionRegisterGSS(t, gss)
		f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
			defer cn.Close()
			_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
			if _, _, ok := f.ReadStartup(cn); !ok {
				return
			}
			securityRegressionWriteAuth(f, cn, proto.AuthReqGSS, "")
			if _, _, ok := f.ReadMsg(cn); !ok {
				return
			}
			securityRegressionWriteAuth(f, cn, proto.AuthReqGSSCont, "server-token")
			securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
			f.WriteMsg(cn, proto.ReadyForQuery, "I")
		})
		defer f.Close()

		dsn := fmt.Sprintf("hostaddr=%s port=%s sslmode=disable user=test", f.Host(), f.Port())
		err := securityRegressionConnect(t, dsn)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "host") {
			t.Fatalf("GSS without an explicit hostname did not return a hostname error: %v", err)
		}
	})
}

func TestSecurityRegressionExplicitDSNOverridesService(t *testing.T) {
	serviceFile := filepath.Join(t.TempDir(), "pg_service.conf")
	if err := os.WriteFile(serviceFile, []byte("[security]\n"+
		"host=service.example\nsslmode=disable\nrequire_auth=none\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := newConfig(
		"service=security host=dsn.example sslmode=verify-full require_auth=scram-sha-256",
		[]string{"PGSERVICEFILE=" + serviceFile, "PGUSER=test"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "dsn.example" {
		t.Errorf("service overrode explicit host: %q", cfg.Host)
	}
	if cfg.SSLMode != SSLModeVerifyFull {
		t.Errorf("service overrode explicit sslmode: %q", cfg.SSLMode)
	}
	if cfg.RequireAuth.String() != string(RequireAuthScramSHA256) {
		t.Errorf("service overrode explicit require_auth: %q", cfg.RequireAuth)
	}
}

func TestSecurityRegressionPortListRequiresMatchingHostCount(t *testing.T) {
	_, err := newConfig(
		"host=one.example port=5432,6543 sslmode=disable",
		[]string{"PGUSER=test"},
	)
	if err == nil {
		t.Fatal("one host with multiple ports was accepted and the extra port was silently discarded")
	}
}

func TestSecurityRegressionVerifyCARequiresRootCertificate(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg, err := newConfig("host=example.test sslmode=verify-ca sslrootcert=''", []string{"PGUSER=test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ssl(cfg, cfg.SSLMode); err == nil {
		t.Fatal("sslmode=verify-ca accepted an empty root certificate and would fall back to system roots")
	}
}

func TestSecurityRegressionNewConnectorConfigValidatesTLS(t *testing.T) {
	base, err := newConfig("host=localhost sslmode=disable", []string{"PGUSER=test"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"system roots with require", func(cfg *Config) {
			cfg.SSLRootCert = "system"
			cfg.SSLMode = SSLModeRequire
		}},
		{"unknown sslmode", func(cfg *Config) {
			cfg.SSLMode = SSLMode("not-a-real-mode")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base.Clone()
			tt.mutate(&cfg)
			if _, err := NewConnectorConfig(cfg); err == nil {
				t.Fatal("NewConnectorConfig accepted an invalid programmatic configuration")
			}
		})
	}
}

func TestSecurityRegressionRegisteredTLSVersionBoundsArePreserved(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	const key = "security-regression-version-bounds"
	if err := RegisterTLSConfig(key, &tls.Config{
		InsecureSkipVerify: true, // Test-only certificate.
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = RegisterTLSConfig(key, nil) })

	upgrade, err := ssl(Config{}, SSLMode("pqgo-"+key))
	if err != nil {
		t.Fatal(err)
	}
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()
	deadline := time.Now().Add(2 * time.Second)
	_ = clientRaw.SetDeadline(deadline)
	_ = serverRaw.SetDeadline(deadline)

	cert, err := tls.LoadX509KeyPair("testdata/ssl/server.crt", "testdata/ssl/server.key")
	if err != nil {
		t.Fatal(err)
	}
	server := tls.Server(serverRaw, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	})
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Handshake() }()

	clientConn, err := upgrade(clientRaw)
	if err != nil {
		t.Fatal(err)
	}
	client, ok := clientConn.(*tls.Conn)
	if !ok {
		t.Fatalf("TLS upgrader returned %T", clientConn)
	}
	clientErr := client.Handshake()
	<-serverErr
	if clientErr == nil {
		t.Fatal("registered TLS 1.3 bounds were overwritten; connection negotiated TLS 1.2")
	}
}

func TestSecurityRegressionVerifyFullWithoutSNIStillVerifiesHostname(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cert, err := tls.LoadX509KeyPair("testdata/ssl/server.crt", "testdata/ssl/server.key")
	if err != nil {
		t.Fatal(err)
	}

	handshake := func(t *testing.T, host string) (string, error) {
		t.Helper()
		cfg := Config{
			Host:        host,
			SSLMode:     SSLModeVerifyFull,
			SSLRootCert: "testdata/ssl/root.crt",
			SSLSNI:      false,
		}
		upgrade, err := ssl(cfg, cfg.SSLMode)
		if err != nil {
			t.Fatal(err)
		}

		clientRaw, serverRaw := net.Pipe()
		defer clientRaw.Close()
		defer serverRaw.Close()
		deadline := time.Now().Add(2 * time.Second)
		_ = clientRaw.SetDeadline(deadline)
		_ = serverRaw.SetDeadline(deadline)

		var sni string
		server := tls.Server(serverRaw, &tls.Config{
			Certificates: []tls.Certificate{cert},
			GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				sni = hello.ServerName
				return nil, nil
			},
		})
		serverErr := make(chan error, 1)
		go func() { serverErr <- server.Handshake() }()

		clientConn, err := upgrade(clientRaw)
		if err != nil {
			t.Fatal(err)
		}
		client, ok := clientConn.(*tls.Conn)
		if !ok {
			t.Fatalf("TLS upgrader returned %T", clientConn)
		}
		clientErr := client.Handshake()
		<-serverErr
		return sni, clientErr
	}

	t.Run("matching hostname", func(t *testing.T) {
		sni, err := handshake(t, "postgres")
		if err != nil {
			t.Fatalf("verify-full rejected the certificate's hostname: %v", err)
		}
		if sni != "" {
			t.Fatalf("sslsni=0 sent SNI %q", sni)
		}
	})

	t.Run("mismatched hostname", func(t *testing.T) {
		sni, err := handshake(t, "postgres-invalid")
		if err == nil {
			t.Fatal("verify-full accepted a certificate for a different hostname")
		}
		if sni != "" {
			t.Fatalf("sslsni=0 sent SNI %q", sni)
		}
	})
}

func TestSecurityRegressionDebugRedactsCredentialsAndAuthPayloads(t *testing.T) {
	gss := &securityRegressionGSS{initToken: []byte("GSS_AUTH_TOKEN_MUST_NOT_BE_LOGGED")}
	securityRegressionRegisterGSS(t, gss)
	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		defer cn.Close()
		_ = cn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, _, ok := f.ReadStartup(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqGSS, "")
		if _, _, ok := f.ReadMsg(cn); !ok {
			return
		}
		securityRegressionWriteAuth(f, cn, proto.AuthReqGSSCont, "server-token")
		securityRegressionWriteAuth(f, cn, proto.AuthReqOk, "")
		f.WriteMsg(cn, proto.ReadyForQuery, "I")
	})
	defer f.Close()

	capture, err := os.CreateTemp(t.TempDir(), "pq-debug")
	if err != nil {
		t.Fatal(err)
	}
	oldStderr, oldDebug := os.Stderr, debugProto
	os.Stderr, debugProto = capture, true
	t.Cleanup(func() {
		os.Stderr, debugProto = oldStderr, oldDebug
		_ = capture.Close()
	})

	err = securityRegressionConnect(t, f.DSN()+" sslmode=disable user=test password=CONFIG_PASSWORD_MUST_NOT_BE_LOGGED")
	os.Stderr, debugProto = oldStderr, oldDebug
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"CONFIG_PASSWORD_MUST_NOT_BE_LOGGED", "GSS_AUTH_TOKEN_MUST_NOT_BE_LOGGED"} {
		if strings.Contains(string(output), secret) {
			t.Errorf("PQGO_DEBUG exposed secret %q in output:\n%s", secret, output)
		}
	}
}
