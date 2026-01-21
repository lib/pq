package pq

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/lib/pq/internal/pqutil"
)

// Registry for custom tls.Configs
var (
	tlsConfs   = make(map[string]*tls.Config)
	tlsConfsMu sync.RWMutex
)

// RegisterTLSConfig registers a custom [tls.Config]. They are used by using
// sslmode=pqgo-«key» in the connection string.
//
// Set the config to nil to remove a configuration.
func RegisterTLSConfig(key string, config *tls.Config) error {
	key = strings.TrimPrefix(key, "pqgo-")
	if config == nil {
		tlsConfsMu.Lock()
		delete(tlsConfs, key)
		tlsConfsMu.Unlock()
		return nil
	}

	tlsConfsMu.Lock()
	tlsConfs[key] = config
	tlsConfsMu.Unlock()
	return nil
}

func hasTLSConfig(key string) bool {
	tlsConfsMu.RLock()
	defer tlsConfsMu.RUnlock()
	_, ok := tlsConfs[key]
	return ok
}

func getTLSConfigClone(key string) *tls.Config {
	tlsConfsMu.RLock()
	defer tlsConfsMu.RUnlock()
	if v, ok := tlsConfs[key]; ok {
		return v.Clone()
	}
	return nil
}

// ssl generates a function to upgrade a net.Conn based on the "sslmode" and
// related settings. The function is nil when no upgrade should take place.
func ssl(cfg Config) (func(net.Conn) (net.Conn, error), error) {
	var (
		verifyCaOnly = false
		tlsConf      = &tls.Config{}
		mode         = cfg.SSLMode
	)
	switch {
	// "require" is the default.
	case mode == "" || mode == SSLModeRequire:
		// We must skip TLS's own verification since it requires full
		// verification since Go 1.3.
		tlsConf.InsecureSkipVerify = true

		// From http://www.postgresql.org/docs/current/static/libpq-ssl.html:
		//
		// Note: For backwards compatibility with earlier versions of
		// PostgreSQL, if a root CA file exists, the behavior of
		// sslmode=require will be the same as that of verify-ca, meaning the
		// server certificate is validated against the CA. Relying on this
		// behavior is discouraged, and applications that need certificate
		// validation should always use verify-ca or verify-full.
		if cfg.SSLRootCert != "" {
			if _, err := os.Stat(cfg.SSLRootCert); err == nil {
				verifyCaOnly = true
			} else {
				cfg.SSLRootCert = ""
			}
		}
	case mode == SSLModeVerifyCA:
		// We must skip TLS's own verification since it requires full
		// verification since Go 1.3.
		tlsConf.InsecureSkipVerify = true
		verifyCaOnly = true
	case mode == SSLModeVerifyFull:
		tlsConf.ServerName = cfg.Host
	case mode == SSLModeDisable:
		return nil, nil
	case strings.HasPrefix(string(mode), "pqgo-"):
		tlsConf = getTLSConfigClone(string(mode[5:]))
		if tlsConf == nil {
			return nil, fmt.Errorf(`pq: unknown custom sslmode %q`, mode)
		}
	default:
		return nil, fmt.Errorf(
			`pq: unsupported sslmode %q; only "require" (default), "verify-full", "verify-ca", and "disable" supported`,
			mode)
	}

	// Set Server Name Indication (SNI), if enabled by connection parameters.
	if cfg.SSLSNI {
		// RFC 6066 asks to not set SNI if the host is a literal IP address (IPv4
		// or IPv6). This check is coded already crypto.tls.hostnameInSNI, so
		// just always set ServerName here and let crypto/tls do the filtering.
		tlsConf.ServerName = cfg.Host
	}

	err := sslClientCertificates(tlsConf, cfg)
	if err != nil {
		return nil, err
	}
	err = sslCertificateAuthority(tlsConf, cfg)
	if err != nil {
		return nil, err
	}

	// Accept renegotiation requests initiated by the backend.
	//
	// Renegotiation was deprecated then removed from PostgreSQL 9.5, but
	// the default configuration of older versions has it enabled. Redshift
	// also initiates renegotiations and cannot be reconfigured.
	tlsConf.Renegotiation = tls.RenegotiateFreelyAsClient

	return func(conn net.Conn) (net.Conn, error) {
		client := tls.Client(conn, tlsConf)
		if verifyCaOnly {
			err := client.Handshake()
			if err != nil {
				return client, err
			}
			var (
				certs = client.ConnectionState().PeerCertificates
				opts  = x509.VerifyOptions{Intermediates: x509.NewCertPool(), Roots: tlsConf.RootCAs}
			)
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err = certs[0].Verify(opts)
			return client, err
		}
		return client, nil
	}, nil
}

// sslClientCertificates adds the certificate specified in the "sslcert" and
//
// "sslkey" settings, or if they aren't set, from the .postgresql directory
// in the user's home directory. The configured files must exist and have
// the correct permissions.
func sslClientCertificates(tlsConf *tls.Config, cfg Config) error {
	if cfg.SSLInline {
		cert, err := tls.X509KeyPair([]byte(cfg.SSLCert), []byte(cfg.SSLKey))
		if err != nil {
			return err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
		return nil
	}

	home := pqutil.Home()

	// In libpq, the client certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1036-L1037
	sslcert := cfg.SSLCert
	if len(sslcert) == 0 && home != "" {
		if runtime.GOOS == "windows" {
			sslcert = filepath.Join(sslcert, "postgresql.crt")
		} else {
			sslcert = filepath.Join(home, ".postgresql/postgresql.crt")
		}
	}
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1045
	if len(sslcert) == 0 {
		return nil
	}
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1050:L1054
	_, err := os.Stat(sslcert)
	if err != nil {
		perr := new(os.PathError)
		if errors.As(err, &perr) && (perr.Err == syscall.ENOENT || perr.Err == syscall.ENOTDIR) {
			return nil
		}
		return err
	}

	// In libpq, the ssl key is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1123-L1222
	sslkey := cfg.SSLKey
	if len(sslkey) == 0 && home != "" {
		if runtime.GOOS == "windows" {
			sslkey = filepath.Join(home, "postgresql.key")
		} else {
			sslkey = filepath.Join(home, ".postgresql/postgresql.key")
		}
	}

	if len(sslkey) > 0 {
		err := pqutil.SSLKeyPermissions(sslkey)
		if err != nil {
			return err
		}
	}

	cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
	if err != nil {
		return err
	}

	tlsConf.Certificates = []tls.Certificate{cert}
	return nil
}

// sslCertificateAuthority adds the RootCA specified in the "sslrootcert" setting.
func sslCertificateAuthority(tlsConf *tls.Config, cfg Config) error {
	// In libpq, the root certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L950-L951
	if sslrootcert := cfg.SSLRootCert; len(sslrootcert) > 0 {
		tlsConf.RootCAs = x509.NewCertPool()

		var cert []byte
		if cfg.SSLInline {
			cert = []byte(sslrootcert)
		} else {
			var err error
			cert, err = os.ReadFile(sslrootcert)
			if err != nil {
				return err
			}
		}

		if !tlsConf.RootCAs.AppendCertsFromPEM(cert) {
			return errors.New("pq: couldn't parse pem in sslrootcert")
		}
	}

	return nil
}
