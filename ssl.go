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
	"syscall"
)

// ssl generates a function to upgrade a net.Conn based on the "sslmode" and
// related settings. The function is nil when no upgrade should take place.
func ssl(o values) (func(net.Conn) (net.Conn, error), error) {
	verifyCaOnly := false
	tlsConf := tls.Config{}
	switch mode := o["sslmode"]; mode {
	// "require" is the default.
	case "", "require":
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
		if sslrootcert, ok := o["sslrootcert"]; ok {
			if _, err := os.Stat(sslrootcert); err == nil {
				verifyCaOnly = true
			} else {
				delete(o, "sslrootcert")
			}
		}
	case "verify-ca":
		// We must skip TLS's own verification since it requires full
		// verification since Go 1.3.
		tlsConf.InsecureSkipVerify = true
		verifyCaOnly = true
	case "verify-full":
		tlsConf.ServerName = o["host"]
	case "disable":
		return nil, nil
	default:
		return nil, fmt.Errorf(
			`pq: unsupported sslmode %q; only "require" (default), "verify-full", "verify-ca", and "disable" supported`,
			mode)
	}

	// Set Server Name Indication (SNI), if enabled by connection parameters.
	// By default SNI is on, any value which is not starting with "1" disables
	// SNI -- that is the same check vanilla libpq uses.
	if sslsni := o["sslsni"]; sslsni == "" || strings.HasPrefix(sslsni, "1") {
		// RFC 6066 asks to not set SNI if the host is a literal IP address (IPv4
		// or IPv6). This check is coded already crypto.tls.hostnameInSNI, so
		// just always set ServerName here and let crypto/tls do the filtering.
		tlsConf.ServerName = o["host"]
	}

	err := sslClientCertificates(&tlsConf, o)
	if err != nil {
		return nil, err
	}
	err = sslCertificateAuthority(&tlsConf, o)
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
		client := tls.Client(conn, &tlsConf)
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
// "sslkey" settings, or if they aren't set, from the .postgresql directory
// in the user's home directory. The configured files must exist and have
// the correct permissions.
func sslClientCertificates(tlsConf *tls.Config, o values) error {
	sslinline := o["sslinline"]
	if sslinline == "true" {
		cert, err := tls.X509KeyPair([]byte(o["sslcert"]), []byte(o["sslkey"]))
		if err != nil {
			return err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
		return nil
	}

	home := getHome()

	// In libpq, the client certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1036-L1037
	sslcert := o["sslcert"]
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
	sslkey := o["sslkey"]
	if len(sslkey) == 0 && home != "" {
		if runtime.GOOS == "windows" {
			sslkey = filepath.Join(home, "postgresql.key")
		} else {
			sslkey = filepath.Join(home, ".postgresql/postgresql.key")
		}
	}

	if len(sslkey) > 0 {
		if err := sslKeyPermissions(sslkey); err != nil {
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
func sslCertificateAuthority(tlsConf *tls.Config, o values) error {
	// In libpq, the root certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L950-L951
	if sslrootcert := o["sslrootcert"]; len(sslrootcert) > 0 {
		tlsConf.RootCAs = x509.NewCertPool()

		sslinline := o["sslinline"]

		var cert []byte
		if sslinline == "true" {
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

// sslnegotiation returns true if we should negotiate SSL.
// returns false if there should be no negotiation and we should upgrade immediately.
func sslnegotiation(o values) bool {
	if v, ok := o["sslnegotiation"]; ok && v == "direct" {
		return false
	}
	return true
}
