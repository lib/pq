package pq

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
)

// ssl generates a function to upgrade a net.Conn based on the "sslmode" and
// related settings. The function is nil when no upgrade should take place.
func ssl(o values) func(net.Conn) net.Conn {
	verifyCaOnly := false
	tlsConf := tls.Config{}
	switch mode := o.Get("sslmode"); mode {
	// "require" is the default.
	case "", "require":
		// We must skip TLS's own verification since it requires full
		// verification since Go 1.3.
		tlsConf.InsecureSkipVerify = true

		// From http://www.postgresql.org/docs/current/static/libpq-ssl.html:
		// Note: For backwards compatibility with earlier versions of PostgreSQL, if a
		// root CA file exists, the behavior of sslmode=require will be the same as
		// that of verify-ca, meaning the server certificate is validated against the
		// CA. Relying on this behavior is discouraged, and applications that need
		// certificate validation should always use verify-ca or verify-full.
		if _, err := os.Stat(o.Get("sslrootcert")); err == nil {
			verifyCaOnly = true
		} else {
			o.Set("sslrootcert", "")
		}
	case "verify-ca":
		// We must skip TLS's own verification since it requires full
		// verification since Go 1.3.
		tlsConf.InsecureSkipVerify = true
		verifyCaOnly = true
	case "verify-full":
		tlsConf.ServerName = o.Get("host")
	case "disable":
		return nil
	default:
		errorf(`unsupported sslmode %q; only "require" (default), "verify-full", "verify-ca", and "disable" supported`, mode)
	}

	sslClientCertificates(&tlsConf, o)
	sslCertificateAuthority(&tlsConf, o)
	sslRenegotiation(&tlsConf)

	return func(conn net.Conn) net.Conn {
		client := tls.Client(conn, &tlsConf)
		if verifyCaOnly {
			sslVerifyCertificateAuthority(client, &tlsConf)
		}
		return client
	}
}

// sslClientCertificates adds the certificate specified in the "sslcert" and
// "sslkey" settings, or if they aren't set, from the .postgresql directory
// in the user's home directory. The configured files must exist and have
// the correct permissions.
func sslClientCertificates(tlsConf *tls.Config, o values) {
	var missingOk bool

	sslkey := o.Get("sslkey")
	sslcert := o.Get("sslcert")
	if sslkey != "" && sslcert != "" {
		// If the user has set an sslkey and sslcert, they *must* exist.
		missingOk = false
	} else {
		// Automatically load certificates from ~/.postgresql.
		user, err := user.Current()
		if err != nil {
			// user.Current() might fail when cross-compiling.  We have to
			// ignore the error and continue without client certificates, since
			// we wouldn't know where to load them from.
			return
		}

		sslkey = filepath.Join(user.HomeDir, ".postgresql", "postgresql.key")
		sslcert = filepath.Join(user.HomeDir, ".postgresql", "postgresql.crt")
		missingOk = true
	}

	// Check that both files exist, and report the error or stop, depending on
	// which behaviour we want.  Note that we don't do any more extensive
	// checks than this (such as checking that the paths aren't directories);
	// LoadX509KeyPair() will take care of the rest.
	keyfinfo, err := os.Stat(sslkey)
	if err != nil && missingOk {
		return
	} else if err != nil {
		panic(err)
	}
	_, err = os.Stat(sslcert)
	if err != nil && missingOk {
		return
	} else if err != nil {
		panic(err)
	}

	// If we got this far, the key file must also have the correct permissions
	kmode := keyfinfo.Mode()
	if kmode != kmode&0600 {
		panic(ErrSSLKeyHasWorldPermissions)
	}

	cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
	if err != nil {
		panic(err)
	}
	tlsConf.Certificates = []tls.Certificate{cert}
}

// sslCertificateAuthority adds the RootCA specified in the "sslrootcert" setting.
func sslCertificateAuthority(tlsConf *tls.Config, o values) {
	if sslrootcert := o.Get("sslrootcert"); sslrootcert != "" {
		tlsConf.RootCAs = x509.NewCertPool()

		cert, err := ioutil.ReadFile(sslrootcert)
		if err != nil {
			panic(err)
		}

		ok := tlsConf.RootCAs.AppendCertsFromPEM(cert)
		if !ok {
			errorf("couldn't parse pem in sslrootcert")
		}
	}
}

// sslVerifyCertificateAuthority carries out a TLS handshake to the server and
// verifies the presented certificate against the CA, i.e. the one specified in
// sslrootcert or the system CA if sslrootcert was not specified.
func sslVerifyCertificateAuthority(client *tls.Conn, tlsConf *tls.Config) {
	err := client.Handshake()
	if err != nil {
		panic(err)
	}
	certs := client.ConnectionState().PeerCertificates
	opts := x509.VerifyOptions{
		DNSName:       client.ConnectionState().ServerName,
		Intermediates: x509.NewCertPool(),
		Roots:         tlsConf.RootCAs,
	}
	for i, cert := range certs {
		if i == 0 {
			continue
		}
		opts.Intermediates.AddCert(cert)
	}
	_, err = certs[0].Verify(opts)
	if err != nil {
		panic(err)
	}
}
