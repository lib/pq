package pq

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"time"
)

type tlsConfWithCrl struct {
	tls.Config

	crl *pkix.CertificateList
}

// ssl generates a function to upgrade a net.Conn based on the "sslmode" and
// related settings. The function is nil when no upgrade should take place.
func ssl(o values) (func(net.Conn) (net.Conn, error), error) {
	verifyCaOnly := false
	tlsConf := tlsConfWithCrl{}
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
		return nil, fmterrorf(`unsupported sslmode %q; only "require" (default), "verify-full", "verify-ca", and "disable" supported`, mode)
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
		client := tls.Client(conn, &tlsConf.Config)
		if err := sslVerifyExtra(client, &tlsConf, verifyCaOnly); err != nil {
			return nil, err
		}
		return client, nil
	}, nil
}

// sslClientCertificates adds the certificate specified in the "sslcert" and
// "sslkey" settings, or if they aren't set, from the .postgresql directory
// in the user's home directory. The configured files must exist and have
// the correct permissions.
func sslClientCertificates(tlsConf *tlsConfWithCrl, o values) error {
	sslinline := o["sslinline"]
	if sslinline == "true" {
		cert, err := tls.X509KeyPair([]byte(o["sslcert"]), []byte(o["sslkey"]))
		if err != nil {
			return err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
		return nil
	}

	// user.Current() might fail when cross-compiling. We have to ignore the
	// error and continue without home directory defaults, since we wouldn't
	// know from where to load them.
	user, _ := user.Current()

	// Load CRL, https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L974
	sslcrl := o["sslcrl"]
	if len(sslcrl) == 0 && user != nil {
		sslcrl = filepath.Join(user.HomeDir, ".postgresql", "root.crl")
	}
	if len(sslcrl) > 0 {
		crlcontent, err := ioutil.ReadFile(sslcrl)
		if err != nil && !os.IsNotExist(err) { // https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1002
			return err
		} else if err == nil {
			tlsConf.crl, err = x509.ParseCRL(crlcontent)
			if err != nil {
				return err
			}
		}
	}

	// In libpq, the client certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1036-L1037
	sslcert := o["sslcert"]
	if len(sslcert) == 0 && user != nil {
		sslcert = filepath.Join(user.HomeDir, ".postgresql", "postgresql.crt")
	}
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1045
	if len(sslcert) == 0 {
		return nil
	}
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1050:L1054
	if _, err := os.Stat(sslcert); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	// In libpq, the ssl key is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L1123-L1222
	sslkey := o["sslkey"]
	if len(sslkey) == 0 && user != nil {
		sslkey = filepath.Join(user.HomeDir, ".postgresql", "postgresql.key")
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
func sslCertificateAuthority(tlsConf *tlsConfWithCrl, o values) error {
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
			cert, err = ioutil.ReadFile(sslrootcert)
			if err != nil {
				return err
			}
		}

		if !tlsConf.RootCAs.AppendCertsFromPEM(cert) {
			return fmterrorf("couldn't parse pem in sslrootcert")
		}
	}

	return nil
}

// sslVerifyExtra carries out a TLS handshake to the server and
// carries out extra verification that Go's TLS package doesn't do:
//   * if verifyCaOnly is true, verifies the presented certificate against the CA,
//     i.e. the one specified in sslrootcert or the system CA if sslrootcert was not specified.
//   * verifies the PeerCertificates against CRL if sslcrl was specified
func sslVerifyExtra(client *tls.Conn, tlsConf *tlsConfWithCrl, verifyCaOnly bool) error {
	err := client.Handshake()
	if err != nil {
		return err
	}

	state := client.ConnectionState()
	if verifyCaOnly {
		opts := x509.VerifyOptions{
			DNSName:       client.ConnectionState().ServerName,
			Intermediates: x509.NewCertPool(),
			Roots:         tlsConf.RootCAs,
		}
		for i, cert := range state.PeerCertificates {
			if i == 0 {
				continue
			}
			opts.Intermediates.AddCert(cert)
		}

		state.VerifiedChains, err = state.PeerCertificates[0].Verify(opts)
		if err != nil {
			return err
		}
	}

	if crl := tlsConf.crl; crl != nil {
		if crl.HasExpired(time.Now()) {
			return fmterrorf("sslcrl has expired on %v", crl.TBSCertList.NextUpdate)
		}

		crlVerified := false
		crlIssuer := crl.TBSCertList.Issuer.String()

	VerifiedChainLoop:
		for _, chain := range state.VerifiedChains {
			for i := len(chain) - 1; i >= 0; i-- {
				cert := chain[i]
				if cert.Subject.ToRDNSequence().String() != crlIssuer {
					continue
				}

				if err := cert.CheckCRLSignature(crl); err != nil {
					return fmterrorf("sslcrl failed to verify with cert subject %s: %w", cert.Subject.String(), err)
				}
				crlVerified = true
				break VerifiedChainLoop
			}
		}

		if !crlVerified {
			return fmterrorf("sslcrl failed to verify with all root certificates.")
		}

		for _, cert := range state.PeerCertificates {
			for _, revoked := range crl.TBSCertList.RevokedCertificates {
				if cert.SerialNumber.Cmp(revoked.SerialNumber) == 0 {
					return fmterrorf("certificate %s was revoked at %v", cert.SerialNumber.String(), revoked.RevocationTime)
				}
			}
		}
	}

	return nil
}
