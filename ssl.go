package pq

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"sync"
)

// To avoid allocating the map if we never use ssl
var configMapOnce sync.Once
var configMapMu sync.Mutex
var configMap map[string]*ssldata

type ssldata struct {
	Conf         *tls.Config
	VerifyCAOnly bool
}

func getTLSConf(o values) (*ssldata, error) {
	verifyCaOnly := false
	configMapOnce.Do(func() {
		configMap = make(map[string]*ssldata)
	})
	// this function modifies o, so take the hash before any modifications are
	// made
	hash := string(o.Hash())
	// This pseudo-parameter is not recognized by the PostgreSQL server, so let's delete it after use.
	configMapMu.Lock()
	conf, ok := configMap[hash]
	configMapMu.Unlock()
	if ok {
		return conf, nil
	}
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

	// This pseudo-parameter is not recognized by the PostgreSQL server, so let's delete it after use.
	delete(o, "sslinline")

	// Accept renegotiation requests initiated by the backend.
	//
	// Renegotiation was deprecated then removed from PostgreSQL 9.5, but
	// the default configuration of older versions has it enabled. Redshift
	// also initiates renegotiations and cannot be reconfigured.
	tlsConf.Renegotiation = tls.RenegotiateFreelyAsClient

	data := &ssldata{&tlsConf, verifyCaOnly}
	configMapMu.Lock()
	configMap[hash] = data
	configMapMu.Unlock()
	return data, nil
}

// ssl generates a function to upgrade a net.Conn based on the "sslmode" and
// related settings. The function is nil when no upgrade should take place.
func ssl(o values) (func(net.Conn) (net.Conn, error), error) {
	data, err := getTLSConf(o)
	if data == nil && err == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return func(conn net.Conn) (net.Conn, error) {
		client := tls.Client(conn, data.Conf)
		if data.VerifyCAOnly {
			err := sslVerifyCertificateAuthority(client, data.Conf)
			if err != nil {
				return nil, err
			}
		}
		return client, nil
	}, nil
}

// sslClientCertificates adds the certificate specified in the "sslcert" and
// "sslkey" settings, or if they aren't set, from the .postgresql directory
// in the user's home directory. The configured files must exist and have
// the correct permissions.
func sslClientCertificates(tlsConf *tls.Config, o values) error {
	if o["sslinline"] == "true" {
		cert, err := tls.X509KeyPair([]byte(o["sslcert"]), []byte(o["sslkey"]))
		// Clear out these params, in case they were to be sent to the PostgreSQL server by mistake
		o["sslcert"] = ""
		o["sslkey"] = ""
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
func sslCertificateAuthority(tlsConf *tls.Config, o values) error {
	// In libpq, the root certificate is only loaded if the setting is not blank.
	//
	// https://github.com/postgres/postgres/blob/REL9_6_2/src/interfaces/libpq/fe-secure-openssl.c#L950-L951
	if sslrootcert := o["sslrootcert"]; len(sslrootcert) > 0 {
		tlsConf.RootCAs = x509.NewCertPool()

		sslinline := o["sslinline"]

		var cert []byte
		if sslinline == "true" {
			// // Clear out this param, in case it were to be sent to the PostgreSQL server by mistake
			o["sslrootcert"] = ""
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

// sslVerifyCertificateAuthority carries out a TLS handshake to the server and
// verifies the presented certificate against the CA, i.e. the one specified in
// sslrootcert or the system CA if sslrootcert was not specified.
func sslVerifyCertificateAuthority(client *tls.Conn, tlsConf *tls.Config) error {
	err := client.Handshake()
	if err != nil {
		return err
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
	return err
}
