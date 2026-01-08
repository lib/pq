//go:build go1.20

package pqtest

import (
	"crypto/tls"
	"crypto/x509"
)

// InvalidCertificate reports if this error is an "invalid certificate" error.
func InvalidCertificate(err error) bool {
	switch err.(type) {
	case x509.UnknownAuthorityError, x509.HostnameError, *tls.CertificateVerificationError:
		return true
	}
	return false
}
