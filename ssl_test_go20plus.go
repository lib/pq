//go:build go1.20
// +build go1.20

package pq

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

const (
	// Error specific to MacOS when certificate is missing SCT, see
	// https://github.com/golang/go/issues/51991
	// Validating such certificate always results with this error first
	errMacOsCertificateNotCompliant = `x509: “postgres” certificate is not standards compliant`
)

func assertInvalidCertificate(t *testing.T, err error) {
	switch x := err.(type) {
	case x509.UnknownAuthorityError:
		break
	case x509.HostnameError:
		break
	case *tls.CertificateVerificationError:
		break
	default:
		t.Fatalf("expected x509.UnknownAuthorityError, x509.HostnameError or tls.CertificateVerificationError (go 1.20+), got %#+v", x)
	}
}
