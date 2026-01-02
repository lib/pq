//go:build !go1.20

package pq

import (
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
	t.Helper()

	if err.Error() == errMacOsCertificateNotCompliant {
		return
	}
	switch x := err.(type) {
	case x509.UnknownAuthorityError, x509.HostnameError:
		break
	default:
		t.Fatalf("wrong error type %T: %[1]s", x)
	}
}
