//go:build !go1.20

package pqtest

import (
	"crypto/x509"
)

// InvalidCertificate reports if this error is an "invalid certificate" error.
func InvalidCertificate(err error) bool {
	if err == nil {
		return false
	}

	// Error specific to MacOS when certificate is missing SCT, see
	// https://github.com/golang/go/issues/51991
	// Validating such certificate always results with this error first
	if err.Error() == "x509: “postgres” certificate is not standards compliant" {
		return true
	}
	switch err.(type) {
	case x509.UnknownAuthorityError, x509.HostnameError:
		return true
	}
	return false
}
