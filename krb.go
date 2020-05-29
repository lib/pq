package pq

// A function that creates a GSS authentication provider. You
// only need to care about this type if you are writing a GSS
// authentication provider.
type NewGSSFunc func() (Gss, error)

var newGss NewGSSFunc

// Register the function for creating a GSS authentication provider.
// You only need to care about this function if you are writing a
// GSS authentication provider.
func RegisterNewGSSFunc(newGssArg NewGSSFunc) {
	newGss = newGssArg
}

// An interface for providing GSSAPI authentication (e.g. Kerberos).
// You only need to care about this interface if you are writing a
// GSS authentication provider.
type Gss interface {
	GetInitToken(host string, service string) ([]byte, error)
	GetInitTokenFromSpn(spn string) ([]byte, error)
	Continue(inToken []byte) (done bool, outToken []byte, err error)
}
