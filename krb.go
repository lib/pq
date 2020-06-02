package pq

// A function that creates a GSS authentication provider,
// for use with RegisterGSSProvider.
type NewGSSFunc func() (Gss, error)

var newGss NewGSSFunc

// Register the function for creating a GSS authentication provider.
// For example, if you need to use Kerberos to authenticate with your server,
// add this to your main package:
//
//	import "github.com/lib/pq/auth/kerberos"
//	
//	func init() {
//		pq.RegisterGSSProvider(func() (pq.Gss, error) { return kerberos.NewGSS() })
//	}
func RegisterGSSProvider(newGssArg NewGSSFunc) {
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
