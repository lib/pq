// +build !linux,!darwin !cgo

package pq

type gssctx struct {
}

func (cn *conn) gssapiStart(o values) {
	errorf("gssapi authentication is not available")
}

func (cn *conn) gssapiContinue() {
}
