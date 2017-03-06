// +build linux,cgo darwin,cgo

package pq

import (
	"fmt"
	"os"

	"github.com/apcera/gssapi"
)

type gssctx struct {
	lib        *gssapi.Lib
	ctx        *gssapi.CtxId
	targetName *gssapi.Name
}

func (cn *conn) gssapiStart(o values) {
	var err error
	cn.gss.lib, err = gssapi.Load(&gssapi.Options{})
	if err != nil {
		errorf("error loading gssapi: %v", err)
	}
	var nameBuf *gssapi.Buffer
	nameBuf, err = cn.gss.lib.MakeBufferString(fmt.Sprintf("%s@%s", o.Get("krbsrvname"), o.Get("host")))
	if err != nil {
		errorf("error allocating buffer: %v", err)
	}
	defer nameBuf.Release()

	cn.gss.targetName, err = nameBuf.Name(cn.gss.lib.GSS_C_NT_HOSTBASED_SERVICE)
	if err != nil {
		errorf("error importing gss name: %v", err)
	}
	cn.gss.ctx = cn.gss.lib.GSS_C_NO_CONTEXT
	cn.gssapiContinue()

	t, r := cn.recv()
	if t != 'R' {
		errorf("unexpected gssapi response: %q", t)
	}
	subcode := r.int32()
	if subcode == 8 {
		// TODO: read the body into a gss input token and call gssapiContinue again
		errorf("two-step gssapi authentication is not implemented")
	} else if subcode != 0 {
		errorf("unknown authentication response: %d", subcode)
	}

}

func (cn *conn) gssapiContinue() {
	var tokenOut *gssapi.Buffer
	var err error
	cn.gss.ctx, _, tokenOut, _, _, err = cn.gss.lib.InitSecContext(
		cn.gss.lib.GSS_C_NO_CREDENTIAL,
		cn.gss.ctx,
		cn.gss.targetName,
		cn.gss.lib.GSS_C_NO_OID,
		0,
		0,
		cn.gss.lib.GSS_C_NO_CHANNEL_BINDINGS,
		cn.gss.lib.GSS_C_NO_BUFFER, // TODO: for two-step gssapi, use the input token
	)
	if cn.gss.ctx != nil {
		//	TODO: free in token
	}
	defer tokenOut.Release()
	if tokenOut.Length() > 0 {
		w := cn.writeBuf('p')
		w.bytes(tokenOut.Bytes())
		cn.send(w)
	}
	if err != nil {
		gssErr, ok := err.(*gssapi.Error)
		if ok {
			if !gssErr.Major.ContinueNeeded() {
				cn.gss.targetName.Release()
				if cn.gss.ctx != nil {
					cn.gss.ctx.DeleteSecContext()
				}
				errorf("error initializing gss security context: %v", err)
			} else {
				fmt.Fprintln(os.Stderr, "gssapi continue")
			}
		} else {
			errorf("error initializing gss security context: %v", err)
		}
	} else {
		cn.gss.targetName.Release()
	}
}
