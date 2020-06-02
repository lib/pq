// +build windows

package kerberos

import (
	"github.com/alexbrainman/sspi"
	"github.com/alexbrainman/sspi/negotiate"
)

// Implements the pq.Gss interface
type Gss struct {
	creds *sspi.Credentials
	ctx   *negotiate.ClientContext
}

func NewGSS() (*Gss, error) {
	g := &Gss{}
	err := g.init()

	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *Gss) init() error {
	creds, err := negotiate.AcquireCurrentUserCredentials()
	if err != nil {
		return err
	}

	g.creds = creds
	return nil
}

func (g *Gss) GetInitToken(host string, service string) ([]byte, error) {

	host, err := canonicalizeHostname(host)
	if err != nil {
		return nil, err
	}

	spn := service + "/" + host

	return g.GetInitTokenFromSpn(spn)
}

func (g *Gss) GetInitTokenFromSpn(spn string) ([]byte, error) {
	ctx, token, err := negotiate.NewClientContext(g.creds, spn)
	if err != nil {
		return nil, err
	}

	g.ctx = ctx

	return token, nil
}

func (g *Gss) Continue(inToken []byte) (done bool, outToken []byte, err error) {
	return g.ctx.Update(inToken)
}
