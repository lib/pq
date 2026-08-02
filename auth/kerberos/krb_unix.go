//go:build !windows

// UNIX Kerberos support, using jcmturner's pure-go implementation

package kerberos

import (
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/flags"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
)

// GSS implements the pq.GSS interface.
type GSS struct {
	cli *client.Client

	pending      bool
	sessionKey   types.EncryptionKey
	authTime     time.Time
	authMicrosec int
}

// NewGSS creates a new GSS provider.
func NewGSS() (*GSS, error) {
	g := &GSS{}
	err := g.init()

	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *GSS) init() error {
	cfgPath, ok := os.LookupEnv("KRB5_CONFIG")
	if !ok {
		cfgPath = "/etc/krb5.conf"
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	u, err := user.Current()
	if err != nil {
		return err
	}

	ccpath := "/tmp/krb5cc_" + u.Uid

	ccname := os.Getenv("KRB5CCNAME")
	if strings.HasPrefix(ccname, "FILE:") {
		ccpath = strings.SplitN(ccname, ":", 2)[1]
	}

	ccache, err := credentials.LoadCCache(ccpath)
	if err != nil {
		return err
	}

	cl, err := client.NewFromCCache(ccache, cfg, client.DisablePAFXFAST(true))
	if err != nil {
		return err
	}

	cl.Login()

	g.cli = cl

	return nil
}

// GetInitToken implements the GSS interface.
func (g *GSS) GetInitToken(host string, service string) ([]byte, error) {

	// Resolve the hostname down to an 'A' record, if required (usually, it is)
	if g.cli.Config.LibDefaults.DNSCanonicalizeHostname {
		var err error
		host, err = canonicalizeHostname(host)
		if err != nil {
			return nil, err
		}
	}

	spn := service + "/" + host

	return g.GetInitTokenFromSpn(spn)
}

// GetInitTokenFromSpn implements the GSS interface.
func (g *GSS) GetInitTokenFromSpn(spn string) ([]byte, error) {
	ticket, sessionKey, err := g.cli.GetServiceTicket(spn)
	if err != nil {
		return nil, fmt.Errorf("kerberos error (getting service ticket): %w", err)
	}
	mechToken, err := spnego.NewKRB5TokenAPREQ(
		g.cli,
		ticket,
		sessionKey,
		[]int{gssapi.ContextFlagMutual, gssapi.ContextFlagInteg, gssapi.ContextFlagConf},
		[]int{flags.APOptionMutualRequired},
	)
	if err != nil {
		return nil, fmt.Errorf("kerberos error (creating AP-REQ): %w", err)
	}
	if err := mechToken.APReq.DecryptAuthenticator(sessionKey); err != nil {
		return nil, fmt.Errorf("kerberos error (retaining authenticator): %w", err)
	}
	mechBytes, err := mechToken.Marshal()
	if err != nil {
		return nil, fmt.Errorf("kerberos error (marshaling AP-REQ): %w", err)
	}
	token := &spnego.SPNEGOToken{
		Init: true,
		NegTokenInit: spnego.NegTokenInit{
			MechTypes:      []asn1.ObjectIdentifier{gssapi.OIDKRB5.OID()},
			MechTokenBytes: mechBytes,
		},
	}
	b, err := token.Marshal()
	if err != nil {
		return nil, fmt.Errorf("kerberos error (marshaling SPNEGO token): %w", err)
	}

	g.pending = true
	g.sessionKey = sessionKey
	g.authTime = mechToken.APReq.Authenticator.CTime
	g.authMicrosec = mechToken.APReq.Authenticator.Cusec
	return b, nil
}

// Continue implements the GSS interface.
func (g *GSS) Continue(inToken []byte) (done bool, outToken []byte, err error) {
	if !g.pending {
		return true, nil, fmt.Errorf("kerberos error: no mutual authentication exchange is pending")
	}
	g.pending = false
	defer func() {
		g.sessionKey = types.EncryptionKey{}
		g.authTime = time.Time{}
		g.authMicrosec = 0
	}()

	t := &spnego.SPNEGOToken{}
	err = t.Unmarshal(inToken)
	if err != nil {
		return true, nil, fmt.Errorf("kerberos error (unmarshaling token): %w", err)
	}
	if !t.Resp {
		return true, nil, fmt.Errorf("kerberos error: server returned a SPNEGO initiation token")
	}
	state := t.NegTokenResp.State()
	if state != spnego.NegStateAcceptCompleted {
		return true, nil, fmt.Errorf("kerberos: expected state 'Completed' - got %d", state)
	}
	if len(t.NegTokenResp.SupportedMech) > 0 &&
		!t.NegTokenResp.SupportedMech.Equal(gssapi.OIDKRB5.OID()) &&
		!t.NegTokenResp.SupportedMech.Equal(gssapi.OIDMSLegacyKRB5.OID()) {
		return true, nil, fmt.Errorf("kerberos error: server selected unsupported mechanism %s", t.NegTokenResp.SupportedMech)
	}
	if len(t.NegTokenResp.ResponseToken) == 0 {
		return true, nil, fmt.Errorf("kerberos error: server completed authentication without an AP-REP")
	}

	var mechToken spnego.KRB5Token
	if err := mechToken.Unmarshal(t.NegTokenResp.ResponseToken); err != nil {
		return true, nil, fmt.Errorf("kerberos error (unmarshaling response token): %w", err)
	}
	if !mechToken.IsAPRep() {
		return true, nil, fmt.Errorf("kerberos error: server response token is not an AP-REP")
	}
	plain, err := crypto.DecryptEncPart(mechToken.APRep.EncPart, g.sessionKey, keyusage.AP_REP_ENCPART)
	if err != nil {
		return true, nil, fmt.Errorf("kerberos error (decrypting AP-REP): %w", err)
	}
	var reply messages.EncAPRepPart
	if err := reply.Unmarshal(plain); err != nil {
		return true, nil, fmt.Errorf("kerberos error (unmarshaling AP-REP): %w", err)
	}
	if !reply.CTime.Equal(g.authTime) || reply.Cusec != g.authMicrosec {
		return true, nil, fmt.Errorf("kerberos error: AP-REP does not match the client authenticator")
	}

	return true, nil, nil
}
