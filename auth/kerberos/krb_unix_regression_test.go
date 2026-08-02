//go:build !windows

package kerberos

import (
	"testing"
	"time"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana"
	"github.com/jcmturner/gokrb5/v8/iana/asnAppTag"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/iana/msgtype"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
)

func TestSecurityRegressionContinueRejectsUnverifiedAcceptCompleted(t *testing.T) {
	forged, err := (&spnego.SPNEGOToken{
		Resp: true,
		NegTokenResp: spnego.NegTokenResp{
			NegState:      asn1.Enumerated(spnego.NegStateAcceptCompleted),
			SupportedMech: gssapi.OIDKRB5.OID(),
			// Deliberately omit ResponseToken. There is no AP-REP proving that
			// the peer possesses the Kerberos service key.
		},
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	done, response, err := (&GSS{pending: true}).Continue(forged)
	if err == nil {
		t.Fatalf("accepted unverified SPNEGO accept-completed response: done=%t response=%x", done, response)
	}
}

func TestContinueVerifiesAPRep(t *testing.T) {
	key := types.EncryptionKey{
		KeyType:  18, // aes256-cts-hmac-sha1-96
		KeyValue: []byte("0123456789abcdef0123456789abcdef"),
	}
	authTime := time.Date(2026, time.August, 1, 12, 34, 56, 0, time.UTC)
	const authMicrosec = 123456

	encReply, err := asn1.Marshal(messages.EncAPRepPart{CTime: authTime, Cusec: authMicrosec})
	if err != nil {
		t.Fatal(err)
	}
	encReply = asn1tools.AddASNAppTag(encReply, asnAppTag.EncAPRepPart)
	encrypted, err := crypto.GetEncryptedData(encReply, key, keyusage.AP_REP_ENCPART, 0)
	if err != nil {
		t.Fatal(err)
	}
	apRep, err := asn1.Marshal(messages.APRep{
		PVNO:    iana.PVNO,
		MsgType: msgtype.KRB_AP_REP,
		EncPart: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	apRep = asn1tools.AddASNAppTag(apRep, asnAppTag.APREP)

	oid, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		t.Fatal(err)
	}
	mechToken := asn1tools.AddASNAppTag(append(append(oid, 0x02, 0x00), apRep...), 0)
	serverToken, err := (&spnego.SPNEGOToken{
		Resp: true,
		NegTokenResp: spnego.NegTokenResp{
			NegState:      asn1.Enumerated(spnego.NegStateAcceptCompleted),
			SupportedMech: gssapi.OIDKRB5.OID(),
			ResponseToken: mechToken,
		},
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	gss := &GSS{
		pending:      true,
		sessionKey:   key,
		authTime:     authTime,
		authMicrosec: authMicrosec,
	}
	done, response, err := gss.Continue(serverToken)
	if err != nil {
		t.Fatal(err)
	}
	if !done || len(response) != 0 {
		t.Fatalf("unexpected continuation result: done=%t response=%x", done, response)
	}
}
