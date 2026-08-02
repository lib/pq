//go:build !windows

package kerberos

import (
	"testing"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/spnego"
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

	done, response, err := (&GSS{}).Continue(forged)
	if err == nil {
		t.Fatalf("accepted unverified SPNEGO accept-completed response: done=%t response=%x", done, response)
	}
}
