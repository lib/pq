package scram

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestAllocationServerVerificationLeavesOutputEmpty(t *testing.T) {
	client := NewClient(sha256.New, "benchmark", "correct horse battery staple")
	client.SetNonce([]byte("fixed-client-nonce"))
	serverFirst := []byte("r=fixed-client-nonce-server,s=QSXCR+Q6sek8bf92,i=1")
	if client.Step(nil) || client.Step(serverFirst) {
		t.Fatalf("SCRAM setup failed: %v", client.Err())
	}

	signature := client.serverSignature()
	serverFinal := make([]byte, 2+base64.StdEncoding.EncodedLen(len(signature)))
	copy(serverFinal, "v=")
	base64.StdEncoding.Encode(serverFinal[2:], signature)
	if !client.Step(serverFinal) || client.Err() != nil {
		t.Fatalf("SCRAM server verification failed: %v", client.Err())
	}
	if out := client.Out(); out != nil {
		t.Fatalf("final SCRAM step produced output %q", out)
	}
}
