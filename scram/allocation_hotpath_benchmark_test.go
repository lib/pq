package scram

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

var allocationClientSink *Client

// BenchmarkAllocationClientProofOneIteration isolates the allocations needed
// to parse the server-first message and construct the client proof without the
// deliberately expensive production PBKDF2 iteration count dominating time.
func BenchmarkAllocationClientProofOneIteration(b *testing.B) {
	serverFirst := []byte("r=fixed-client-nonce-server,s=QSXCR+Q6sek8bf92,i=1")
	nonce := []byte("fixed-client-nonce")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		client := NewClient(sha256.New, "benchmark", "correct horse battery staple")
		client.SetNonce(nonce)
		if done := client.Step(nil); done {
			b.Fatalf("SCRAM exchange ended during the client-first step: %v", client.Err())
		}
		if done := client.Step(serverFirst); done {
			b.Fatalf("SCRAM exchange ended before server verification: %v", client.Err())
		}
		allocationClientSink = client
	}
}

func BenchmarkAllocationServerVerification(b *testing.B) {
	serverFirst := []byte("r=fixed-client-nonce-server,s=QSXCR+Q6sek8bf92,i=1")
	client := NewClient(sha256.New, "benchmark", "correct horse battery staple")
	client.SetNonce([]byte("fixed-client-nonce"))
	if client.Step(nil) || client.Step(serverFirst) {
		b.Fatalf("SCRAM setup failed: %v", client.Err())
	}

	signature := client.serverSignature()
	serverFinal := make([]byte, 2+base64.StdEncoding.EncodedLen(len(signature)))
	copy(serverFinal, "v=")
	base64.StdEncoding.Encode(serverFinal[2:], signature)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := client.step3(serverFinal); err != nil {
			b.Fatal(err)
		}
	}
}
