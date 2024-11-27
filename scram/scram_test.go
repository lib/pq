// Package scram_test provides functional tests for the scram package.
package scram_test

import (
	"crypto/sha1"
	"testing"

	"github.com/lib/pq/scram"
)

func TestScramSHA1(t *testing.T) {
	s := 0
	var in []byte // first step requires no input
	var client = scram.NewClient(sha1.New, rfc5802User, rfc5802Pass)
	client.SetNonce(rfc5802Nonce)
	for client.Step(in) {
		if s >= 2 {
			t.Fatal("Step didn't stop after 3rd step")
		}
		out := client.Out()
		if cm := rfc5802ClientMsgs[s]; cm != string(out) {
			t.Fatalf(
				`Step: %d
Expected message: %q
Actual message:   %q`,
				s+1, cm, out,
			)
		}
		in = rfc5802ServerMsgs[s]
		s++
	}
	if err := client.Err(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}
