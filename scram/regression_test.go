package scram

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"strings"
	"testing"
)

const regressionServerFirstPrefix = "r=fixed-client-nonce-server,s=QSXCR+Q6sek8bf92,i="

func regressionSCRAMClient(newHash func() hash.Hash, password, iterations string) (*Client, bool) {
	c := NewClient(newHash, "user", password)
	c.SetNonce([]byte("fixed-client-nonce"))
	if done := c.Step(nil); done {
		return c, done
	}
	done := c.Step([]byte(regressionServerFirstPrefix + iterations))
	return c, done
}

func TestRegressionSCRAMRejectsNonPositiveIterations(t *testing.T) {
	for _, iterations := range []string{"0000", "-001"} {
		t.Run(iterations, func(t *testing.T) {
			c, done := regressionSCRAMClient(sha256.New, "password", iterations)
			if !done || c.Err() == nil {
				t.Fatalf("server iteration count %q was accepted", iterations)
			}
			if !strings.Contains(strings.ToLower(c.Err().Error()), "iteration") {
				t.Errorf("error %q does not identify the invalid iteration count", c.Err())
			}
		})
	}
}

func TestRegressionSCRAMAcceptsShortPositiveIterationCount(t *testing.T) {
	c, done := regressionSCRAMClient(sha256.New, "password", "1")
	if done || c.Err() != nil {
		t.Fatalf("positive server iteration count was rejected: %v", c.Err())
	}
}

type regressionHashBudgetExceeded struct{}

type regressionBudgetHash struct {
	hash.Hash
	remaining *int
}

func (h *regressionBudgetHash) Sum(b []byte) []byte {
	*h.remaining = *h.remaining - 1
	if *h.remaining < 0 {
		panic(regressionHashBudgetExceeded{})
	}
	return h.Hash.Sum(b)
}

func TestRegressionSCRAMRejectsAbsurdIterationsBeforeDerivation(t *testing.T) {
	// A hostile server controls this value. Give the implementation a small,
	// deterministic hash-operation budget so the regression cannot leave a
	// goroutine grinding through billions of PBKDF2 iterations.
	remaining := 64
	newHash := func() hash.Hash {
		return &regressionBudgetHash{Hash: sha256.New(), remaining: &remaining}
	}

	var (
		c           *Client
		done        bool
		budgetPanic bool
	)
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if _, ok := recovered.(regressionHashBudgetExceeded); !ok {
					panic(recovered)
				}
				budgetPanic = true
			}
		}()
		c, done = regressionSCRAMClient(newHash, "password", "2147483647")
	}()

	if budgetPanic {
		t.Fatal("client began unbounded work for an absurd server iteration count")
	}
	if !done || c.Err() == nil {
		t.Fatal("client accepted an absurd server iteration count")
	}
}

func regressionSCRAMProof(t *testing.T, password string) []byte {
	t.Helper()
	c, done := regressionSCRAMClient(sha256.New, password, "4096")
	if done || c.Err() != nil {
		t.Fatalf("SCRAM exchange failed before proof: %v", c.Err())
	}
	return bytes.Clone(c.Out())
}

func TestRegressionPostgreSQLSASLprep(t *testing.T) {
	// PostgreSQL applies RFC 4013 SASLprep to valid UTF-8 passwords.
	for _, tt := range []struct {
		name, password, prepared string
	}{
		{"mapped_to_nothing", "I\u00adX", "IX"},
		{"non_ascii_space", "a\u00a0b", "a b"},
		{"NFKC", "\u2168", "IX"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			have := regressionSCRAMProof(t, tt.password)
			want := regressionSCRAMProof(t, tt.prepared)
			if !bytes.Equal(have, want) {
				t.Error("SCRAM proof was calculated without PostgreSQL SASLprep normalization")
			}
		})
	}

	// PostgreSQL deliberately falls back to the original bytes when SASLprep
	// rejects a password, preserving prohibited and non-UTF-8 passwords.
	for _, tt := range []struct {
		name, password, stripped string
	}{
		{"prohibited_falls_back", "a\u0007b", "ab"},
		{"unicode_3_2_unassigned_falls_back", "\U0001f100", "0."},
		{"invalid_UTF8_falls_back", string([]byte{'a', 0xff, 'b'}), "ab"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			have := regressionSCRAMProof(t, tt.password)
			stripped := regressionSCRAMProof(t, tt.stripped)
			if bytes.Equal(have, stripped) {
				t.Error("password bytes were discarded instead of using PostgreSQL's raw-password fallback")
			}
		})
	}
}
