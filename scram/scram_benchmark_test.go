package scram

import (
	"crypto/sha256"
	"strings"
	"testing"
)

var benchmarkClientSink *Client

// BenchmarkNewClientPasswordPreparation measures password setup separately
// from SCRAM's deliberately expensive PBKDF2 exchange.
func BenchmarkNewClientPasswordPreparation(b *testing.B) {
	benchmarks := []struct {
		name     string
		password string
	}{
		{"ASCII/Short", "correct horse battery staple"},
		{"ASCII/4KiB", strings.Repeat("password", 512)},
		{"UnicodeMapping", strings.Repeat("I\u00adX\u00a0\u2168", 16)},
		{"InvalidUTF8", string([]byte{'p', 'a', 's', 's', 0xff, 'w', 'o', 'r', 'd'})},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(benchmark.password)))
			for range b.N {
				benchmarkClientSink = NewClient(sha256.New, "benchmark", benchmark.password)
			}
		})
	}
}

// BenchmarkClientProof includes PostgreSQL's default 4096 PBKDF2 iterations so
// constructor costs can be interpreted in the context of a real exchange.
func BenchmarkClientProof(b *testing.B) {
	serverFirst := []byte("r=fixed-client-nonce-server,s=QSXCR+Q6sek8bf92,i=4096")
	nonce := []byte("fixed-client-nonce")
	benchmarks := []struct {
		name     string
		password string
	}{
		{"ASCII/Short", "correct horse battery staple"},
		{"ASCII/4KiB", strings.Repeat("password", 512)},
		{"UnicodeMapping", strings.Repeat("I\u00adX\u00a0\u2168", 16)},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				client := NewClient(sha256.New, "benchmark", benchmark.password)
				client.SetNonce(nonce)
				if done := client.Step(nil); done {
					b.Fatalf("SCRAM exchange ended during the client-first step: %v", client.Err())
				}
				if done := client.Step(serverFirst); done {
					b.Fatalf("SCRAM exchange ended before server verification: %v", client.Err())
				}
				benchmarkClientSink = client
			}
		})
	}
}
