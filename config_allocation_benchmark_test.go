package pq

import (
	"net"
	"net/netip"
	"testing"
)

var (
	configAllocationConfigSink    Config
	configAllocationConnectorSink *Connector
	configAllocationSSLSink       func(net.Conn) (net.Conn, error)
	configAllocationMapSink       map[string]string
)

func BenchmarkConfigAllocationParsing(b *testing.B) {
	benchmarks := []struct {
		name string
		dsn  string
	}{
		{"RequireAuthPositive", "host=localhost port=5432 user=benchmark sslmode=disable require_auth=md5,scram-sha-256"},
		{"RequireAuthNegative", "host=localhost port=5432 user=benchmark sslmode=disable require_auth=!password,!md5"},
		{"Escaped", `host=localhost user='bench mark' password='it\'s valid' application_name=pq\ benchmark sslmode=disable`},
		{"URL", "postgres://benchmark:password@localhost:5432/benchmark?sslmode=disable&application_name=pq-benchmark&connect_timeout=5"},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				cfg, err := newConfig(benchmark.dsn, nil)
				if err != nil {
					b.Fatal(err)
				}
				configAllocationConfigSink = cfg
			}
		})
	}
}

func BenchmarkParseDSNAllocations(b *testing.B) {
	benchmarks := []struct {
		name string
		dsn  string
	}{
		{"Plain", "host=localhost port=5432 user=benchmark dbname=benchmark sslmode=disable"},
		{"Escaped", `host=localhost user='bench mark' password='it\'s valid' application_name=pq\ benchmark sslmode=disable`},
		{"URL", "postgres://benchmark:password@localhost:5432/benchmark?sslmode=disable&application_name=pq-benchmark&connect_timeout=5"},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				options, err := parseDSN(benchmark.dsn)
				if err != nil {
					b.Fatal(err)
				}
				configAllocationMapSink = options
			}
		})
	}
}

func BenchmarkConnectorConfigAllocations(b *testing.B) {
	benchmarks := []struct {
		name string
		cfg  Config
	}{
		{
			name: "ScalarOnly",
			cfg: Config{
				Host: "localhost", Port: 5432, User: "benchmark",
				SSLMode: SSLModeDisable,
			},
		},
		{
			name: "MutableFields",
			cfg: Config{
				Host: "one", Port: 5432, User: "benchmark", SSLMode: SSLModeDisable,
				Runtime: map[string]string{"application_name": "benchmark"},
				Multi: []ConfigMultihost{
					{Host: "two", Port: 5433, Hostaddr: netip.MustParseAddr("127.0.0.2")},
					{Host: "three", Port: 5434, Hostaddr: netip.MustParseAddr("127.0.0.3")},
				},
				RequireAuth: RequireAuths{RequireAuthMD5, RequireAuthScramSHA256},
			},
		},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				connector, err := NewConnectorConfig(benchmark.cfg)
				if err != nil {
					b.Fatal(err)
				}
				configAllocationConnectorSink = connector
			}
		})
	}
}

func BenchmarkSSLSetupAllocations(b *testing.B) {
	b.Setenv("HOME", b.TempDir())
	cfg := Config{Host: "localhost", SSLSNI: true}
	for _, mode := range []SSLMode{SSLModeDisable, SSLModeAllow, SSLModeRequire} {
		b.Run(string(mode), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				upgrader, err := ssl(cfg, mode)
				if err != nil {
					b.Fatal(err)
				}
				configAllocationSSLSink = upgrader
			}
		})
	}
}
