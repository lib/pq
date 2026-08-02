package pq

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

var benchmarkStringSink string

func BenchmarkCommandCompleteParsing(b *testing.B) {
	tags := []string{
		"UPDATE 1",
		"INSERT 0 1",
		"SELECT 42",
		"CREATE TABLE",
	}
	for _, tag := range tags {
		b.Run(tag, func(b *testing.B) {
			cn := new(conn)
			b.ReportAllocs()
			for range b.N {
				result, command, err := cn.parseComplete(tag)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkResultSink = result
				benchmarkStringSink = command
			}
		})
	}
}

func BenchmarkRowsCompletion(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	cn := newBenchmarkConn(0, benchmarkCommandResponse("SELECT 1"))
	rs := &rows{cn: cn}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rs.done = false
		if err := rs.Next(nil); err != io.EOF {
			b.Fatalf("Next: got %v, want io.EOF", err)
		}
		benchmarkResultSink = rs.result
	}
}

func BenchmarkPreparedStatementExec(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	benchmarks := []struct {
		name  string
		value driver.Value
		typ   oid.Oid
	}{
		{"Int64", int64(42), oid.T_int8},
		{"Float64", 3.14159, oid.T_float8},
		{"String", "benchmark", oid.T_text},
		{"Bool", true, oid.T_bool},
		{"Bytea", []byte("benchmark"), oid.T_bytea},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			response := benchmarkBackendFrame(proto.BindComplete, nil)
			response = append(response, benchmarkCommandResponse("UPDATE 1")...)
			cn := newBenchmarkConn(0, response)
			st := &stmt{
				cn:        cn,
				paramTyps: []oid.Oid{benchmark.typ},
			}
			args := []driver.NamedValue{{Ordinal: 1, Value: benchmark.value}}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := st.ExecContext(context.Background(), args)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkResultSink = result
			}
		})
	}
}
