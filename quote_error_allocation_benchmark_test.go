package pq

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lib/pq/pqerror"
)

var (
	benchmarkAllocationStringSink string
	benchmarkAllocationIntSink    int
	benchmarkAllocationErrorSink  *Error
)

// BenchmarkQuoteAllocationHotPaths tracks the allocation cost of the public
// SQL quoting helpers for both their common no-escape path and escaped input.
func BenchmarkQuoteAllocationHotPaths(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		input string
	}{
		{"Identifier/Plain", "benchmark_table"},
		{"Identifier/Escaped", `benchmark_"table`},
		{"Literal/Plain", "benchmark value"},
		{"Literal/Quote", "benchmark's value"},
		{"Literal/QuoteAndBackslash", `benchmark's \\ value`},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if benchmark.name[:10] == "Identifier" {
					benchmarkAllocationStringSink = QuoteIdentifier(benchmark.input)
				} else {
					benchmarkAllocationStringSink = QuoteLiteral(benchmark.input)
				}
			}
		})
	}

	b.Run("Identifier/BufferReused", func(b *testing.B) {
		var buffer bytes.Buffer
		buffer.Grow(64)
		b.ReportAllocs()
		for range b.N {
			buffer.Reset()
			BufferQuoteIdentifier(`benchmark_"table`, &buffer)
			benchmarkAllocationIntSink = buffer.Len()
		}
	})
}

func BenchmarkParseErrorAllocations(b *testing.B) {
	payload := []byte("SERROR\x00C23505\x00Mduplicate key value violates unique constraint\x00" +
		"DKey (account_id)=(42) already exists.\x00HChoose another identifier.\x00" +
		"P42\x00sbenchmark\x00taccounts\x00caccount_id\x00naccounts_pkey\x00" +
		"Fnbtinsert.c\x00L666\x00R_check_unique\x00\x00")

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		r := readBuf(payload)
		benchmarkAllocationErrorSink = parseError(&r, "insert into accounts values (42)")
	}
}

// BenchmarkErrorFormattingAllocationHotPaths uses an in-memory Error so it
// measures formatting and type inspection rather than PostgreSQL round trips.
func BenchmarkErrorFormattingAllocationHotPaths(b *testing.B) {
	err := &Error{
		Severity: "ERROR",
		Code:     pqerror.UniqueViolation,
		Message:  "duplicate key value violates unique constraint",
		Detail:   "Key (account_id)=(42) already exists.",
		Hint:     "Choose another account identifier.",
		Position: "42",
		query:    "insert into accounts (account_id, name)\n\tvalues (42, 'benchmark')",
	}

	b.Run("Error", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkAllocationStringSink = err.Error()
		}
	})
	b.Run("ErrorWithDetail", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkAllocationStringSink = err.ErrorWithDetail()
		}
	})
	b.Run("As/Direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBoolSink = As(err, pqerror.UniqueViolation) != nil
		}
	})
	b.Run("As/Wrapped", func(b *testing.B) {
		wrapped := fmt.Errorf("execute statement: %w", err)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkBoolSink = As(wrapped, pqerror.UniqueViolation) != nil
		}
	})
}
