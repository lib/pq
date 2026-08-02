package pq

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestErrorRegressionOutOfRangePositionDoesNotPanic(t *testing.T) {
	err := &Error{
		Message:  "backend supplied an invalid cursor position",
		Position: "999",
		query:    "select 1",
	}

	panicValue, _ := regressionCallWithoutPanic(func() error {
		_ = err.ErrorWithDetail()
		return nil
	})
	if panicValue != nil {
		t.Fatalf("out-of-range backend error position caused a panic: %v", panicValue)
	}
}

func TestQueryPositionPreservesErrorPositionSemantics(t *testing.T) {
	queries := []string{
		"",
		"select 1",
		"select 1\n",
		"first\nsecond\nthird",
		"αβγ\nδεζ",
		"α\tb\nsecond",
		"\n\n",
	}
	for _, query := range queries {
		lines := strings.Split(query, "\n")
		for pos := -1; pos <= utf8.RuneCountInString(query)+len(lines)+2; pos++ {
			wantLine, wantColumn, wantOK := referenceErrorPosition(pos, lines)
			line, column, ok := queryPosition(query, pos)
			if line != wantLine || column != wantColumn || ok != wantOK {
				t.Fatalf("queryPosition(%q, %d) = (%d, %d, %v); want (%d, %d, %v)",
					query, pos, line, column, ok, wantLine, wantColumn, wantOK)
			}
		}
	}
}

func referenceErrorPosition(pos int, lines []string) (line, col int, ok bool) {
	if pos < 1 {
		return 0, 0, false
	}
	read := 0
	for i := range lines {
		lineLength := utf8.RuneCountInString(lines[i]) + 1
		if read+lineLength >= pos {
			return i + 1, pos - read, true
		}
		read += lineLength
	}
	return 0, 0, false
}

func TestExpandedTabLengthMatchesFormatting(t *testing.T) {
	for _, input := range []string{"", "plain", "\t", "α\tb", "a\tβ\tγ"} {
		var output strings.Builder
		writeExpandedTabs(&output, input)
		if have, want := output.Len(), expandedTabLen(input); have != want {
			t.Fatalf("expandedTabLen(%q) = %d; formatted length is %d", input, want, have)
		}
	}
}

func TestErrorSourceLineRetainsPositiveSignPaddingPastFieldWidth(t *testing.T) {
	var output strings.Builder
	writeErrorSourceLine(&output, 1_000_000, "source")
	if got, want := output.String(), " 1000000 | source\n"; got != want {
		t.Fatalf("source line = %q; want %q", got, want)
	}
}

func TestParseErrorOwnsCombinedBackendFields(t *testing.T) {
	payload := []byte("SERROR\x00C23505\x00Mduplicate\x00Ddetail\x00Hhint\x00\x00trailing")
	r := readBuf(payload)
	err := parseError(&r, "select benchmark")
	if err.Severity != "ERROR" || err.Code != "23505" || err.Message != "duplicate" ||
		err.Detail != "detail" || err.Hint != "hint" {
		t.Fatalf("parsed error = %#v", err)
	}
	if got := string(r); got != "trailing" {
		t.Fatalf("unconsumed payload = %q; want trailing", got)
	}
	for i := range len(payload) - len("trailing") {
		payload[i] = 'x'
	}
	if err.Severity != "ERROR" || err.Code != "23505" || err.Message != "duplicate" ||
		err.Detail != "detail" || err.Hint != "hint" {
		t.Fatal("parsed Error fields retained the reusable backend buffer")
	}
}
