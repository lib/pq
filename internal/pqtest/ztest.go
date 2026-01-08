// Copied from https://github.com/arp242/zstd/tree/main/ztest

package pqtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ErrorContains checks if the error message in have contains the text in
// want.
//
// This is safe when have is nil. Use an empty string for want if you want to
// test that err is nil.
func ErrorContains(have error, want string) bool {
	if have == nil {
		return want == ""
	}
	if want == "" {
		return false
	}
	return strings.Contains(have.Error(), want)
}

// Read data from a file.
func Read(t *testing.T, paths ...string) []byte {
	t.Helper()

	path := filepath.Join(paths...)
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ztest.Read: cannot read %v: %v", path, err)
	}
	return file
}

// TempFile creates a new temporary file and returns the path.
func TempFile(t *testing.T, name, data string) string {
	t.Helper()
	if name == "" {
		name = "ztest"
	}
	name = filepath.Join(t.TempDir(), name)
	tempFile(t, name, data)
	return name
}

func tempFile(t *testing.T, path, data string) {
	t.Helper()
	err := os.WriteFile(path, []byte(data), 0o666)
	if err != nil {
		t.Fatalf("ztest.TempFile: %s", err)
	}
}

// NormalizeIndent removes tab indentation from every line.
//
// This is useful for "inline" multiline strings:
//
//	  cases := []struct {
//	      string in
//	  }{
//	      `
//		 	    Hello,
//		 	    world!
//	      `,
//	  }
//
// This is nice and readable, but the downside is that every line will now have
// two extra tabs. This will remove those two tabs from every line.
//
// The amount of tabs to remove is based only on the first line, any further
// tabs will be preserved.
func NormalizeIndent(in string) string {
	indent := 0
	for _, c := range strings.TrimLeft(in, "\n") {
		if c != '\t' {
			break
		}
		indent++
	}

	r := ""
	for _, line := range strings.Split(in, "\n") {
		r += strings.Replace(line, "\t", "", indent) + "\n"
	}

	return strings.TrimSpace(r)
}
