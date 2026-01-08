package pgpass

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/lib/pq/internal/pqutil"
)

func PasswordFromPgpass(o map[string]string) string {
	// Do not process .pgpass if a password was supplied.
	if _, ok := o["password"]; ok {
		return o["password"]
	}

	filename := pqutil.Pgpass(o["passfile"])
	if filename == "" {
		return ""
	}

	fp, err := os.Open(filename)
	if err != nil {
		return ""
	}
	defer fp.Close()

	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		split := splitFields(line)
		if len(split) != 5 {
			continue
		}

		socket := o["host"] == "" || filepath.IsAbs(o["host"]) || strings.HasPrefix(o["host"], "@")
		if (split[0] == "*" || split[0] == o["host"] || (split[0] == "localhost" && socket)) &&
			(split[1] == "*" || split[1] == o["port"]) &&
			(split[2] == "*" || split[2] == o["dbname"]) &&
			(split[3] == "*" || split[3] == o["user"]) {
			return split[4]
		}
	}

	return ""
}

func splitFields(s string) []string {
	var (
		fs  = make([]string, 0, 5)
		f   = make([]rune, 0, len(s))
		esc bool
	)
	for _, c := range s {
		switch {
		case esc:
			f, esc = append(f, c), false
		case c == '\\':
			esc = true
		case c == ':':
			fs, f = append(fs, string(f)), f[:0]
		default:
			f = append(f, c)
		}
	}
	return append(fs, string(f))
}
