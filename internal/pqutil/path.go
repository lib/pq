package pqutil

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
)

// Home gets the user's home directory. Matches pqGetHomeDirectory() from
// PostgreSQL
//
// https://github.com/postgres/postgres/blob/2b117bb/src/interfaces/libpq/fe-connect.c#L8214
func Home() string {
	if runtime.GOOS == "windows" {
		// pq uses SHGetFolderPath(), which is deprecated but x/sys/windows has
		// KnownFolderPath(). We don't really want to pull that in though, so
		// use APPDATA env. This is also what PostgreSQL uses in some other
		// codepaths (get_home_path() for example).
		ad := os.Getenv("APPDATA")
		if ad == "" {
			return ""
		}
		return filepath.Join(ad, "postgresql")
	}

	home, _ := os.UserHomeDir()
	if home == "" {
		u, err := user.Current()
		if err != nil {
			return ""
		}
		home = u.HomeDir
	}
	return home
}

// Pgpass gets the filepath to the pgpass file to use, returning "" if a pgpass
// file shouldn't be used.
func Pgpass(passfile string) string {
	// Get passfile from the options.
	if passfile == "" {
		home := Home()
		if home == "" {
			return ""
		}
		passfile = filepath.Join(home, ".pgpass")
	}

	// On Win32, the directory is protected, so we don't have to check the file.
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(passfile)
		if err != nil {
			return ""
		}
		if fi.Mode().Perm()&(0x77) != 0 {
			fmt.Fprintf(os.Stderr,
				"WARNING: password file %q has group or world access; permissions should be u=rw (0600) or less\n",
				passfile)
			return ""
		}
	}
	return passfile
}
