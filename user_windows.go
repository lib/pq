// Package pq is a pure Go Postgres driver for the database/sql package.
package pq

import (
	"path/filepath"
	"syscall"
)

// Perform Windows user name lookup identical to postgreSQL 9.2, who's user
// lookup functions can be found in postgresql-9.2.4/src/bin/initdb/initdb.c,
// lines 609 to 623.
//
// Note that the postgresql code makes use of the legacy win32 function
// GetUserName, and that function has not been imported into stock Go.
// GetUserNameEx is available though, the difference being that a wider range
// of names are available.  To get the output to be the same as GetUserName,
// only the base (or last) component of the result is returned.
func userCurrent() (string, error) {
	pw_name := make([]uint16, 128)
	pwname_size := uint32(len(pw_name)) - 1
	err := syscall.GetUserNameEx(syscall.NameSamCompatible, &pw_name[0], &pwname_size)
	if err != nil {
		return "", err
	}
	s := syscall.UTF16ToString(pw_name)
	u := filepath.Base(s)
	return u, nil
}
