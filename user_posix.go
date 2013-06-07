// Package pq is a pure Go Postgres driver for the database/sql package.

// +build darwin freebsd linux netbsd openbsd

package pq

import "os/user"

func userCurrent() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return u.Username, nil
}
