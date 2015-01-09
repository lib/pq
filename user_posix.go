// Package pq is a pure Go Postgres driver for the database/sql package.

// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package pq

import (
	"errors"
	"os"
	"os/user"
)

func userCurrent() (string, error) {
	u, err := user.Current()
	if err == nil {
		return u.Username, nil
	}

	name := os.Getenv("USER")
	if name != "" {
		return name, nil
	}

	return "", errors.New("Current OS user cannot be detected")
}
