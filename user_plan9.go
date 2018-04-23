// Package pq is a pure Go Postgres driver for the database/sql package.

// +build plan9

package pq

import (
	"os"
	"os/user"
)

func userCurrent() (string, error) {
	u, err := user.Current()
	if err == nil {
		return u.Username, nil
	}

	name := os.Getenv("user")
	if name != "" {
		return name, nil
	}

	return "", ErrCouldNotDetectUsername
}
