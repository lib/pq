// Package pq is a pure Go Postgres driver for the database/sql package.

// +build js android hurd illumos zos

package pq

import (
	"os"
	"os/user"
)

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
