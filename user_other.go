// Package pq is a pure Go Postgres driver for the database/sql package.

// +build js android hurd zos

package pq

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
