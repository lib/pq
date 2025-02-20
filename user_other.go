// Package pq is a pure Go Postgres driver for the database/sql package.

//go:build js || android || hurd || zos || wasip1
// +build js android hurd zos wasip1

package pq

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
