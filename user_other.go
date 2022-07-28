// Package pq is a pure Go Postgres driver for the database/sql package.

//go:build js || android || hurd || zos || !solaris || !illumos
// +build js android hurd zos !solaris !illumos

package pq

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
