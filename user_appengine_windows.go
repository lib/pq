// Package pq is a pure Go Postgres driver for the database/sql package.

//+build appengine

package pq

import "errors"

func userCurrent() (string, error) {
	err := errors.New("no supported under appengine development server")
	return "", err
}
