// Package pq is a pure Go Postgres driver for the database/sql package.

// +build appengine

package pq

// For Google App Engine(GAE)
// GAE refuse `import "syscall"`,
// so you cannot build with the original `userCurrent` function in "user_windows.go"
// If we are on GAE, exclude "user_windows.go" by build constraint,
// and use this file instead.
func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
