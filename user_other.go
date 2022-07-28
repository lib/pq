// Package pq is a pure Go Postgres driver for the database/sql package.

// For some reason the current setup with the build tags causes this version of the userCurrent to be defined twice on at least illumos 
// (thus porbably solaris aswell) to make the exclusion explicit the build tags listed here are an inverted mirror of those used in user_posix
// user_windows still provides the function on windows NT based platforms. 

//go:build js || android || hurd || zos || !aix || !darwin || !dragonfly || !freebsd || !nacl || !netbsd || !openbsd || !plan9 || !solaris || !rumprun || !illumos
// +build js android hurd zos !aix !darwin !dragonfly !freebsd !nacl !netbsd !openbsd !plan9 !solaris !rumprun !illumos

package pq

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
