//go:build js || android || hurd || zos || wasip1 || appengine

package pq

func userCurrent() (string, error) {
	return "", ErrCouldNotDetectUsername
}
