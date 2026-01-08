//go:build !windows && !js && !android && !hurd && !zos && !wasip1 && !appengine

package pqutil

import (
	"os"
	"os/user"
)

func User() (string, error) {
	if n := os.Getenv("USER"); n != "" {
		return n, nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return u.Username, nil
}
