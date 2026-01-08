//go:build windows || plan9

package pqutil

func sslKeyPermissions(string) error { return nil }
