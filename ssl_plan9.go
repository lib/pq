//go:build plan9

package pq

// sslKeyPermissions checks the permissions on user-supplied ssl key files.
// The key file should have very little access.
//
// libpq does not check key file permissions on Plan 9.
func sslKeyPermissions(string) error { return nil }
