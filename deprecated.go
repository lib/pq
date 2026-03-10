package pq

import "database/sql"

// PGError is an interface used by previous versions of pq.
//
// Deprecated: use the Error type. This is never used.
type PGError interface {
	Error() string
	Fatal() bool
	Get(k byte) (v string)
}

// ParseURL converts a url to a connection string for driver.Open.
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }

// NullTime represents a [time.Time] that may be null.
//
// Deprecated: this is an alias for [sql.NullTime].
type NullTime = sql.NullTime
