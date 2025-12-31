# pq - A pure Go postgres driver for Go's database/sql package

API docs: https://pkg.go.dev/github.com/lib/pq

Install with:

    go get github.com/lib/pq@latest

## Features

* SSL
* Handles bad connections for `database/sql`
* Scan `time.Time` correctly (i.e. `timestamp[tz]`, `time[tz]`, `date`)
* Scan binary blobs correctly (i.e. `bytea`)
* Package for `hstore` support
* COPY FROM support
* pq.ParseURL for converting urls to connection strings for sql.Open.
* Many libpq compatible environment variables
* Unix socket support
* Notifications: `LISTEN`/`NOTIFY`
* pgpass support
* GSS (Kerberos) auth

## Tests

`go test` is used for testing.  See [TESTS.md](TESTS.md) for more details.
