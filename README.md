# pq - A pure Go postgres driver for Go's database/sql package

[![Build Status](https://travis-ci.org/lib/pq.png?branch=master)](https://travis-ci.org/lib/pq)

## Install

	go get github.com/lib/pq

## Docs

<http://godoc.org/github.com/lib/pq>

## Use

	package main

	import (
		_ "github.com/lib/pq"
		"database/sql"
	)

	func main() {
		db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")
		// ...
	}

**Connection String Parameters**

Similarly to libpq, when establishing a connection using pq you are expected to
supply a [connection string](http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING)
containing zero or more parameters.  A subset of the connection parameters
supported by libpq are also supported by pq.  Additionally, pq also lets you
specify run-time parameters (such as `search_path` or `work_mem`) directly in
the connection string.  This is different from libpq, which does not allow
run-time parameters in the connection string, instead requiring you to supply
them in the `options` parameter.

Most [environment variables](http://www.postgresql.org/docs/current/static/libpq-envars.html)
supported by libpq are also supported by pq.  If any of the environment
variables not supported by pq are set, pq will panic during connection
establishment.  Environment variables have a lower precedence than explicitly
provided connection parameters.

Note that the connection parameter `client_encoding` (which sets the
text encoding for the connection) may be set but must be "UTF8",
matching with the same rules as Postgres. It is an error to provide
any other value.

See http://www.postgresql.org/docs/current/static/libpq-connect.html.

For compatibility with libpq, the following special connection parameters are
supported:

* `dbname` - The name of the database to connect to
* `user` - The user to sign in as
* `password` - The user's password
* `host` - The host to connect to. Values that start with `/` are for unix domain sockets. (default is `localhost`)
* `port` - The port to bind to. (default is `5432`)
* `sslmode` - Whether or not to use SSL (default is `require`, this is not the default for libpq)
	Valid values are:
	* `disable` - No SSL
	* `require` - Always SSL (skip verification)
	* `verify-full` - Always SSL (require verification)

Use single quotes for values that contain whitespace:

    "user=pqgotest password='with spaces'"

In addition to the parameters listed above, any run-time parameter that can be
set at backend start time can be set in the connection string.  For more
information, see
http://www.postgresql.org/docs/current/static/runtime-config.html.

See http://golang.org/pkg/database/sql to learn how to use with `pq` through the `database/sql` package.

## Tests

`go test` is used for testing.  A running PostgreSQL server is
required, with the ability to log in.  The default database to connect
to test with is "pqgotest," but it can be overridden using environment
variables.

Example:

	PGHOST=/var/run/postgresql go test github.com/lib/pq

Optionally, a benchmark suite can be run as part of the tests:

	PGHOST=/var/run/postgresql go test -bench .

## Features

* SSL
* Handles bad connections for `database/sql`
* Scan `time.Time` correctly (i.e. `timestamp[tz]`, `time[tz]`, `date`)
* Scan binary blobs correctly (i.e. `bytea`)
* pq.ParseURL for converting urls to connection strings for sql.Open.
* Many libpq compatible environment variables
* Unix socket support

## Future / Things you can help with

* Notifications: `LISTEN`/`NOTIFY`
* `hstore` sugar (i.e. handling hstore in `rows.Scan`)

## Thank you (alphabetical)

Some of these contributors are from the original library `bmizerany/pq.go` whose
code still exists in here.

* Andy Balholm (andybalholm)
* Ben Berkert (benburkert)
* Bill Mill (llimllib)
* Bjørn Madsen (aeons)
* Blake Gentry (bgentry)
* Brad Fitzpatrick (bradfitz)
* Chris Walsh (cwds)
* Daniel Farina (fdr)
* Everyone at The Go Team
* Evan Shaw (edsrzf)
* Ewan Chou (coocood)
* Federico Romero (federomero)
* Gary Burd (garyburd)
* Heroku (heroku)
* Jason McVetta (jmcvetta)
* Joakim Sernbrant (serbaut)
* John Gallagher (jgallagher)
* Kamil Kisiel (kisielk)
* Kelly Dunn (kellydunn)
* Keith Rarick (kr)
* Maciek Sakrejda (deafbybeheading)
* Marc Brinkmann (mbr)
* Marko Tiikkaja (johto)
* Matt Robenolt (mattrobenolt)
* Martin Olsen (martinolsen)
* Mike Lewis (mikelikespie)
* Nicolas Patry (Narsil)
* Paul Hammond (paulhammond)
* Ryan Smith (ryandotsmith)
* Samuel Stauffer (samuel)
* notedit (notedit)
