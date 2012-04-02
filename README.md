# pq - A pure Go postgres driver for Go's database/sql package

## Install

	go get github.com/bmizerany/pq

## Use

	package main

	import (
		_ "github.com/bmizerany/pq"
		"database/sql"
	)

	func main() {
		db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")
		// ...
	}

**Connection String Parameters**

These are a subset of the libpq connection parameters.
See http://www.postgresql.org/docs/9.0/static/libpq-connect.html

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

See http://tip.golang.org/pkg/database/sql to learn how to use with `pq` through the `database/sql` package.

## Features

* SSL
* Handles bad connections for `database/sql`
* Scan `time.Time` correctly (i.e. `timestamp[tz]`, `time[tz]`, `date`)
* Scan binary blobs correctly (i.e. `bytea`)
* pq.ParseURL for converting urls to connection strings for sql.Open.

## Future / Things you can help with

* Notifications: `LISTEN`/`NOTIFY`
* `hstore` sugar (i.e. handling hstore in `rows.Scan`)

## Thank you (alphabetical)

Some of these contributors are from the original library `bmizerany/pq.go` whose
code still exists in here.

* Blake Gentry (bgentry)
* Brad Fitzpatrick (bradfitz)
* Daniel Farina (fdr)
* Everyone at The Go Team
* Federico Romero (federomero)
* Heroku (heroku)
* Keith Rarick (kr)
* Mike Lewis (mikelikespie)
* Ryan Smith (ryandotsmith)
* Samuel Stauffer (samuel)
* notedit (notedit)
