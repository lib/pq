# pq - A pure Go postgres driver for Go's database/sql package

[![Build Status](https://travis-ci.org/lib/pq.png?branch=master)](https://travis-ci.org/lib/pq)

## Install

	go get github.com/lib/pq

## Docs

For detailed documentation and basic usage examples, please see the package
documentation at <http://godoc.org/github.com/lib/pq>.

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
