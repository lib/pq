pq is a Go PostgreSQL driver for database/sql.

All [maintained versions of PostgreSQL] are supported. Older versions may work,
but this is not tested.

API docs: https://pkg.go.dev/github.com/lib/pq

Install with:

    go get github.com/lib/pq@latest

[maintained versions of PostgreSQL]: https://www.postgresql.org/support/versioning

Features
--------
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

Running Tests
-------------
Tests need to be run against a PostgreSQL database; you can use Docker compose
to start one:

    docker compose up -d

This starts the latest PostgreSQL; use `docker compose up -d pg«v»` to start a
different version.

In addition, your `/etc/hosts` currently needs an entry:

    127.0.0.1 postgres postgres-invalid

Or you can use any other PostgreSQL instance; see
`testdata/init/docker-entrypoint-initdb.d` for the required setup. You can use
the standard `PG*` environment variables to control the connection details; it
uses the following defaults:

    PGHOST=localhost
    PGDATABASE=pqgo
    PGUSER=pqgo
    PGSSLMODE=disable
    PGCONNECT_TIMEOUT=20

`PQTEST_BINARY_PARAMETERS` can be used to add `binary_parameters=yes` to all
connection strings:

    PQTEST_BINARY_PARAMETERS=1 go test

Tests can be run against pgbouncer with:

    docker compose up -d pgbouncer pg18
    PGPORT=6432 go test ./...

and pgpool with:

    docker compose up -d pgpool pg18
    PGPORT=7432 go test ./...

You can use PQGO_DEBUG=1 to make the driver print the communication with
PostgreSQL to stderr; this works anywhere (test or applications) and can be
useful to debug protocol problems.

For example:

    % PQGO_DEBUG=1 go test -run TestSimpleQuery
    CLIENT → Startup                 69  "\x00\x03\x00\x00database\x00pqgo\x00user [..]"
    SERVER ← (R) AuthRequest          4  "\x00\x00\x00\x00"
    SERVER ← (S) ParamStatus         19  "in_hot_standby\x00off\x00"
    [..]
    SERVER ← (Z) ReadyForQuery        1  "I"
             START conn.query
             START conn.simpleQuery
    CLIENT → (Q) Query                9  "select 1\x00"
    SERVER ← (T) RowDescription      29  "\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00"
    SERVER ← (D) DataRow              7  "\x00\x01\x00\x00\x00\x011"
             END conn.simpleQuery
             END conn.query
    SERVER ← (C) CommandComplete      9  "SELECT 1\x00"
    SERVER ← (Z) ReadyForQuery        1  "I"
    CLIENT → (X) Terminate            0  ""
    PASS
    ok      github.com/lib/pq       0.010s
