unreleased
----------
This version of kq requires Go 1.18 or newer.

pq now supports only maintained PostgreSQL releases, which is PostgreSQL 14 and
newer. Previously PostgreSQL 8.4 and newer were supported.

### Features

- Add support for NamedValueChecker interface ([#1125], [#1238]).

- Support [`sslnegotiation`] to use SSL without negotiation ([#1180]).

- The `pq.Error.Error()` text  includes the position of the error (if reported
  by PostgreSQL) and SQLSTATE code ([#1219], [#1224]):

      pq: column "columndoesntexist" does not exist at column 8 (42703)
      pq: syntax error at or near ")" at position 2:71 (42601)

- The `pq.Error.ErrorWithDetail()` method prints a more detailed multiline
  message, with the Detail, Hint, and error position (if any) ([#1219]):

      ERROR:   syntax error at or near ")" (42601)
      CONTEXT: line 12, column 1:

           10 |     name           varchar,
           11 |     version        varchar,
           12 | );
                ^

- Allow using a custom `tls.Config`, for example for encrypted keys ([#1228]).

- Add `PQGO_DEBUG=1` print the communication with PostgreSQL to stderr, to aid
  in debugging, testing, and bug reports ([#1223]).

### Fixes

- Match HOME directory lookup logic with libpq: prefer $HOME over /etc/passwd,
  ignore ENOTDIR errors, and use APPDATA on Windows ([#1214]).

- Fix `sslmode=verify-ca` verifying the hostname anyway when connecting to a DNS
  name (rather than IP) ([#1226])

- Fix build with wasm ([#1184]), appengine ([#745]), and Plan 9 ([#1133]).

- Deprecate and type alias `pq.NullTime` to `sql.NullTime` ([#1211]).

- Enforce integer limits of the Postgres wire protocol ([#1161]).

- Accept the `passfile` connection parameter to override `PGPASSFILE` ([#1129]).

- Fix connecting to socket on Windows systems ([#1179]).

- Don't perform a permission check on the .pgpass file on Windows ([#595]).

- Warn about incorrect .pgpass permissions ([#595]).

- Don't set extra_float_digits ([#1212]).

- Decode bpchar into a string ([#949]).

- Fix panic in Ping() by not requiring CommandComplete or EmptyQueryResponse in
  simpleQuery() ([#1234])

- Recognize bit/varbit ([#743]) and float types ([#1166]) in ColumnTypeScanType().

- Accept `PGGSSLIB` and `PGKRBSRVNAME` environment variables ([#1143]).

- Handle ErrorResponse in readReadyForQuery and return proper error ([#1136]).

- CopyIn() and CopyInSchema() now work if the list of columns is empty, in which
  case it will copy all columns ([#1239]).

- Treat nil []byte in query parameters as nil/NULL rather than `""` ([#838]).

[`sslnegotiation`]: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLNEGOTIATION
[#595]: https://github.com/lib/pq/pull/595
[#745]: https://github.com/lib/pq/pull/745
[#743]: https://github.com/lib/pq/pull/743
[#838]: https://github.com/lib/pq/pull/838
[#949]: https://github.com/lib/pq/pull/949
[#1125]: https://github.com/lib/pq/pull/1125
[#1129]: https://github.com/lib/pq/pull/1129
[#1133]: https://github.com/lib/pq/pull/1133
[#1136]: https://github.com/lib/pq/pull/1136
[#1143]: https://github.com/lib/pq/pull/1143
[#1161]: https://github.com/lib/pq/pull/1161
[#1166]: https://github.com/lib/pq/pull/1166
[#1179]: https://github.com/lib/pq/pull/1179
[#1180]: https://github.com/lib/pq/pull/1180
[#1184]: https://github.com/lib/pq/pull/1184
[#1211]: https://github.com/lib/pq/pull/1211
[#1212]: https://github.com/lib/pq/pull/1212
[#1214]: https://github.com/lib/pq/pull/1214
[#1219]: https://github.com/lib/pq/pull/1219
[#1223]: https://github.com/lib/pq/pull/1223
[#1224]: https://github.com/lib/pq/pull/1224
[#1226]: https://github.com/lib/pq/pull/1226
[#1228]: https://github.com/lib/pq/pull/1228
[#1234]: https://github.com/lib/pq/pull/1234
[#1238]: https://github.com/lib/pq/pull/1238
[#1239]: https://github.com/lib/pq/pull/1239


v1.10.9 (2023-04-26)
--------------------
- Fixes backwards incompat bug with 1.13.

- Fixes pgpass issue
