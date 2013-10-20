/*
Package pq is a pure Go Postgres driver for the database/sql package.

In most cases clients will use the database/sql package instead of
using this package directly. For example:

	import (
		_ "github.com/lib/pq"
		"database/sql"
	)

	func main() {
		db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")
		if err != nil {
			log.Fatal(err)
		}

		age := 21
		rows, err := db.Query("SELECT name FROM users WHERE age = $1", age)
		…
	}

You can also connect to a database using a URL. For example:

	db, err := sql.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full")


Connection String Parameters


Similarly to libpq, when establishing a connection using pq you are expected to
supply a connection string containing zero or more parameters.
A subset of the connection parameters supported by libpq are also supported by pq.
Additionally, pq also lets you specify run-time parameters (such as search_path or work_mem)
directly in the connection string.  This is different from libpq, which does not allow
run-time parameters in the connection string, instead requiring you to supply
them in the options parameter.

For compatibility with libpq, the following special connection parameters are
supported:

	* dbname - The name of the database to connect to
	* user - The user to sign in as
	* password - The user's password
	* host - The host to connect to. Values that start with / are for unix domain sockets. (default is localhost)
	* port - The port to bind to. (default is 5432)
	* sslmode - Whether or not to use SSL (default is require, this is not the default for libpq)

Valid values for sslmode are:

	* disable - No SSL
	* require - Always SSL (skip verification)
	* verify-full - Always SSL (require verification)

See http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
for more information about connection string parameters.

Use single quotes for values that contain whitespace:

    "user=pqgotest password='with spaces'"

Note that the connection parameter client_encoding (which sets the
text encoding for the connection) may be set but must be "UTF8",
matching with the same rules as Postgres. It is an error to provide
any other value.

In addition to the parameters listed above, any run-time parameter that can be
set at backend start time can be set in the connection string.  For more
information, see
http://www.postgresql.org/docs/current/static/runtime-config.html.

Most environment variables as specified at http://www.postgresql.org/docs/current/static/libpq-envars.html
supported by libpq are also supported by pq.  If any of the environment
variables not supported by pq are set, pq will panic during connection
establishment.  Environment variables have a lower precedence than explicitly
provided connection parameters.


Queries

database/sql does not dictate any specific format for parameter
markers in query strings, and pq uses the Postgres-native ordinal markers,
as shown above. The same marker can be reused for the same parameter:

	rows, err := db.Query(`SELECT name FROM users WHERE favorite_fruit = $1
		OR age BETWEEN $2 AND $2 + 3`, "orange", 64)

pq does not support the LastInsertId() method of the Result type in database/sql.
To return the identifier of an INSERT (or UPDATE or DELETE), use the Postgres
RETURNING clause with a standard Query or QueryRow call:

	rows, err := db.Query(`INSERT INTO users(name, favorite_fruit, age)
		VALUES('beatrice', 'starfruit', 93) RETURNING id`)

For more details on RETURNING, see the Postgres documentation:

	http://www.postgresql.org/docs/current/static/sql-insert.html
	http://www.postgresql.org/docs/current/static/sql-update.html
	http://www.postgresql.org/docs/current/static/sql-delete.html

For additional instructions on querying see the documentation for the database/sql package.

Errors

pq may return errors of type *pq.Error which can be interrogated for error details:

        if err, ok := err.(*pq.Error), ok {
            fmt.Println("pq error:", err.Code.Name())
        }

See the pq.Error type for details.
*/
package pq
