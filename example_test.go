package pq_test

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
)

func Example_open() {
	// Or as URL: postgresql://localhost/pqgo
	db, err := sql.Open("postgres", "dbname=pqgo")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// db.Open() only creates a connection pool, and doesn't actually establish
	// a connection to the database. To ensure the connection works you need to
	// do *something* with a connection.
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// Output:
}

func Example_openConfig() {
	cfg := pq.Config{
		Host: "localhost",
		Port: 5432,
		User: "pqgo",
	}
	// Or: create a new Config from the defaults, environment, and DSN.
	// cfg, err := pq.NewConfig("host=postgres dbname=pqgo")
	// if err != nil {
	//     log.Fatal(err)
	// }

	c, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Create connection pool.
	db := sql.OpenDB(c)
	defer db.Close()

	// Make sure it works.
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// Output:
}

func Example_timestampWithTimezone() {
	dbUTC, err := sql.Open("postgres", "dbname=pqgo timezone=UTC")
	if err != nil {
		log.Fatal(err)
	}
	defer dbUTC.Close()

	dbPL, err := sql.Open("postgres", "dbname=pqgo timezone=Asia/Gaza")
	if err != nil {
		log.Fatal(err)
	}
	defer dbPL.Close()

	var tsUTC, tsPL time.Time
	err = dbUTC.QueryRow(`select '2026-03-15 17:45:47Z'::timestamptz`).Scan(&tsUTC)
	if err != nil {
		log.Fatal(err)
	}
	err = dbPL.QueryRow(`select '2026-03-15 17:45:47Z'::timestamptz`).Scan(&tsPL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("timestamptz in UTC:      ", tsUTC)
	fmt.Println("timestamptz in Asia/Gaza:", tsPL)
	fmt.Println("Equal():                 ", tsUTC.Equal(tsPL))
	// Output:
	// timestamptz in UTC:       2026-03-15 17:45:47 +0000 UTC
	// timestamptz in Asia/Gaza: 2026-03-15 19:45:47 +0200 EET
	// Equal():                  true
}

func Example_timestampWithoutTimezone() {
	db, err := sql.Open("postgres", "dbname=pqgo timezone=UTC")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var ts time.Time
	err = db.QueryRow(`select '2026-03-15 17:45:47'::timestamp`).Scan(&ts)
	if err != nil {
		log.Fatal(err)
	}

	z, o := ts.Zone()
	fmt.Println("timestamp :               ", ts)
	fmt.Printf("Zone():                    %q %v\n", z, o)
	fmt.Println("Location() == time.UTC:   ", ts.Location() == time.UTC)
	fmt.Println("Location() == FixedZone:  ", ts.Location() == time.FixedZone("", 0))

	// Output:
	// timestamp :                2026-03-15 17:45:47 +0000 +0000
	// Zone():                    "" 0
	// Location() == time.UTC:    false
	// Location() == FixedZone:   true
}

func Example_copyFromStdin() {
	// Connect and create table.
	db, err := sql.Open("postgres", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create temp table users (name text, age int)`)
	if err != nil {
		log.Fatal(err)
	}

	// Need to start transaction and prepare a statement.
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`copy users (name, age) from stdin`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Insert rows.
	users := []struct {
		Name string
		Age  int
	}{
		{"Donald Duck", 36},
		{"Scrooge McDuck", 86},
	}
	for _, user := range users {
		_, err = stmt.Exec(user.Name, user.Age)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Finalize copy and statement, and commit transaction.
	if _, err := stmt.Exec(); err != nil {
		log.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Query rows to verify.
	rows, err := db.Query(`select * from users order by name`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name string
			age  int
		)
		err := rows.Scan(&name, &age)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(name, age)
	}

	// Output:
	// Donald Duck 36
	// Scrooge McDuck 86
}

func ExampleNewConnector() {
	c, err := pq.NewConnector("host=postgres dbname=pqgo")
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// Output:
}

func ExampleConnectorWithNoticeHandler() {
	// Base connector to wrap
	base, err := pq.NewConnector("dbname=pqgo")
	if err != nil {
		log.Fatal(err)
	}

	// Wrap the connector to simply print out the message
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		fmt.Printf("NOTICE: %s\n", notice.Message)
	})
	db := sql.OpenDB(connector)
	defer db.Close()

	// Raise a notice
	_, err = db.Exec(`drop table if exists doesntexist`)
	if err != nil {
		log.Fatal(err)
	}

	// And via PL/pgSQL.
	_, err = db.Exec(`
		do language plpgsql $$ begin
			raise notice 'test notice';
		end $$
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// NOTICE: table "doesntexist" does not exist, skipping
	// NOTICE: test notice
}

func ExampleRegisterTLSConfig() {
	pem, err := os.ReadFile("testdata/ssl/root.crt")
	if err != nil {
		log.Fatal(err)
	}

	root := x509.NewCertPool()
	root.AppendCertsFromPEM(pem)

	certs, err := tls.LoadX509KeyPair("testdata/ssl/postgresql.crt", "testdata/ssl/postgresql.key")
	if err != nil {
		log.Fatal(err)
	}

	pq.RegisterTLSConfig("mytls", &tls.Config{
		RootCAs:      root,
		Certificates: []tls.Certificate{certs},
		ServerName:   "postgres",
	})

	db, err := sql.Open("postgres", "host=postgres dbname=pqgo sslmode=pqgo-mytls")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// Output:
}

func ExampleListener() {
	// Connect with Listener.
	var (
		dsn          = "dbname=pqgo "
		minReconnect = 10 * time.Second
		maxReconnect = time.Minute
	)
	l := pq.NewListener(dsn, minReconnect, maxReconnect, func(ev pq.ListenerEventType, err error) {
		fmt.Printf("callback: %s: %v\n", ev, err)
	})
	defer l.Close()

	// Can listen on as many channels as you want.
	err := l.Listen("coconut")
	if err != nil {
		log.Fatal(err)
	}
	err = l.Listen("banana")
	if err != nil {
		log.Fatal(err)
	}

	// Send notifications for our test.
	go func() {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		_, err = db.Exec(`notify coconut, 'got a lovely bunch'`)
		if err != nil {
			log.Fatal(err)
		}
		_, err = db.Exec(`notify banana, 'yellow and curvy'`)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Keep listening on Notify channel.
	var i int
	for {
		select {
		case <-time.After(1 * time.Second):
			l.Close()
		case n := <-l.Notify:
			i++
			if n == nil {
				fmt.Println("nil notify: closing Listener")
				return
			}
			fmt.Printf("notification on %q with data %q\n", n.Channel, n.Extra)

			// Quickly exit after second notification in this example, so tests
			// run faster.
			if i == 2 {
				l.Close()
			}
		}
	}

	// Output:
	// callback: connected: <nil>
	// notification on "coconut" with data "got a lovely bunch"
	// notification on "banana" with data "yellow and curvy"
	// nil notify: closing Listener
}

func ExampleAs() {
	db, err := sql.Open("postgres", "")
	if err != nil {
		log.Fatal(err)
	}

	email := "hello@example.com"

	_, err = db.Exec("insert into t (email) values ($1)", email)
	if pqErr := pq.As(err, pqerror.UniqueViolation); pqErr != nil {
		log.Fatalf("email %q already exsts", email)
	}
	if err != nil {
		log.Fatalf("unknown error: %s", err)
	}
}
