package pq_test

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"os"
	"testing"

	"github.com/lib/pq"
	"github.com/lib/pq/internal/pqtest"
)

func getTestDSN(t *testing.T) string {
	t.Helper()
	var dsn string
	if pqtest.Supavisor() {
		dsn = "host=localhost dbname=pqgo sslmode=disable"
	} else {
		dsn = "host=postgres dbname=pqgo"
	}
	return dsn
}

func TestExampleNewConnector(t *testing.T) {
	c, err := pq.NewConnector(getTestDSN(t))
	if err != nil {
		t.Fatalf("could not create connector: %v", err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	// Use the DB
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("could not start transaction: %v", err)
	}
	tx.Rollback()
	// Output:
}

func TestExampleNewConfig(t *testing.T) {
	cfg, err := pq.NewConfig(getTestDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host == "localhost" {
		cfg.Host = "127.0.0.1"
	}

	c, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	// Use the DB
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("could not start transaction: %v", err)
	}
	tx.Rollback()
	// Output:
}

func TestExampleConnectorWithNoticeHandler(t *testing.T) {
	// Base connector to wrap
	dsn := ""
	base, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}

	// Wrap the connector to simply print out the message
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		t.Logf("Notice sent: %s", notice.Message)
	})
	db := sql.OpenDB(connector)
	defer db.Close()

	// Raise a notice
	sql := "DO language plpgsql $$ BEGIN RAISE NOTICE 'test notice'; END $$"
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}
	// Output:
	// Notice sent: test notice
}

func TestExampleRegisterTLSConfig(t *testing.T) {
	// TODO: implement SSL support in Supavisor config
	pqtest.SkipSupavisor(t)

	pem, err := os.ReadFile("testdata/init/root.crt")
	if err != nil {
		t.Fatal(err)
	}

	root := x509.NewCertPool()
	root.AppendCertsFromPEM(pem)

	certs, err := tls.LoadX509KeyPair("testdata/init/postgresql.crt", "testdata/init/postgresql.key")
	if err != nil {
		t.Fatal(err)
	}

	pq.RegisterTLSConfig("mytls", &tls.Config{
		RootCAs:      root,
		Certificates: []tls.Certificate{certs},
		ServerName:   "postgres",
	})

	db, err := sql.Open("postgres", "host=postgres dbname=pqgo sslmode=pqgo-mytls")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	// Output:
}

func TestExampleCopyIn(t *testing.T) {
	// This test won't work with transaction mode connection pooling
	// without a transaction.
	pqtest.SkipSupavisorTransactionMode(t)

	// Connect and create table.
	db, err := sql.Open("postgres", getTestDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create temp table users (name text, age int)`)
	if err != nil {
		t.Fatal(err)
	}

	// Need to start transaction and prepare a statement.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(pq.CopyIn("users", "name", "age"))
	if err != nil {
		t.Fatal(err)
	}

	// Insert rows.
	users := []struct {
		Name string
		Age  int
	}{
		{"Donald Duck", 36},
		{"Scrooge McDuck", 86},
	}
	for _, user := range users {
		_, err = stmt.Exec(user.Name, int64(user.Age))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Finalize copy and statement, and commit transaction.
	if _, err := stmt.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query rows to verify.
	rows, err := db.Query(`select * from users order by name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	tests := []struct {
		name string
		age  int
	}{
		{"Donald Duck", 36},
		{"Scrooge McDuck", 86},
	}
	i := 0
	for rows.Next() {
		var (
			name string
			age  int
		)
		err := rows.Scan(&name, &age)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s %d", name, age)
		if have, want := name, tests[i].name; have != want {
			t.Errorf("\nhave: %s\nwant: %s", have, want)
		}
		if have, want := age, tests[i].age; have != want {
			t.Errorf("\nhave: %d\nwant: %d", have, want)
		}
		i = i + 1
	}

	// Output:
	// Donald Duck 36
	// Scrooge McDuck 86
}
