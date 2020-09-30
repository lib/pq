// +build go1.10

package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestNewConnector_WorksWithOpenDB(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Connect(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnector_Driver(t *testing.T) {
	name := ""
	c, err := NewConnector(name)
	if err != nil {
		t.Fatal(err)
	}
	db, err := c.Driver().Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.(driver.ConnBeginTx).BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
}

func TestNewConnectorKeepalive(t *testing.T) {
	c, err := NewConnector("keepalives=1 keepalives_interval=10")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	// database/sql might not call our Open at all unless we do something with
	// the connection
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()

	d, _ := c.dialer.(defaultDialer)
	want := 10 * time.Second
	if want != d.d.KeepAlive {
		t.Fatalf("expected: %v, got: %v", want, d.d.KeepAlive)
	}
}

func TestKeepalive(t *testing.T) {
	var tt = map[string]struct {
		input values
		want  time.Duration
	}{
		"keepalives on":                 {values{"keepalives": "1"}, 0},
		"keepalives on by default":      {nil, 0},
		"keepalives off":                {values{"keepalives": "0"}, -1},
		"keepalives_interval 5 seconds": {values{"keepalives_interval": "5"}, 5 * time.Second},
		"keepalives_interval default":   {values{"keepalives_interval": "0"}, 0},
		"keepalives_interval off":       {values{"keepalives_interval": "-1"}, -1 * time.Second},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			got, err := keepalive(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want != got {
				t.Fatalf("expected: %v, got: %v", tc.want, got)
			}
		})
	}
}

func TestKeepaliveError(t *testing.T) {
	var tt = map[string]struct {
		input values
		want  error
	}{
		"keepalives_interval whitespace": {values{"keepalives_interval": " "}, strconv.ErrSyntax},
		"keepalives_interval float":      {values{"keepalives_interval": "1.1"}, strconv.ErrSyntax},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			_, err := keepalive(tc.input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected: %v, got: %v", tc.want, err)
			}
		})
	}
}
