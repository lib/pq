package pq

import (
	"testing"
)

func TestNewListener(t *testing.T) {
	l, err := NewListener("dbname=pqgotest sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	defer l.Close()
}

func TestListen(t *testing.T) {
	channel := make(chan *Notification)

	l, err := NewListener("dbname=pqgotest sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err = l.Listen("notify_test", channel)

	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")

	if err != nil {
		t.Fatal(err)
	}

	n := <-channel

	if n.RelName != "notify_test" {
		t.Errorf("Notification RelName invalid: %v", n.RelName)
	}
}

func TestNotifyExtra(t *testing.T) {
	channel := make(chan *Notification)

	l, err := NewListener("dbname=pqgotest sslmode=disable")

	if err != nil {
		t.Fatal(err)
	}

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err = l.Listen("notify_test", channel)

	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test, 'something'")

	if err != nil {
		t.Fatal(err)
	}

	n := <-channel

	if n.Extra != "something" {
		t.Errorf("Notification extra invalid: %v", n.Extra)
	}
}
