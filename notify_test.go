package pq

import (
	"os"
	"testing"
	"time"
)

func newTestListener(t *testing.T) *Listener {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	l, err := NewListener("")

	if err != nil {
		t.Fatal(err)
	}

	return l
}

func TestNewListener(t *testing.T) {
	l := newTestListener(t)

	defer l.Close()
}

func TestListen(t *testing.T) {
	channel := make(chan *Notification)
	l := newTestListener(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_test", channel)

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

func TestUnlisten(t *testing.T) {
	channel := make(chan *Notification)
	l := newTestListener(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_test", channel)

	if err != nil {
		t.Fatal(err)
	}

	l.Unlisten("notify_test", channel)

	_, err = db.Exec("NOTIFY notify_test")

	if err != nil {
		t.Fatal(err)
	}

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		timeout <- true
	}()

	select {
	case <-channel:
		t.Fatal("Got notification after Unlisten")

	case <-timeout:
	}
}

func TestNotifyExtra(t *testing.T) {
	channel := make(chan *Notification)
	l := newTestListener(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_test", channel)

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
