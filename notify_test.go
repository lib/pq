package pq

import (
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func wantNotification(t *testing.T, ch <-chan *Notification, channel string, extra string) {
	t.Helper()
	select {
	case <-time.After(100 * time.Millisecond):
		if channel != "(empty)" {
			t.Fatalf("wantNotification: timeout")
		}
	case n := <-ch:
		if channel == "(empty)" && extra == "" {
			t.Fatalf("wantNotification: unexpected notification %v", n)
		}
		if n == nil {
			n = &Notification{Channel: "(nil)"}
		}
		if n.Channel != channel || n.Extra != extra {
			t.Fatalf("wantNotification: wrong notification\nhave: %q, %q\nwant: %q, %q", n.Channel, n.Extra, channel, extra)
		}
	}
}
func wantEvent(t *testing.T, ch <-chan ListenerEventType, want ListenerEventType) {
	t.Helper()
	select {
	case <-time.After(100 * time.Millisecond):
		panic("wantEvent: timeout")
	case e := <-ch:
		if e != want {
			t.Fatalf("wantEvent: wrong event\nhave: %v\nwant: %v", e, want)
		}
	}
}
func newTestListenerConn(t *testing.T) (*ListenerConn, <-chan *Notification) {
	t.Helper()
	pqtest.SkipCockroach(t) // Not supported

	ch := make(chan *Notification)
	l, err := NewListenerConn("", ch)
	if err != nil {
		t.Fatal(err)
	}
	return l, ch
}
func newTestListenerTimeout(t *testing.T, min time.Duration, max time.Duration) (*Listener, <-chan ListenerEventType) {
	pqtest.SkipCockroach(t) // Not supported
	t.Helper()
	var (
		ch = make(chan ListenerEventType, 16)
		l  = NewListener("", min, max, func(t ListenerEventType, err error) { ch <- t })
	)
	wantEvent(t, ch, ListenerEventConnected)
	return l, ch
}
func newTestListener(t *testing.T) (*Listener, <-chan ListenerEventType) {
	return newTestListenerTimeout(t, time.Hour, time.Hour)
}
func channelName() string {
	b := []byte("pqtest")
	sel := "abcdefghjkmnpqrstuvwxyz"
	m := big.NewInt(int64(len(sel)))
	for range 10 {
		n, _ := rand.Int(rand.Reader, m)
		b = append(b, sel[n.Int64()])
	}
	return string(b)
}

func TestListenerConnListen(t *testing.T) {
	t.Parallel()
	l, ch := newTestListenerConn(t)
	defer l.Close()
	n := channelName()

	ok, err := l.Listen(n)
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, pqtest.MustDB(t), "notify "+n)
	wantNotification(t, ch, n, "")
}

func TestListenerConnUnlisten(t *testing.T) {
	t.Parallel()
	l, ch := newTestListenerConn(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	ok, err := l.Listen(n)
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, ch, n, "")

	ok, err = l.Unlisten(n)
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, ch, "(empty)", "")
}

func TestListenerConnUnlistenAll(t *testing.T) {
	t.Parallel()
	l, ch := newTestListenerConn(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	ok, err := l.Listen(n)
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, ch, n, "")

	ok, err = l.UnlistenAll()
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, ch, "(empty)", "")
}

func TestListenerConnClose(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerConn(t)
	defer l.Close()

	err := l.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = l.Close()
	if err != errListenerConnClosed {
		t.Fatalf("expected errListenerConnClosed; got %v", err)
	}
}

func TestListernerConnPing(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerConn(t)
	defer l.Close()
	err := l.Ping()
	if err != nil {
		t.Fatal(err)
	}
	err = l.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = l.Ping()
	if err != errListenerConnClosed {
		t.Fatalf("expected errListenerConnClosed; got %v", err)
	}
}

// Test for deadlock where a query fails while another one is queued
func TestListenerConnExecDeadlock(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerConn(t)
	defer l.Close()

	var done atomic.Int32
	go func() {
		l.ExecSimpleQuery("select pg_sleep(0.2)")
		done.Add(1)
	}()
	runtime.Gosched()
	go func() {
		l.ExecSimpleQuery("select 1")
		done.Add(1)
	}()
	runtime.Gosched() // Give the above goroutine some time to get into position.
	l.Close()         // Calls Close on the net.Conn; equivalent to a network failure.

	time.Sleep(200 * time.Millisecond)
	if done.Load() != 2 {
		t.Fatal("timed out")
	}
}

// Test for ListenerConn being closed while a slow query is executing
func TestListenerConnCloseWhileQueryIsExecuting(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerConn(t)
	defer l.Close()

	var done atomic.Int32
	go func() {
		sent, err := l.ExecSimpleQuery("select pg_sleep(0.2)")
		if sent {
			panic("expected sent=false")
		}
		if err == nil { // Could be any of a number of errors.
			panic("expected error")
		}
		done.Add(1)
	}()
	runtime.Gosched() // Give the above goroutine some time to get into position.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	if done.Load() != 1 {
		t.Fatal("timed out")
	}
}

func TestListenerNotifyExtra(t *testing.T) {
	t.Parallel()
	l, ch := newTestListenerConn(t)
	defer l.Close()
	n := channelName()

	ok, err := l.Listen(n)
	if !ok || err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, pqtest.MustDB(t), fmt.Sprintf("notify %s, 'something'", n))
	wantNotification(t, ch, n, "something")
}

func TestListenerListen(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	err := l.Listen(n)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, n, "")
}

func TestListenerUnlisten(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	err := l.Listen(n)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)

	err = l.Unlisten(n)
	if err != nil {
		t.Fatal(err)
	}

	wantNotification(t, l.Notify, n, "")

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, "(empty)", "")
}

func TestListenerUnlistenAll(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	err := l.Listen(n)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)

	err = l.UnlistenAll()
	if err != nil {
		t.Fatal(err)
	}

	wantNotification(t, l.Notify, n, "")
	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, "(empty)", "")
}

func TestListenerFailedQuery(t *testing.T) {
	t.Parallel()
	l, ch := newTestListener(t)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	err := l.Listen(n)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, n, "")

	// shouldn't cause a disconnect
	ok, err := l.cn.ExecSimpleQuery("SELECT error")
	if !ok {
		t.Fatalf("could not send query to server: %v", err)
	}
	_, ok = err.(PGError)
	if !ok {
		t.Fatalf("unexpected error %v", err)
	}

	select {
	case <-time.After(100 * time.Millisecond):
	case e := <-ch:
		t.Fatalf("unexpected event %v", e)
	}

	// Should still work.
	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, n, "")
}

func TestListenerReconnect(t *testing.T) {
	t.Parallel()
	l, ch := newTestListenerTimeout(t, 20*time.Millisecond, time.Hour)
	defer l.Close()
	db := pqtest.MustDB(t)
	n := channelName()

	err := l.Listen(n)
	if err != nil {
		t.Fatal(err)
	}

	pqtest.Exec(t, db, "notify "+n)
	wantNotification(t, l.Notify, n, "")

	// Kill the connection and make sure it comes back up.
	ok, err := l.cn.ExecSimpleQuery("select pg_terminate_backend(pg_backend_pid())")
	if ok {
		t.Fatalf("could not kill the connection: %v", err)
	}
	// PostgreSQL, pgbouncer, and pgpool all use different errors.
	if !pqtest.ErrorContains(err, `or:EOF|pq: server conn crashed? (08P01)|pq: unable to forward message to frontend (XX000)`) {
		t.Fatalf("unexpected error %T: %[1]s", err)
	}

	wantEvent(t, ch, ListenerEventDisconnected)
	wantEvent(t, ch, ListenerEventReconnected)

	// Should still work.
	pqtest.Exec(t, db, "notify "+n)
	// Should get nil after Reconnected.
	wantNotification(t, l.Notify, "(nil)", "")
	wantNotification(t, l.Notify, n, "")
}

func TestListenerClose(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerTimeout(t, 20*time.Millisecond, time.Hour)
	defer l.Close()

	err := l.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = l.Close()
	if err != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed; got %v", err)
	}
}

func TestListenerPing(t *testing.T) {
	t.Parallel()
	l, _ := newTestListenerTimeout(t, 20*time.Millisecond, time.Hour)
	defer l.Close()

	if err := l.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Ping(); err != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed; got %v", err)
	}
}

func TestConnectorWithNotificationHandler_Simple(t *testing.T) {
	pqtest.SkipCockroach(t) // Not supported

	b, err := NewConnector("")
	if err != nil {
		t.Fatal(err)
	}
	var notification *Notification
	// Make connector w/ handler to set the local var
	c := ConnectorWithNotificationHandler(b, func(n *Notification) { notification = n })
	sendNotification(c, t, "Test notification #1")
	if notification == nil || notification.Extra != "Test notification #1" {
		t.Fatalf("Expected notification w/ message, got %v", notification)
	}
	// Unset the handler on the same connector
	prevC := c
	if c = ConnectorWithNotificationHandler(c, nil); c != prevC {
		t.Fatalf("Expected to not create new connector but did")
	}
	sendNotification(c, t, "Test notification #2")
	if notification == nil || notification.Extra != "Test notification #1" {
		t.Fatalf("Expected notification to not change, got %v", notification)
	}
	// Set it back on the same connector
	if c = ConnectorWithNotificationHandler(c, func(n *Notification) { notification = n }); c != prevC {
		t.Fatal("Expected to not create new connector but did")
	}
	sendNotification(c, t, "Test notification #3")
	if notification == nil || notification.Extra != "Test notification #3" {
		t.Fatalf("Expected notification w/ message, got %v", notification)
	}
}

func sendNotification(c driver.Connector, t *testing.T, escapedNotification string) {
	db := sql.OpenDB(c)
	defer db.Close()
	sql := fmt.Sprintf("LISTEN foo; NOTIFY foo, '%s';", escapedNotification)
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}
}
