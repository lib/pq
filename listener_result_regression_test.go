package pq

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type regressionGatedFailDialer struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (d *regressionGatedFailDialer) fail() (net.Conn, error) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return nil, errors.New("regression dial failure")
}

func (d *regressionGatedFailDialer) Dial(string, string) (net.Conn, error) {
	return d.fail()
}

func (d *regressionGatedFailDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.fail()
}

func TestRegressionListenerCloseInterruptsReconnectBackoff(t *testing.T) {
	d := &regressionGatedFailDialer{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	closeResult := make(chan error, 1)

	var l *Listener
	l = NewDialListener(d, "host=127.0.0.1 port=1 sslmode=disable", 300*time.Millisecond, 300*time.Millisecond,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnectionAttemptFailed {
				// Closing from the callback makes the ordering deterministic: Close
				// happens immediately before listenerConnLoop enters its backoff.
				closeResult <- l.Close()
			}
		})

	select {
	case <-d.entered:
	case <-time.After(time.Second):
		t.Fatal("listener did not attempt its initial connection")
	}
	close(d.release)

	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener callback did not close the listener")
	}

	closedPromptly := false
	select {
	case _, ok := <-l.Notify:
		closedPromptly = !ok
	case <-time.After(50 * time.Millisecond):
	}

	// Let the old implementation finish sleeping before reporting the
	// failure, so this regression test never strands its listener goroutine.
	if !closedPromptly {
		select {
		case _, ok := <-l.Notify:
			if ok {
				t.Fatal("unexpected notification while closing listener")
			}
		case <-time.After(time.Second):
			t.Fatal("listener did not eventually stop")
		}
		t.Error("Close did not interrupt reconnect backoff and close Notify promptly")
	}
}

func TestRegressionListenerCloseWithFullNotify(t *testing.T) {
	pqtest.SkipCockroach(t) // LISTEN is not supported.

	events := make(chan ListenerEventType, 16)
	closeResult := make(chan error, 1)
	done := make(chan struct{})

	var l *Listener
	l = &Listener{
		dsn:                  pqtest.DSN(""),
		minReconnectInterval: 10 * time.Millisecond,
		maxReconnectInterval: 10 * time.Millisecond,
		dialer:               defaultDialer{},
		channels:             make(map[string]struct{}),
		Notify:               make(chan *Notification, 32),
		eventCallback: func(event ListenerEventType, _ error) {
			if event == ListenerEventReconnected {
				// listenerConnLoop sends the reconnect marker immediately after
				// this callback. Close while Notify is still full.
				closeResult <- l.Close()
			}
			events <- event
		},
	}
	l.reconnectCond = sync.NewCond(&l.lock)
	go func() {
		l.listenerMain()
		close(done)
	}()

	t.Cleanup(func() {
		_ = l.Close()
		drained := make(chan struct{})
		go func() {
			for range l.Notify {
			}
			close(drained)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("listener goroutine did not stop during cleanup")
		}
		select {
		case <-drained:
		case <-time.After(2 * time.Second):
			t.Error("Notify did not close during cleanup")
		}
	})

	regressionWaitListenerEvent(t, events, ListenerEventConnected)
	for range cap(l.Notify) {
		l.Notify <- &Notification{Channel: "buffered"}
	}

	l.lock.Lock()
	cn := l.cn
	l.lock.Unlock()
	if cn == nil {
		t.Fatal("listener has no connection after connected event")
	}
	if err := cn.Close(); err != nil {
		t.Fatalf("close underlying listener connection: %v", err)
	}

	regressionWaitListenerEvent(t, events, ListenerEventDisconnected)
	regressionWaitListenerEvent(t, events, ListenerEventReconnected)
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect callback did not close listener")
	}

	select {
	case <-done:
		// Notify can be closed while it still contains buffered values; a
		// consumer must not be required to drain it to let Close finish.
	case <-time.After(100 * time.Millisecond):
		t.Error("listener remained blocked sending to a full Notify after Close")
	}
}

func regressionWaitListenerEvent(t *testing.T, events <-chan ListenerEventType, want ListenerEventType) {
	t.Helper()
	select {
	case have := <-events:
		if have != want {
			t.Fatalf("listener event: got %s, want %s", have, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for listener event %s", want)
	}
}

func TestRegressionListenerResyncFailureClearsConnection(t *testing.T) {
	f := pqtest.NewFake(t, func(f pqtest.Fake, cn net.Conn) {
		defer cn.Close()
		f.Startup(cn, nil)
		code, _, ok := f.ReadMsg(cn)
		if !ok {
			return
		}
		if code != proto.Query {
			t.Errorf("got frontend message %s, want Query", code)
			return
		}
		f.WriteMsg(cn, proto.ErrorResponse, "SERROR\x00C42601\x00Mforced resync failure\x00\x00")
		f.WriteMsg(cn, proto.ReadyForQuery, "I")
	})
	defer f.Close()

	l := &Listener{
		dsn:      f.DSN() + " sslmode=disable",
		dialer:   defaultDialer{},
		channels: map[string]struct{}{`requires "quoting"`: {}},
		Notify:   make(chan *Notification, 1),
	}
	l.reconnectCond = sync.NewCond(&l.lock)
	defer l.Close()

	if err := l.connect(); err == nil {
		t.Fatal("resync unexpectedly succeeded")
	}
	if l.cn != nil {
		t.Error("failed resync retained a stale ListenerConn")
	}
}

func TestRegressionMergeRowsAffected(t *testing.T) {
	result, tag, err := (&conn{}).parseComplete("MERGE 7")
	if err != nil {
		t.Fatalf("parseComplete: %v", err)
	}
	if tag != "MERGE" {
		t.Errorf("command tag: got %q, want %q", tag, "MERGE")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != 7 {
		t.Errorf("RowsAffected: got %d, want 7", rows)
	}
}

func TestRegressionUnlimitedTypeModifiers(t *testing.T) {
	for _, typ := range []oid.Oid{oid.T_varchar, oid.T_varbit} {
		t.Run(oid.TypeName[typ], func(t *testing.T) {
			length, ok := (fieldDesc{OID: typ, Mod: -1}).Length()
			if !ok || length != math.MaxInt64 {
				t.Errorf("Length: got (%d, %t), want (%d, true)", length, ok, int64(math.MaxInt64))
			}
		})
	}

	t.Run("NUMERIC", func(t *testing.T) {
		precision, scale, ok := (fieldDesc{OID: oid.T_numeric, Mod: -1}).PrecisionScale()
		if ok || precision != 0 || scale != 0 {
			t.Errorf("PrecisionScale: got (%d, %d, %t), want (0, 0, false)", precision, scale, ok)
		}
	})
}

type regressionCancelDialer struct {
	called chan struct{}
}

func (d *regressionCancelDialer) failedDial() (net.Conn, error) {
	select {
	case d.called <- struct{}{}:
	default:
	}
	return nil, errors.New("unexpected cancellation dial")
}

func (d *regressionCancelDialer) Dial(string, string) (net.Conn, error) {
	return d.failedDial()
}

func (d *regressionCancelDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.failedDial()
}

func TestRegressionStmtBadConnStopsCancelWatcher(t *testing.T) {
	d := &regressionCancelDialer{called: make(chan struct{}, 1)}
	cn := &conn{
		dialer: d,
		cfg: Config{
			Host:    "127.0.0.1",
			Port:    5432,
			SSLMode: SSLModeDisable,
		},
	}
	cn.err.set(errors.New("connection is already bad"))

	ctx, cancel := context.WithCancel(context.Background())
	_, err := (&stmt{cn: cn}).QueryContext(ctx, nil)
	if err != driver.ErrBadConn {
		cancel()
		t.Fatalf("QueryContext: got %v, want %v", err, driver.ErrBadConn)
	}

	// Once QueryContext has returned there is no operation left to cancel.
	// A leaked watcher will observe this and try to open a cancellation
	// connection; the fixed path finishes the watcher before returning.
	cancel()
	select {
	case <-d.called:
		t.Error("cancelled context triggered a cancellation request after QueryContext returned")
	case <-time.After(100 * time.Millisecond):
	}
}
