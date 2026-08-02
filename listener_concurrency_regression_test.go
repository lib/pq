package pq

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
)

const (
	listenerConcurrencyTestTimeout = 2 * time.Second
	listenerConcurrencyEventBound  = 64
	listenerConcurrencyFailCount   = 128
	listenerConcurrencyDSN         = "host=listener-concurrency.invalid port=1 user=test dbname=test sslmode=disable connect_timeout=0"
)

var errListenerConcurrencyDial = errors.New("intentional listener concurrency dial failure")

type listenerConcurrencyFailDialer struct {
	mu       sync.Mutex
	calls    int
	blockAt  int
	blocked  chan struct{}
	release  chan struct{}
	blockOne sync.Once
}

func (d *listenerConcurrencyFailDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *listenerConcurrencyFailDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *listenerConcurrencyFailDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.mu.Unlock()

	if call < d.blockAt {
		return nil, errListenerConcurrencyDial
	}
	d.blockOne.Do(func() { close(d.blocked) })
	select {
	case <-d.release:
		return nil, errListenerConcurrencyDial
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type listenerConcurrencySequenceDialer struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (d *listenerConcurrencySequenceDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *listenerConcurrencySequenceDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *listenerConcurrencySequenceDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	if len(d.conns) == 0 {
		d.mu.Unlock()
		return nil, errListenerConcurrencyDial
	}
	cn := d.conns[0]
	d.conns[0] = nil
	d.conns = d.conns[1:]
	d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		_ = cn.Close()
		return nil, err
	}
	return cn, nil
}

func listenerConcurrencyBackend(server net.Conn, release <-chan struct{}) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer server.Close()
		if !regressionReadStartupPacket(server) {
			done <- io.ErrUnexpectedEOF
			return
		}
		_, err := server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil))
		if err != nil {
			done <- err
			return
		}
		if release != nil {
			<-release
			done <- nil
			return
		}
		_, err = io.Copy(io.Discard, server)
		if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
			err = nil
		}
		done <- err
	}()
	return done
}

func listenerConcurrencyAwaitSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(listenerConcurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func listenerConcurrencyAwaitBackend(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	case <-time.After(listenerConcurrencyTestTimeout):
		t.Errorf("timed out waiting for %s", what)
	}
}

func listenerConcurrencyAwaitNotifyClose(t *testing.T, notify <-chan *Notification) {
	t.Helper()
	timer := time.NewTimer(listenerConcurrencyTestTimeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-notify:
			if !ok {
				return
			}
		case <-timer.C:
			t.Error("timed out waiting for Listener.Notify to close")
			return
		}
	}
}

func listenerConcurrencyAwaitQueuedEvent(t *testing.T, listener *Listener, want ListenerEventType) {
	t.Helper()
	deadline := time.Now().Add(listenerConcurrencyTestTimeout)
	for {
		listener.eventLock.Lock()
		found := false
		for _, event := range listener.eventQueue {
			if event.typ == want {
				found = true
				break
			}
		}
		listener.eventLock.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for queued listener event %s", want)
		}
		runtime.Gosched()
	}
}

func TestListenerConcurrencyCallbackBacklogIsBounded(t *testing.T) {
	dialer := &listenerConcurrencyFailDialer{
		blockAt: listenerConcurrencyFailCount + 1,
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackFinished := make(chan struct{})
	var callbackOnce sync.Once

	listener := NewDialListener(dialer, listenerConcurrencyDSN, 0, 0, func(event ListenerEventType, _ error) {
		if event == ListenerEventConnectionAttemptFailed {
			callbackOnce.Do(func() {
				close(callbackStarted)
				<-releaseCallback
				close(callbackFinished)
			})
		}
	})
	var releaseDialerOnce, releaseCallbackOnce sync.Once
	t.Cleanup(func() {
		_ = listener.Close()
		releaseDialerOnce.Do(func() { close(dialer.release) })
		releaseCallbackOnce.Do(func() { close(releaseCallback) })
		select {
		case <-callbackStarted:
			listenerConcurrencyAwaitSignal(t, callbackFinished, "the blocked callback to finish")
		default:
		}
		listenerConcurrencyAwaitNotifyClose(t, listener.Notify)
	})

	listenerConcurrencyAwaitSignal(t, callbackStarted, "the first connection-failure callback")
	listenerConcurrencyAwaitSignal(t, dialer.blocked, "the fail-fast retry limit")

	listener.eventLock.Lock()
	queued := len(listener.eventQueue)
	listener.eventLock.Unlock()
	if queued > listenerConcurrencyEventBound {
		t.Errorf("blocked callback retained %d queued events; want at most %d", queued, listenerConcurrencyEventBound)
	}
}

func TestListenerConcurrencyCloseEmitsOneDisconnectedEvent(t *testing.T) {
	client, server := net.Pipe()
	backendDone := listenerConcurrencyBackend(server, nil)
	dialer := &listenerConcurrencySequenceDialer{conns: []net.Conn{client}}
	connected := make(chan struct{})
	disconnected := make(chan struct{})
	var connectedOnce sync.Once
	var disconnectedCount atomic.Int32
	var disconnectedErr error

	listener := NewDialListener(dialer, listenerConcurrencyDSN, time.Hour, time.Hour, func(event ListenerEventType, err error) {
		switch event {
		case ListenerEventConnected:
			connectedOnce.Do(func() { close(connected) })
		case ListenerEventDisconnected:
			if disconnectedCount.Add(1) == 1 {
				disconnectedErr = err
				close(disconnected)
			}
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		_ = client.Close()
		_ = server.Close()
		listenerConcurrencyAwaitBackend(t, backendDone, "the stable close backend")
		listenerConcurrencyAwaitNotifyClose(t, listener.Notify)
	})

	listenerConcurrencyAwaitSignal(t, connected, "the initial Connected callback")
	if err := listener.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}
	listenerConcurrencyAwaitSignal(t, disconnected, "the documented Disconnected callback from Close")
	listenerConcurrencyAwaitNotifyClose(t, listener.Notify)
	listenerConcurrencyAwaitBackend(t, backendDone, "the stable close backend")

	if count := disconnectedCount.Load(); count != 1 {
		t.Errorf("Close delivered %d Disconnected callbacks; want exactly 1", count)
	}
	if disconnectedErr == nil {
		t.Error("Close delivered a Disconnected callback without its close error")
	}
}

func TestListenerConcurrencyReconnectMarkerFollowsCallback(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	secondClient, secondServer := net.Pipe()
	dropFirst := make(chan struct{})
	firstBackendDone := listenerConcurrencyBackend(firstServer, dropFirst)
	secondBackendDone := listenerConcurrencyBackend(secondServer, nil)
	dialer := &listenerConcurrencySequenceDialer{conns: []net.Conn{firstClient, secondClient}}
	connected := make(chan struct{})
	disconnectedStarted := make(chan struct{})
	releaseDisconnected := make(chan struct{})
	disconnectedFinished := make(chan struct{})
	reconnectedExecuted := make(chan struct{})
	var connectedOnce, disconnectedOnce, reconnectedOnce sync.Once

	listener := NewDialListener(dialer, listenerConcurrencyDSN, 0, 0, func(event ListenerEventType, _ error) {
		switch event {
		case ListenerEventConnected:
			connectedOnce.Do(func() { close(connected) })
		case ListenerEventDisconnected:
			disconnectedOnce.Do(func() {
				close(disconnectedStarted)
				<-releaseDisconnected
				close(disconnectedFinished)
			})
		case ListenerEventReconnected:
			reconnectedOnce.Do(func() { close(reconnectedExecuted) })
		}
	})
	var dropFirstOnce, releaseDisconnectedOnce sync.Once
	t.Cleanup(func() {
		dropFirstOnce.Do(func() { close(dropFirst) })
		releaseDisconnectedOnce.Do(func() { close(releaseDisconnected) })
		select {
		case <-disconnectedStarted:
			listenerConcurrencyAwaitSignal(t, disconnectedFinished, "the blocked Disconnected callback to finish")
		default:
		}
		_ = listener.Close()
		_ = firstClient.Close()
		_ = firstServer.Close()
		_ = secondClient.Close()
		_ = secondServer.Close()
		listenerConcurrencyAwaitBackend(t, firstBackendDone, "the first reconnect backend")
		listenerConcurrencyAwaitBackend(t, secondBackendDone, "the second reconnect backend")
		listenerConcurrencyAwaitNotifyClose(t, listener.Notify)
	})

	listenerConcurrencyAwaitSignal(t, connected, "the initial Connected callback")
	dropFirstOnce.Do(func() { close(dropFirst) })
	listenerConcurrencyAwaitSignal(t, disconnectedStarted, "the blocked Disconnected callback")
	listenerConcurrencyAwaitQueuedEvent(t, listener, ListenerEventReconnected)

	earlyMarker := false
	select {
	case notification, ok := <-listener.Notify:
		if !ok {
			t.Fatal("Listener.Notify closed during reconnect")
		}
		if notification != nil {
			t.Fatalf("got unexpected notification before Reconnected callback: %#v", notification)
		}
		earlyMarker = true
	case <-time.After(250 * time.Millisecond):
	}
	if earlyMarker {
		t.Error("reconnect loss marker became observable before the Reconnected callback executed")
	}

	releaseDisconnectedOnce.Do(func() { close(releaseDisconnected) })
	listenerConcurrencyAwaitSignal(t, reconnectedExecuted, "the Reconnected callback")
	if !earlyMarker {
		select {
		case notification, ok := <-listener.Notify:
			if !ok {
				t.Fatal("Listener.Notify closed before the reconnect loss marker")
			}
			if notification != nil {
				t.Fatalf("got notification %#v; want reconnect loss marker", notification)
			}
		case <-time.After(listenerConcurrencyTestTimeout):
			t.Fatal("timed out waiting for reconnect loss marker after Reconnected callback")
		}
	}
}
