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
	releaseDisconnected := make(chan struct{})
	disconnectedFinished := make(chan struct{})
	var connectedOnce sync.Once
	var releaseDisconnectedOnce sync.Once
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
				<-releaseDisconnected
				close(disconnectedFinished)
			}
		}
	})
	t.Cleanup(func() {
		releaseDisconnectedOnce.Do(func() { close(releaseDisconnected) })
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

	// Keep the first terminal callback in flight so any duplicate must remain
	// visible in the retained queue instead of racing this assertion.
	listener.eventLock.Lock()
	for _, event := range listener.eventQueue {
		if event.typ == ListenerEventDisconnected {
			t.Error("Close queued a duplicate Disconnected callback")
		}
	}
	listener.eventLock.Unlock()
	releaseDisconnectedOnce.Do(func() { close(releaseDisconnected) })
	listenerConcurrencyAwaitSignal(t, disconnectedFinished, "the Disconnected callback to return")

	if count := disconnectedCount.Load(); count != 1 {
		t.Errorf("Close delivered %d Disconnected callbacks; want exactly 1", count)
	}
	if disconnectedErr == nil {
		t.Error("Close delivered a Disconnected callback without its close error")
	}
}

func TestListenerConcurrencyCloseAfterDroppedDisconnect(t *testing.T) {
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	dispatcherDone := make(chan struct{})
	events := make(chan listenerEvent, listenerEventQueueCapacity+2)
	var releaseOnce sync.Once

	listener := &Listener{
		done:      make(chan struct{}),
		eventWake: make(chan struct{}, 1),
		eventCallback: func(event ListenerEventType, err error) {
			events <- listenerEvent{typ: event, err: err}
			if event == ListenerEventConnected {
				close(callbackStarted)
				<-releaseCallback
			}
		},
	}
	listener.reconnectCond = sync.NewCond(&listener.lock)
	go func() {
		listener.eventDispatcher()
		close(dispatcherDone)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		releaseOnce.Do(func() { close(releaseCallback) })
		listenerConcurrencyAwaitSignal(t, dispatcherDone, "the event dispatcher to stop")
	})

	listener.emitEvent(ListenerEventConnected, nil)
	listenerConcurrencyAwaitSignal(t, callbackStarted, "the initial callback to block")
	for range listenerEventQueueCapacity - 1 {
		listener.emitEvent(ListenerEventDisconnected, errors.New("intentional churn"))
		listener.emitEvent(ListenerEventReconnected, nil)
	}

	listener.eventLock.Lock()
	if len(listener.eventQueue) != listenerEventQueueCapacity-1 {
		t.Errorf("churn retained %d events; want %d", len(listener.eventQueue), listenerEventQueueCapacity-1)
	}
	for _, event := range listener.eventQueue {
		if event.typ != ListenerEventReconnected || event.barrier == nil {
			t.Errorf("saturated queue contains unprotected event %+v", event)
			break
		}
	}
	listener.eventLock.Unlock()

	// No protected reconnect can be evicted, so this transition is dropped.
	// Close must nevertheless preserve the state of the retained callback stream
	// and append its terminal Disconnected callback.
	listener.emitEvent(ListenerEventDisconnected, errors.New("dropped disconnect"))
	if err := listener.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}
	releaseOnce.Do(func() { close(releaseCallback) })
	listenerConcurrencyAwaitSignal(t, dispatcherDone, "the event dispatcher to stop")
	close(events)

	last := listenerEvent{typ: ListenerEventType(-1)}
	disconnected := 0
	for event := range events {
		last = event
		if event.typ == ListenerEventDisconnected {
			disconnected++
		}
	}
	if last.typ != ListenerEventDisconnected {
		t.Errorf("retained callback stream ended in %s; want %s", last.typ, ListenerEventDisconnected)
	}
	if disconnected != 1 {
		t.Errorf("Close delivered %d Disconnected callbacks; want exactly 1", disconnected)
	}
	if last.err != errListenerConnClosed {
		t.Errorf("terminal Disconnected callback error = %v; want %v", last.err, errListenerConnClosed)
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
	releaseReconnected := make(chan struct{})
	reconnectedFinished := make(chan struct{})
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
			reconnectedOnce.Do(func() {
				close(reconnectedExecuted)
				<-releaseReconnected
				close(reconnectedFinished)
			})
		}
	})
	var dropFirstOnce, releaseDisconnectedOnce, releaseReconnectedOnce sync.Once
	t.Cleanup(func() {
		dropFirstOnce.Do(func() { close(dropFirst) })
		releaseDisconnectedOnce.Do(func() { close(releaseDisconnected) })
		releaseReconnectedOnce.Do(func() { close(releaseReconnected) })
		select {
		case <-disconnectedStarted:
			listenerConcurrencyAwaitSignal(t, disconnectedFinished, "the blocked Disconnected callback to finish")
		default:
		}
		select {
		case <-reconnectedExecuted:
			listenerConcurrencyAwaitSignal(t, reconnectedFinished, "the blocked Reconnected callback to finish")
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
				t.Fatal("Listener.Notify closed while the Reconnected callback was running")
			}
			if notification != nil {
				t.Fatalf("got notification %#v; want reconnect loss marker", notification)
			}
			earlyMarker = true
		case <-time.After(250 * time.Millisecond):
		}
	}
	if earlyMarker {
		t.Error("reconnect loss marker became observable before the Reconnected callback returned")
	}
	releaseReconnectedOnce.Do(func() { close(releaseReconnected) })
	listenerConcurrencyAwaitSignal(t, reconnectedFinished, "the Reconnected callback to return")
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
			t.Fatal("timed out waiting for reconnect loss marker after Reconnected callback returned")
		}
	}
}

func TestListenerConcurrencyReconnectBarrierCannotBeStarved(t *testing.T) {
	const churnCallbacks = listenerEventQueueCapacity * 2

	callbackBlocked := make(chan struct{})
	releaseCallback := make(chan struct{})
	barrierChecked := make(chan bool, 1)
	dispatcherDone := make(chan struct{})
	var releaseOnce sync.Once

	var listener *Listener
	var reconnectBarrier *listenerEventBarrier
	disconnectedCallbacks := 0
	listener = &Listener{
		done:      make(chan struct{}),
		eventWake: make(chan struct{}, 1),
		eventCallback: func(event ListenerEventType, _ error) {
			switch event {
			case ListenerEventConnected:
				close(callbackBlocked)
				<-releaseCallback

			case ListenerEventDisconnected:
				disconnectedCallbacks++
				if disconnectedCallbacks == churnCallbacks {
					select {
					case <-reconnectBarrier.ready:
						barrierChecked <- false
					default:
						barrierChecked <- true
					}
					return
				}
				if disconnectedCallbacks < churnCallbacks {
					// Keep one ordinary event ahead of the pending reconnect.
					// Moving that reconnect to the tail on every churn cycle
					// must not prevent its callback barrier from ever completing.
					listener.emitEvent(ListenerEventDisconnected, errors.New("intentional churn"))
					listener.emitEvent(ListenerEventReconnected, nil)
				}
			}
		},
	}

	go func() {
		listener.eventDispatcher()
		close(dispatcherDone)
	}()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseCallback) })
		listener.closeEventQueue(false)
		listenerConcurrencyAwaitSignal(t, dispatcherDone, "the event dispatcher to stop")
	})

	listener.emitEvent(ListenerEventConnected, nil)
	listenerConcurrencyAwaitSignal(t, callbackBlocked, "the initial callback to block")
	listener.emitEvent(ListenerEventDisconnected, errors.New("intentional disconnect"))
	reconnectBarrier = listener.emitEvent(ListenerEventReconnected, nil)
	if reconnectBarrier == nil {
		t.Fatal("Reconnected event did not retain a callback barrier")
	}
	releaseOnce.Do(func() { close(releaseCallback) })

	select {
	case starved := <-barrierChecked:
		if starved {
			t.Errorf("event dispatcher ran %d later callbacks without invoking the queued Reconnected callback", churnCallbacks)
		}
	case <-time.After(listenerConcurrencyTestTimeout):
		t.Fatal("event dispatcher stopped making progress during reconnect churn")
	}
}

func TestListenerConcurrencyReconnectCoalescingPreservesInterveningDisconnect(t *testing.T) {
	listener := &Listener{
		done:          make(chan struct{}),
		eventWake:     make(chan struct{}, 1),
		eventCallback: func(ListenerEventType, error) {},
	}

	first := listener.emitEvent(ListenerEventReconnected, nil)
	listener.emitEvent(ListenerEventDisconnected, errors.New("intentional disconnect"))
	second := listener.emitEvent(ListenerEventReconnected, nil)
	if first == nil || second == nil {
		t.Fatal("Reconnected event did not retain a callback barrier")
	}
	if second == first {
		t.Error("Reconnected event was coalesced across an intervening Disconnected transition")
	}

	listener.eventLock.Lock()
	defer listener.eventLock.Unlock()
	if len(listener.eventQueue) == 0 || listener.eventQueue[len(listener.eventQueue)-1].typ != ListenerEventReconnected {
		t.Errorf("retained event queue ended in %v; want Reconnected", listener.eventQueue)
	}
}
