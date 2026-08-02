package pq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
)

const listenerNetworkTestTimeout = 2 * time.Second

type listenerNetworkContextPinger interface {
	PingContext(context.Context) error
}

type listenerNetworkContextListener interface {
	ListenContext(context.Context, string) error
}

type listenerNetworkFixedDialer struct {
	conn net.Conn
}

func (d listenerNetworkFixedDialer) Dial(string, string) (net.Conn, error) {
	return d.conn, nil
}

func (d listenerNetworkFixedDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.conn, nil
}

func TestListenerNetworkPingContextCancelsBlackholedQuery(t *testing.T) {
	listener, queryReceived := listenerNetworkBlackhole(t)
	pinger, ok := any(listener).(listenerNetworkContextPinger)
	if !ok {
		t.Fatal("Listener has no PingContext method, so a live blackholed connection has no bounded per-operation escape")
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pinger.PingContext(ctx) }()

	listenerNetworkAwait(t, queryReceived, "Listener.PingContext did not send its query")
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Listener.PingContext returned %v after cancellation; want %v", err, context.Canceled)
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal("Listener.PingContext remained blocked after its context was canceled")
	}
}

func TestListenerNetworkListenContextCancelsBlackholedQuery(t *testing.T) {
	listener, queryReceived := listenerNetworkBlackhole(t)
	contextListener, ok := any(listener).(listenerNetworkContextListener)
	if !ok {
		t.Fatal("Listener has no ListenContext method, so LISTEN on a live blackholed connection has no bounded per-operation escape")
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- contextListener.ListenContext(ctx, "listener_network_blackhole") }()

	listenerNetworkAwait(t, queryReceived, "Listener.ListenContext did not send LISTEN")
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Listener.ListenContext returned %v after cancellation; want %v", err, context.Canceled)
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal("Listener.ListenContext remained blocked after its context was canceled")
	}
}

func TestListenerNetworkCanceledListenDoesNotPersistDesiredChannel(t *testing.T) {
	dialer := &listenerNetworkContextBlockingDialer{entered: make(chan struct{})}
	listener := NewDialListener(
		dialer,
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		time.Hour,
		time.Hour,
		nil,
	)
	t.Cleanup(func() {
		_ = listener.Close()
		listenerNetworkAwaitNotificationCloseCleanup(t, listener.Notify)
	})
	listenerNetworkAwait(t, dialer.entered, "listener did not enter its initial connection attempt")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	const channel = "listener_network_canceled_before_connect"
	if err := listener.ListenContext(ctx, channel); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ListenContext error = %v; want %v", err, context.DeadlineExceeded)
	}

	listener.lock.Lock()
	_, retained := listener.channels[channel]
	listener.lock.Unlock()
	if retained {
		t.Error("canceled ListenContext remained in the desired channel set and can subscribe on a later reconnect")
	}
}

func TestListenerNetworkFullNotifyDoesNotBlockPing(t *testing.T) {
	client, server := net.Pipe()
	connected := make(chan struct{})
	startFlood := make(chan struct{})
	flooded := make(chan struct{})
	queryReceived := make(chan struct{})
	backendDone := make(chan struct{})
	var connectedOnce sync.Once
	var startFloodOnce sync.Once
	var pingResult <-chan error

	go func() {
		defer close(backendDone)
		if !regressionReadStartupPacket(server) {
			return
		}
		if _, err := server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil)); err != nil {
			return
		}

		<-startFlood
		frame := listenerNetworkNotificationFrame()
		// Notify is pre-filled by the test. One notification is held by the
		// public forwarding loop, 32 fit in the per-connection channel, and the
		// 34th leaves ListenerConn blocked unless socket reads are decoupled from
		// notification delivery.
		for range 34 {
			if _, err := server.Write(frame); err != nil {
				return
			}
		}
		close(flooded)

		if !regressionReadFrontendMessage(server) {
			return
		}
		close(queryReceived)
		_, _ = server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.EmptyQueryResponse, nil),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil))
	}()

	listener := NewDialListener(
		listenerNetworkFixedDialer{conn: client},
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		time.Hour,
		time.Hour,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				connectedOnce.Do(func() { close(connected) })
			}
		},
	)
	t.Cleanup(func() {
		startFloodOnce.Do(func() { close(startFlood) })
		_ = listener.Close()
		_ = server.Close()
		listenerNetworkAwaitCleanup(t, backendDone, "notification backend did not stop")
		if pingResult != nil {
			listenerNetworkAwaitErrorCleanup(t, pingResult, "Ping remained blocked after listener cleanup")
		}
	})

	listenerNetworkAwait(t, connected, "listener did not establish its initial connection")
	for range cap(listener.Notify) {
		listener.Notify <- &Notification{Channel: "pre-filled"}
	}
	startFloodOnce.Do(func() { close(startFlood) })
	listenerNetworkAwait(t, flooded, "backend could not saturate listener notification delivery")

	result := make(chan error, 1)
	pingResult = result
	go func() { result <- listener.Ping() }()
	listenerNetworkAwait(t, queryReceived, "Listener.Ping did not send its query")

	select {
	case err := <-result:
		pingResult = nil
		if err != nil {
			t.Fatalf("Listener.Ping returned %v with a full Notify channel", err)
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal("a full Notify channel prevented Listener from processing the Ping response")
	}
}

func TestListenerNetworkFullNotifyDoesNotBlockReconnect(t *testing.T) {
	client, server := net.Pipe()
	dialer := &listenerNetworkRetryDialer{
		first:        client,
		retryStarted: make(chan struct{}),
	}
	connected := make(chan struct{})
	startFlood := make(chan struct{})
	flooded := make(chan struct{})
	backendDone := make(chan struct{})
	var connectedOnce sync.Once
	var startFloodOnce sync.Once

	go func() {
		defer close(backendDone)
		defer server.Close()
		if !regressionReadStartupPacket(server) {
			return
		}
		if _, err := server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil)); err != nil {
			return
		}

		<-startFlood
		frame := listenerNetworkNotificationFrame()
		for range 34 {
			if _, err := server.Write(frame); err != nil {
				return
			}
		}
		close(flooded)
		// A closed peer must be observed and trigger a reconnect even while the
		// application is not draining the already-full public Notify channel.
	}()

	listener := NewDialListener(
		dialer,
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		5*time.Millisecond,
		5*time.Millisecond,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				connectedOnce.Do(func() { close(connected) })
			}
		},
	)
	t.Cleanup(func() {
		startFloodOnce.Do(func() { close(startFlood) })
		_ = listener.Close()
		_ = server.Close()
		listenerNetworkAwaitCleanup(t, backendDone, "disconnecting notification backend did not stop")
	})

	listenerNetworkAwait(t, connected, "listener did not establish its initial connection")
	for range cap(listener.Notify) {
		listener.Notify <- &Notification{Channel: "pre-filled"}
	}
	startFloodOnce.Do(func() { close(startFlood) })
	listenerNetworkAwait(t, flooded, "backend could not saturate listener notification delivery")
	listenerNetworkAwait(t, dialer.retryStarted, "a full Notify channel prevented Listener from detecting disconnect and reconnecting")
}

func TestListenerNetworkInvalidReconnectRangeIsNormalized(t *testing.T) {
	dialer := &listenerNetworkContextBlockingDialer{entered: make(chan struct{})}
	listener := NewDialListener(
		dialer,
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		25*time.Millisecond,
		0,
		nil,
	)
	t.Cleanup(func() {
		_ = listener.Close()
		listenerNetworkAwaitNotificationCloseCleanup(t, listener.Notify)
	})

	listenerNetworkAwait(t, dialer.entered, "listener did not enter its initial connection attempt")
	if listener.maxReconnectInterval < listener.minReconnectInterval {
		t.Fatalf(
			"invalid reconnect range was retained: min=%s max=%s; after one failure this clamps retries to a tight zero-delay loop",
			listener.minReconnectInterval,
			listener.maxReconnectInterval,
		)
	}
}

func listenerNetworkBlackhole(t *testing.T) (*Listener, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	connected := make(chan struct{})
	queryReceived := make(chan struct{})
	backendDone := make(chan struct{})
	var connectedOnce sync.Once

	go func() {
		defer close(backendDone)
		if !regressionReadStartupPacket(server) {
			return
		}
		if _, err := server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil)); err != nil {
			return
		}
		if !regressionReadFrontendMessage(server) {
			return
		}
		close(queryReceived)

		// Read forever instead of replying. Closing the client during context
		// cancellation or test cleanup releases this goroutine.
		var scratch [1]byte
		_, _ = server.Read(scratch[:])
	}()

	listener := NewDialListener(
		listenerNetworkFixedDialer{conn: client},
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		time.Hour,
		time.Hour,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				connectedOnce.Do(func() { close(connected) })
			}
		},
	)
	t.Cleanup(func() {
		_ = listener.Close()
		_ = server.Close()
		listenerNetworkAwaitCleanup(t, backendDone, "blackhole backend did not stop")
	})
	listenerNetworkAwait(t, connected, "listener did not establish its initial connection")
	return listener, queryReceived
}

func listenerNetworkNotificationFrame() []byte {
	payload := binary.BigEndian.AppendUint32(nil, 42)
	payload = append(payload, "listener_network_flood\x00payload\x00"...)
	return regressionBackendFrame(proto.NotificationResponse, payload)
}

type listenerNetworkRetryDialer struct {
	first        net.Conn
	calls        atomic.Int32
	retryOnce    sync.Once
	retryStarted chan struct{}
}

func (d *listenerNetworkRetryDialer) dial() (net.Conn, error) {
	if d.calls.Add(1) == 1 {
		return d.first, nil
	}
	d.retryOnce.Do(func() { close(d.retryStarted) })
	return nil, errors.New("intentional reconnect failure")
}

func (d *listenerNetworkRetryDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *listenerNetworkRetryDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

type listenerNetworkContextBlockingDialer struct {
	entered     chan struct{}
	enteredOnce sync.Once
}

func (d *listenerNetworkContextBlockingDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("unexpected legacy Dial call")
}

func (d *listenerNetworkContextBlockingDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return nil, errors.New("unexpected legacy DialTimeout call")
}

func (d *listenerNetworkContextBlockingDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	d.enteredOnce.Do(func() { close(d.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func listenerNetworkAwait(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal(failure)
	}
}

func listenerNetworkAwaitCleanup(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(listenerNetworkTestTimeout):
		t.Error(failure)
	}
}

func listenerNetworkAwaitErrorCleanup(t *testing.T, result <-chan error, failure string) {
	t.Helper()
	select {
	case <-result:
	case <-time.After(listenerNetworkTestTimeout):
		t.Error(failure)
	}
}

func listenerNetworkAwaitNotificationCloseCleanup(t *testing.T, notifications <-chan *Notification) {
	t.Helper()
	select {
	case _, ok := <-notifications:
		if ok {
			t.Error("listener emitted an unexpected notification during cleanup")
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Error("listener Notify channel did not close during cleanup")
	}
}
