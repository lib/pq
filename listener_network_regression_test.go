package pq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
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

type listenerNetworkBlockingWriteConn struct {
	net.Conn
	writeStarted chan struct{}
	releaseWrite <-chan struct{}
	writeOnce    sync.Once
}

type listenerNetworkCallbackDialer struct {
	conn         net.Conn
	calls        atomic.Int32
	firstEntered chan struct{}
	releaseFirst <-chan struct{}
}

func (d *listenerNetworkCallbackDialer) dial() (net.Conn, error) {
	switch d.calls.Add(1) {
	case 1:
		close(d.firstEntered)
		<-d.releaseFirst
		return nil, errors.New("intentional callback reconnect failure")
	case 2:
		return nil, errors.New("intentional second callback reconnect failure")
	case 3:
		return d.conn, nil
	default:
		return nil, errors.New("unexpected callback reconnect attempt")
	}
}

func (d *listenerNetworkCallbackDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *listenerNetworkCallbackDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func (c *listenerNetworkBlockingWriteConn) Write([]byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	<-c.releaseWrite
	return 0, net.ErrClosed
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

func TestListenerNetworkProtocolFailureDoesNotBlockClose(t *testing.T) {
	clientRaw, server := net.Pipe()
	releaseWrite := make(chan struct{})
	var releaseOnce sync.Once
	client := &listenerNetworkBlockingWriteConn{
		Conn:         clientRaw,
		writeStarted: make(chan struct{}),
		releaseWrite: releaseWrite,
	}
	notifications := make(chan *Notification)
	listener := startListenerConn(&conn{
		c:   client,
		buf: bufio.NewReader(client),
	}, notifications)

	malformedWritten := make(chan struct{})
	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		defer server.Close()
		_, _ = server.Write(regressionBackendFrame(proto.ParseComplete, nil))
		close(malformedWritten)
		<-releaseWrite
	}()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseWrite) })
		_ = clientRaw.Close()
		_ = server.Close()
		listenerNetworkAwaitCleanup(t, backendDone, "malformed Listener backend did not stop")
	})

	listenerNetworkAwait(t, malformedWritten, "Listener did not read malformed backend response")
	select {
	case <-client.writeStarted:
		// The defective path is now blocked writing Terminate while holding
		// connectionLock.
	case _, ok := <-notifications:
		if ok {
			t.Fatal("Listener emitted an unexpected notification")
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal("Listener neither began nor completed protocol-failure cleanup")
	}

	result := make(chan error, 1)
	go func() { result <- listener.Close() }()
	returned := false
	select {
	case <-result:
		returned = true
	case <-time.After(200 * time.Millisecond):
		t.Error("ListenerConn.Close blocked behind a graceful shutdown write to a blackholed peer")
	}

	releaseOnce.Do(func() { close(releaseWrite) })
	if !returned {
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("ListenerConn.Close did not return after releasing the test write")
		}
	}
}

func TestListenerNetworkLossMarkerFollowsRetainedNotifications(t *testing.T) {
	listener := &Listener{
		Notify:            make(chan *Notification),
		done:              make(chan struct{}),
		notificationQueue: make(chan *Notification, listenerChannelCapacity),
	}
	dispatcherDone := make(chan struct{})
	go func() {
		listener.notificationDispatcher()
		close(dispatcherDone)
	}()
	t.Cleanup(func() {
		close(listener.done)
		listenerNetworkAwaitCleanup(t, dispatcherDone, "notification dispatcher did not stop")
	})

	if !listener.sendNotification(&Notification{Extra: "held"}) {
		t.Fatal("notification dispatcher stopped unexpectedly")
	}
	deadline := time.Now().Add(time.Second)
	for len(listener.notificationQueue) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("dispatcher did not begin forwarding the held notification")
		}
		runtime.Gosched()
	}

	for range listenerChannelCapacity {
		if !listener.sendNotification(&Notification{Extra: "retained"}) {
			t.Fatal("notification dispatcher stopped unexpectedly")
		}
	}
	if !listener.sendNotification(&Notification{Extra: "dropped"}) {
		t.Fatal("notification dispatcher stopped unexpectedly")
	}

	nilIndex := -1
	for i := range listenerChannelCapacity + 2 {
		select {
		case notification := <-listener.Notify:
			if notification == nil {
				nilIndex = i
			}
		case <-time.After(listenerNetworkTestTimeout):
			t.Fatal("notification dispatcher did not deliver its retained prefix and loss marker")
		}
	}
	if want := listenerChannelCapacity + 1; nilIndex != want {
		t.Errorf("loss marker index = %d; want %d after every retained pre-loss notification", nilIndex, want)
	}
}

func TestListenerNetworkDirectDeliveryPreservesFallbackOrder(t *testing.T) {
	listener := &Listener{
		Notify:            make(chan *Notification, 1),
		done:              make(chan struct{}),
		notificationQueue: make(chan *Notification, listenerChannelCapacity),
	}
	dispatcherDone := make(chan struct{})
	go func() {
		listener.notificationDispatcher()
		close(dispatcherDone)
	}()
	t.Cleanup(func() {
		close(listener.done)
		listenerNetworkAwaitCleanup(t, dispatcherDone, "notification dispatcher did not stop")
	})

	notifications := []*Notification{
		{Extra: "direct"},
		{Extra: "queued-first"},
		{Extra: "queued-second"},
	}
	for _, notification := range notifications {
		if !listener.sendNotification(notification) {
			t.Fatal("notification dispatcher stopped unexpectedly")
		}
	}
	if got := len(listener.Notify); got != 1 {
		t.Fatalf("direct delivery buffered %d notifications; want 1", got)
	}

	for i, want := range notifications {
		select {
		case got := <-listener.Notify:
			if got != want {
				t.Fatalf("notification %d = %#v; want %#v", i, got, want)
			}
		case <-time.After(listenerNetworkTestTimeout):
			t.Fatalf("timed out waiting for notification %d", i)
		}
	}
}

func TestListenerNetworkCallbackCanListenDuringReconnect(t *testing.T) {
	client, server := net.Pipe()
	releaseFirst := make(chan struct{})
	dialer := &listenerNetworkCallbackDialer{
		conn:         client,
		firstEntered: make(chan struct{}),
		releaseFirst: releaseFirst,
	}
	backendDone := make(chan struct{})
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
		if !regressionReadFrontendMessage(server) {
			return
		}
		if _, err := server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.CommandComplete, []byte("LISTEN\x00")),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil)); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, server)
	}()

	callbackResult := make(chan error, 1)
	var callbackOnce sync.Once
	var listener *Listener
	listener = NewDialListener(
		dialer,
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable connect_timeout=0",
		5*time.Millisecond,
		5*time.Millisecond,
		func(event ListenerEventType, _ error) {
			if event != ListenerEventConnectionAttemptFailed {
				return
			}
			callbackOnce.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
				defer cancel()
				callbackResult <- listener.ListenContext(ctx, "listener_network_callback")
			})
		},
	)
	t.Cleanup(func() {
		_ = listener.Close()
		_ = client.Close()
		_ = server.Close()
		listenerNetworkAwaitCleanup(t, backendDone, "callback reconnect backend did not stop")
	})
	listenerNetworkAwait(t, dialer.firstEntered, "listener did not begin its first connection attempt")
	close(releaseFirst)

	select {
	case err := <-callbackResult:
		if err != nil {
			t.Fatalf("Listener callback could not wait for reconnect and LISTEN: %v", err)
		}
	case <-time.After(listenerNetworkTestTimeout):
		t.Fatal("Listener callback deadlocked the reconnect needed by ListenContext")
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
