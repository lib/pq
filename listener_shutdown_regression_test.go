package pq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
)

type regressionListenerBlockingDialer struct {
	entered      chan struct{}
	release      chan struct{}
	once         sync.Once
	releaseOnce  sync.Once
	dialCalls    atomic.Int64
	timeoutCalls atomic.Int64
}

func newRegressionListenerBlockingDialer() *regressionListenerBlockingDialer {
	return &regressionListenerBlockingDialer{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (d *regressionListenerBlockingDialer) block() (net.Conn, error) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return nil, errors.New("released blocked listener dial")
}

func (d *regressionListenerBlockingDialer) releaseDial() {
	d.releaseOnce.Do(func() { close(d.release) })
}

func (d *regressionListenerBlockingDialer) Dial(string, string) (net.Conn, error) {
	d.dialCalls.Add(1)
	return d.block()
}

func (d *regressionListenerBlockingDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	d.timeoutCalls.Add(1)
	return d.block()
}

func TestListenerRegressionCloseInterruptsBlockingReconnectDial(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{
			name: "Dial",
			dsn:  "host=listener.invalid port=1 sslmode=disable connect_timeout=0",
		},
		{
			name: "DialTimeout",
			dsn:  "host=listener.invalid port=1 sslmode=disable connect_timeout=1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer := newRegressionListenerBlockingDialer()
			t.Cleanup(dialer.releaseDial)
			listener := NewDialListener(dialer, tt.dsn, time.Hour, time.Hour, nil)
			t.Cleanup(func() {
				dialer.releaseDial()
				_ = listener.Close()
			})
			regressionAwaitSignal(t, dialer.entered, "listener did not enter the blocking dial")
			if tt.name == "Dial" {
				if dialer.dialCalls.Load() != 1 || dialer.timeoutCalls.Load() != 0 {
					t.Fatalf("dial calls = %d, timeout calls = %d; want 1, 0",
						dialer.dialCalls.Load(), dialer.timeoutCalls.Load())
				}
			} else if dialer.dialCalls.Load() != 0 || dialer.timeoutCalls.Load() != 1 {
				t.Fatalf("dial calls = %d, timeout calls = %d; want 0, 1",
					dialer.dialCalls.Load(), dialer.timeoutCalls.Load())
			}

			closeResult := make(chan error, 1)
			go func() { closeResult <- listener.Close() }()

			closeReturned := false
			select {
			case err := <-closeResult:
				closeReturned = true
				if err != nil {
					t.Errorf("Listener.Close returned %v", err)
				}
			case <-time.After(regressionOperationTimeout):
				t.Error("Listener.Close blocked behind an in-progress dial")
			}

			// Release even when Close returned promptly so an abandoned legacy
			// Dial call cannot leave a goroutine behind.
			dialer.releaseDial()
			if !closeReturned {
				select {
				case <-closeResult:
				case <-time.After(time.Second):
					t.Fatal("Listener.Close remained blocked after releasing the dial")
				}
			}
			regressionAwaitListenerClosed(t, listener.Notify)
		})
	}
}

func TestListenerRegressionCloseInterruptsInFlightListen(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	connected := make(chan struct{})
	queryReceived := make(chan struct{})
	go func() {
		if !regressionReadStartupPacket(server) {
			return
		}
		_, _ = server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil))
		if regressionReadFrontendMessage(server) {
			close(queryReceived)
		}
	}()

	listener := NewDialListener(
		protocolLifecycleFixedDialer{conn: client},
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		time.Hour,
		time.Hour,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				close(connected)
			}
		},
	)
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	regressionAwaitSignal(t, connected, "listener did not establish its initial connection")

	listenResult := make(chan error, 1)
	go func() { listenResult <- listener.Listen("regression_blocked") }()
	regressionAwaitSignal(t, queryReceived, "listener did not send LISTEN")

	closeResult := make(chan error, 1)
	go func() { closeResult <- listener.Close() }()

	closeReturned := false
	select {
	case err := <-closeResult:
		closeReturned = true
		if err != nil {
			t.Errorf("Listener.Close returned %v", err)
		}
	case <-time.After(regressionOperationTimeout):
		t.Error("Listener.Close blocked behind an in-flight LISTEN")
	}

	// Closing the peer releases both the old implementation and any failed
	// replacement implementation before the test waits for their goroutines.
	_ = server.Close()
	if !closeReturned {
		select {
		case <-closeResult:
		case <-time.After(time.Second):
			t.Fatal("Listener.Close remained blocked after the backend disconnected")
		}
	}
	select {
	case err := <-listenResult:
		if err == nil {
			t.Error("in-flight Listener.Listen succeeded after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("Listener.Listen remained blocked after shutdown")
	}
	regressionAwaitListenerClosed(t, listener.Notify)
}

func TestListenerRegressionCloseUnblocksSaturatedConnectionNotifications(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	connected := make(chan struct{})
	flooded := make(chan struct{})
	go func() {
		if !regressionReadStartupPacket(server) {
			return
		}
		_, _ = server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil))

		payload := binary.BigEndian.AppendUint32(nil, 42)
		payload = append(payload, "regression_flood\x00payload\x00"...)
		frame := regressionBackendFrame(proto.NotificationResponse, payload)
		// Listener.Notify and the per-connection notification channel each have
		// capacity 32. One more notification can be held by the forwarding loop;
		// the 66th therefore leaves ListenerConn blocked trying to forward it.
		for range 66 {
			if _, err := server.Write(frame); err != nil {
				return
			}
		}
		close(flooded)
	}()

	listener := NewDialListener(
		protocolLifecycleFixedDialer{conn: client},
		"host=listener.invalid port=1 user=test dbname=test sslmode=disable",
		time.Hour,
		time.Hour,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				close(connected)
			}
		},
	)
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	regressionAwaitSignal(t, connected, "listener did not establish its initial connection")

	listener.lock.Lock()
	listenerConn := listener.cn
	connectionNotifications := listener.connNotificationChan
	listener.lock.Unlock()
	if listenerConn == nil || connectionNotifications == nil {
		t.Fatal("listener did not publish its per-connection notification channel")
	}
	regressionAwaitSignal(t, flooded, "listener notification buffers did not saturate")

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	stalled := false
	select {
	case _, ok := <-listenerConn.replyChan:
		if ok {
			t.Error("ListenerConn produced an unexpected query reply during shutdown")
		}
	case <-time.After(regressionOperationTimeout):
		stalled = true
		t.Error("Listener.Close left the per-connection receiver blocked forwarding a notification")
	}

	// Draining before the timeout would itself release the bug. Drain only as
	// cleanup (and to verify eventual channel closure) after checking the
	// receiver's independent reply channel completion signal.
	drained := make(chan struct{})
	go func() {
		for range connectionNotifications {
		}
		close(drained)
	}()
	if stalled {
		_ = server.Close()
	}
	regressionAwaitSignal(t, drained, "per-connection notification channel did not close")
	regressionAwaitListenerClosed(t, listener.Notify)
}

func regressionAwaitListenerClosed(t *testing.T, notifications <-chan *Notification) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-notifications:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("Listener notification channel did not close")
		}
	}
}
