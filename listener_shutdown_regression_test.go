package pq

import (
	"bytes"
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
