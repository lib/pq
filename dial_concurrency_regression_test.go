package pq

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	dialConcurrencyContextTimeout = 100 * time.Millisecond
	// Leave ample scheduler slack while still proving that the caller's short
	// deadline, rather than a much longer configured timeout, was forwarded.
	dialConcurrencyMaxForwardedTimeout = 500 * time.Millisecond
	dialConcurrencyWaitTimeout         = 2 * time.Second
)

var errDialConcurrencyReleased = errors.New("legacy dial released by test cleanup")

type dialConcurrencyCall struct {
	method  string
	timeout time.Duration
}

type dialConcurrencyProbe struct {
	calls    chan dialConcurrencyCall
	release  chan struct{}
	done     chan struct{}
	doneOnce sync.Once
	relOnce  sync.Once
	active   atomic.Int32
}

func newDialConcurrencyProbe() *dialConcurrencyProbe {
	return &dialConcurrencyProbe{
		calls:   make(chan dialConcurrencyCall, 1),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (d *dialConcurrencyProbe) Dial(string, string) (net.Conn, error) {
	d.begin(dialConcurrencyCall{method: "Dial"})
	defer d.end()
	<-d.release
	return nil, errDialConcurrencyReleased
}

func (d *dialConcurrencyProbe) DialTimeout(_ string, _ string, timeout time.Duration) (net.Conn, error) {
	d.begin(dialConcurrencyCall{method: "DialTimeout", timeout: timeout})
	defer d.end()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil, context.DeadlineExceeded
	case <-d.release:
		return nil, errDialConcurrencyReleased
	}
}

func (d *dialConcurrencyProbe) begin(call dialConcurrencyCall) {
	d.active.Add(1)
	d.calls <- call
}

func (d *dialConcurrencyProbe) end() {
	d.active.Add(-1)
	d.doneOnce.Do(func() { close(d.done) })
}

func (d *dialConcurrencyProbe) cleanup(t *testing.T) {
	t.Helper()
	d.relOnce.Do(func() { close(d.release) })
	if d.active.Load() == 0 {
		return
	}
	select {
	case <-d.done:
	case <-time.After(dialConcurrencyWaitTimeout):
		t.Errorf("legacy dial call remained active after test cleanup")
	}
}

func dialConcurrencyAwaitCall(t *testing.T, d *dialConcurrencyProbe) dialConcurrencyCall {
	t.Helper()
	select {
	case call := <-d.calls:
		return call
	case <-time.After(dialConcurrencyWaitTimeout):
		t.Fatal("legacy dialer was not called")
		return dialConcurrencyCall{}
	}
}

func dialConcurrencyAwaitStopped(t *testing.T, d *dialConcurrencyProbe) {
	t.Helper()
	select {
	case <-d.done:
	case <-time.After(dialConcurrencyWaitTimeout):
		t.Errorf("legacy dial call outlived the context deadline")
	}
	if active := d.active.Load(); active != 0 {
		t.Errorf("active legacy dial calls = %d; want 0", active)
	}
}

func TestDialConcurrencyRegressionContextDeadlineUsesLegacyDialTimeout(t *testing.T) {
	dialer := newDialConcurrencyProbe()
	t.Cleanup(func() { dialer.cleanup(t) })

	ctx, cancel := context.WithTimeout(context.Background(), dialConcurrencyContextTimeout)
	defer cancel()
	cn, err := dial(ctx, dialer, Config{Host: "context.invalid", Port: 1})
	if cn != nil {
		t.Errorf("dial returned connection %T after context deadline", cn)
		_ = cn.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("dial error = %v; want %v", err, context.DeadlineExceeded)
	}

	call := dialConcurrencyAwaitCall(t, dialer)
	if call.method != "DialTimeout" {
		t.Errorf("context deadline used legacy %s; want DialTimeout", call.method)
		if active := dialer.active.Load(); active != 0 {
			t.Errorf("active legacy dial calls after dial returned = %d; want 0", active)
		}
		return
	}
	if call.timeout <= 0 || call.timeout > dialConcurrencyMaxForwardedTimeout {
		t.Errorf("legacy DialTimeout duration = %s; want a positive duration bounded by the context deadline", call.timeout)
	}
	dialConcurrencyAwaitStopped(t, dialer)
}

func TestDialConcurrencyRegressionContextDeadlineShortensConfiguredTimeout(t *testing.T) {
	dialer := newDialConcurrencyProbe()
	t.Cleanup(func() { dialer.cleanup(t) })

	ctx, cancel := context.WithTimeout(context.Background(), dialConcurrencyContextTimeout)
	defer cancel()
	cn, err := dial(ctx, dialer, Config{
		Host:           "context.invalid",
		Port:           1,
		ConnectTimeout: time.Hour,
	})
	if cn != nil {
		t.Errorf("dial returned connection %T after context deadline", cn)
		_ = cn.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("dial error = %v; want %v", err, context.DeadlineExceeded)
	}

	call := dialConcurrencyAwaitCall(t, dialer)
	if call.method != "DialTimeout" {
		t.Fatalf("configured timeout used legacy %s; want DialTimeout", call.method)
	}
	// The context is deliberately orders of magnitude shorter than the
	// configured timeout. A broad upper bound avoids depending on scheduler
	// precision while still detecting forwarding the one-hour value.
	if call.timeout <= 0 || call.timeout > dialConcurrencyMaxForwardedTimeout {
		t.Errorf("legacy DialTimeout duration = %s; want the shorter context deadline", call.timeout)
		if active := dialer.active.Load(); active != 0 {
			t.Errorf("active legacy dial calls after dial returned = %d; want 0", active)
		}
		return
	}
	dialConcurrencyAwaitStopped(t, dialer)
}
