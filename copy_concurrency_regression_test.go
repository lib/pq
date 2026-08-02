package pq

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/proto"
)

type copyConcurrencyReadTrackingConn struct {
	net.Conn

	activeReads    atomic.Int32
	firstRead      chan struct{}
	concurrentRead chan struct{}
	firstOnce      sync.Once
	concurrentOnce sync.Once
}

func (c *copyConcurrencyReadTrackingConn) Read(p []byte) (int, error) {
	active := c.activeReads.Add(1)
	c.firstOnce.Do(func() { close(c.firstRead) })
	if active > 1 {
		c.concurrentOnce.Do(func() { close(c.concurrentRead) })
	}
	n, err := c.Conn.Read(p)
	c.activeReads.Add(-1)
	return n, err
}

type copyConcurrencyWriteTrackingConn struct {
	net.Conn

	writeStarted chan struct{}
	writeOnce    sync.Once
}

type copyConcurrencyGatedScriptConn struct {
	*regressionScriptConn

	gate        <-chan struct{}
	readEntered chan struct{}
	readOnce    sync.Once
}

func (c *copyConcurrencyGatedScriptConn) Read(p []byte) (int, error) {
	c.readOnce.Do(func() {
		close(c.readEntered)
		<-c.gate
	})
	return c.regressionScriptConn.Read(p)
}

func (c *copyConcurrencyWriteTrackingConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	return c.Conn.Write(p)
}

type copyConcurrencySuccessfulCancelDialer struct {
	entered chan struct{}
	once    sync.Once
}

func (d *copyConcurrencySuccessfulCancelDialer) dial() (net.Conn, error) {
	d.once.Do(func() { close(d.entered) })
	// An empty scripted connection accepts the CancelRequest write and returns
	// EOF immediately, which is how the cancellation connection reports that
	// PostgreSQL consumed the one-shot request.
	return newRegressionScriptConn(nil), nil
}

func (d *copyConcurrencySuccessfulCancelDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *copyConcurrencySuccessfulCancelDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func copyConcurrencyAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for %s", what)
	}
}

func copyConcurrencyAwaitError(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for %s", what)
		return errors.New("copy concurrency test timed out")
	}
}

func TestCopyConcurrencyRegressionTransactionEndDoesNotRaceResponseLoop(t *testing.T) {
	tests := []struct {
		name string
		end  func(*conn) error
	}{
		{name: "rollback", end: func(cn *conn) error { return cn.Rollback() }},
		{name: "commit", end: func(cn *conn) error { return cn.Commit() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			wire := &copyConcurrencyReadTrackingConn{
				Conn:           client,
				firstRead:      make(chan struct{}),
				concurrentRead: make(chan struct{}),
			}
			cn := &conn{
				c:         wire,
				buf:       bufio.NewReader(wire),
				txnStatus: txnStatusIdleInTransaction,
			}
			ci := &copyin{cn: cn, done: make(chan bool, 1)}

			responseDone := make(chan struct{})
			go func() {
				ci.resploop()
				close(responseDone)
			}()
			copyConcurrencyAwait(t, wire.firstRead, "COPY response loop to start reading")

			// Drain frontend writes so the transaction-ending operation can reach
			// its response read. A correct implementation may instead wait for or
			// abort the active COPY; cleanup below releases either behavior.
			drainDone := make(chan struct{})
			go func() {
				_, _ = io.Copy(io.Discard, server)
				close(drainDone)
			}()

			endDone := make(chan error, 1)
			go func() { endDone <- tt.end(cn) }()

			endFinished := false
			select {
			case <-wire.concurrentRead:
				t.Error("transaction end started a second reader while the COPY response loop owned the connection")
			case <-endDone:
				endFinished = true
				// Returning without overlapping the response reader is safe.
			case <-time.After(500 * time.Millisecond):
				// Waiting for the active COPY is also safe. Cleanup interrupts it.
			}

			_ = client.Close()
			_ = server.Close()
			copyConcurrencyAwait(t, responseDone, "COPY response loop cleanup")
			copyConcurrencyAwait(t, drainDone, "frontend drain cleanup")
			if !endFinished {
				select {
				case <-endDone:
				case <-time.After(time.Second):
					t.Error("transaction-ending operation did not stop during cleanup")
				}
			}
		})
	}
}

// Commit must synchronize with the COPY response owner before it inspects
// txnStatus. Both goroutines start after the same barrier, so neither access is
// ordered before the other; this test is intended to be run with -race.
func TestCopyConcurrencyRegressionCommitSynchronizesTransactionStatus(t *testing.T) {
	for range 32 {
		start := make(chan struct{})
		wire := &copyConcurrencyGatedScriptConn{
			regressionScriptConn: newRegressionScriptConn(
				regressionBackendFrame(proto.ReadyForQuery, []byte{'E'}),
			),
			gate:        start,
			readEntered: make(chan struct{}),
		}
		cn := &conn{
			c:         wire,
			buf:       bufio.NewReader(wire),
			txnStatus: txnStatusIdleInTransaction,
		}
		ci := &copyin{cn: cn, done: make(chan bool, 1)}

		responseDone := make(chan struct{})
		go func() {
			ci.resploop()
			close(responseDone)
		}()
		copyConcurrencyAwait(t, wire.readEntered, "COPY response read")

		commitDone := make(chan error, 1)
		go func() {
			<-start
			commitDone <- cn.Commit()
		}()
		close(start)

		copyConcurrencyAwait(t, responseDone, "COPY response completion")
		_ = copyConcurrencyAwaitError(t, commitDone, "Commit completion")
	}
}

// database/sql permanently marks a driver statement closed even when its
// driver's Close method returns an error. A close requested while COPY owns the
// protocol must therefore be deferred or otherwise completed; returning
// errCopyInProgress leaks the named server statement on a reusable session.
func TestCopyConcurrencyRegressionStmtCloseDuringCopyDoesNotLeak(t *testing.T) {
	db := pqtest.MustDB(t)
	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// This test deliberately detects a server-side resource leak. Discard the
		// physical session so a red run cannot contaminate another test.
		_ = sqlConn.Raw(func(any) error { return driver.ErrBadConn })
		_ = sqlConn.Close()
	})

	var baseline int
	if err := sqlConn.QueryRowContext(context.Background(),
		`select count(*) from pg_prepared_statements`).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlConn.ExecContext(context.Background(),
		`create temporary table copy_stmt_close_regression (value bigint)`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	txFinished := false
	t.Cleanup(func() {
		if !txFinished {
			_ = tx.Rollback()
		}
	})

	normalStmt, err := tx.PrepareContext(context.Background(), `select 1`)
	if err != nil {
		t.Fatal(err)
	}
	copyStmt, err := tx.PrepareContext(context.Background(),
		CopyIn("copy_stmt_close_regression", "value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyStmt.ExecContext(context.Background(), int64(1)); err != nil {
		t.Fatal(err)
	}

	if err := normalStmt.Close(); err != nil {
		t.Errorf("closing a prepared statement during COPY: %v", err)
	}
	if _, err := copyStmt.ExecContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	var afterClose int
	if err := tx.QueryRowContext(context.Background(),
		`select count(*) from pg_prepared_statements`).Scan(&afterClose); err != nil {
		t.Fatal(err)
	}
	if afterClose != baseline {
		t.Errorf("server prepared statements after Close = %d; want baseline %d", afterClose, baseline)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	txFinished = true
}

func TestCopyConcurrencyRegressionCloseWaitsAfterAsyncError(t *testing.T) {
	client, server := net.Pipe()
	cn := &conn{
		c:         client,
		buf:       bufio.NewReader(client),
		txnStatus: txnStatusIdleInTransaction,
	}
	ci := &copyin{
		cn:     cn,
		buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0, 'x', '\n'},
		done:   make(chan bool, 1),
	}

	responseDone := make(chan struct{})
	go func() {
		ci.resploop()
		close(responseDone)
	}()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		copyConcurrencyAwait(t, responseDone, "COPY response loop cleanup")
	}()

	errorSent := make(chan error, 1)
	go func() {
		_, err := server.Write(regressionBackendFrame(
			proto.ErrorResponse,
			regressionBackendError("22000", "COPY rejected"),
		))
		errorSent <- err
	}()
	if err := copyConcurrencyAwaitError(t, errorSent, "backend COPY error delivery"); err != nil {
		t.Fatalf("sending backend COPY error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for ci.err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ci.err() == nil {
		t.Fatal("COPY response loop did not publish the backend error")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- ci.Close() }()

	type frontendRead struct {
		typ proto.RequestCode
		err error
	}
	frontend := make(chan frontendRead, 1)
	frontendDone := make(chan struct{})
	go func() {
		defer close(frontendDone)
		var b [1]byte
		n, err := server.Read(b[:])
		if n == 1 {
			frontend <- frontendRead{typ: proto.RequestCode(b[0])}
			return
		}
		frontend <- frontendRead{err: err}
	}()

	closeFinished := false
	select {
	case got := <-frontend:
		if got.err == nil && (got.typ == proto.CopyDataRequest || got.typ == proto.CopyDoneRequest) {
			t.Errorf("copyin.Close sent %s after the backend had already rejected COPY and before ReadyForQuery", got.typ)
		} else if got.err == nil {
			t.Errorf("copyin.Close sent unexpected frontend message %s before ReadyForQuery", got.typ)
		}
	case <-time.After(200 * time.Millisecond):
		readySent := make(chan error, 1)
		go func() {
			_, err := server.Write(regressionBackendFrame(proto.ReadyForQuery, []byte{'E'}))
			readySent <- err
		}()
		if err := copyConcurrencyAwaitError(t, readySent, "ReadyForQuery delivery"); err != nil {
			t.Errorf("sending ReadyForQuery: %v", err)
		}
		if err := copyConcurrencyAwaitError(t, closeDone, "copyin.Close completion"); err == nil {
			t.Error("copyin.Close lost the asynchronous backend error")
		}
		closeFinished = true
	}

	_ = client.Close()
	_ = server.Close()
	copyConcurrencyAwait(t, frontendDone, "frontend read cleanup")
	if !closeFinished {
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Error("copyin.Close did not stop during cleanup")
		}
	}
}

func TestCopyConcurrencyRegressionCanceledExecInvalidatesPendingCopy(t *testing.T) {
	client, server := net.Pipe()
	wire := &copyConcurrencyWriteTrackingConn{Conn: client, writeStarted: make(chan struct{})}
	cancelDialer := &copyConcurrencySuccessfulCancelDialer{entered: make(chan struct{})}
	cn := &conn{
		c:      wire,
		buf:    bufio.NewReader(wire),
		dialer: cancelDialer,
		cfg: Config{
			Host:    "cancel.invalid",
			Port:    1,
			SSLMode: SSLModeDisable,
		},
	}
	ci := &copyin{
		cn:     cn,
		buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0},
		done:   make(chan bool, 1),
	}

	responseDone := make(chan struct{})
	go func() {
		ci.resploop()
		close(responseDone)
	}()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		copyConcurrencyAwait(t, responseDone, "COPY response loop cleanup")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := ci.ExecContext(ctx, []driver.NamedValue{{
			Ordinal: 1,
			Value:   strings.Repeat("x", ciBufferFlushSize+1),
		}})
		firstDone <- err
	}()
	copyConcurrencyAwait(t, wire.writeStarted, "first COPY write to block")
	cancel()
	copyConcurrencyAwait(t, cancelDialer.entered, "CancelRequest dial")
	if !regressionReadFrontendMessage(server) {
		t.Error("failed to drain the first COPY frontend message")
	}
	if err := copyConcurrencyAwaitError(t, firstDone, "canceled COPY ExecContext"); !errors.Is(err, context.Canceled) {
		t.Errorf("ExecContext error = %v; want %v", err, context.Canceled)
	}
	if err := cn.err.get(); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("canceled COPY ExecContext left the connection reusable: %v", err)
	}

	type secondOutcome struct {
		err error
	}
	secondFrontend := make(chan bool, 1)
	secondReadDone := make(chan struct{})
	go func() {
		secondFrontend <- regressionReadFrontendMessage(server)
		close(secondReadDone)
	}()
	secondDone := make(chan secondOutcome, 1)
	go func() {
		_, err := ci.Exec([]driver.Value{strings.Repeat("y", ciBufferFlushSize+1)})
		secondDone <- secondOutcome{err: err}
	}()
	select {
	case got := <-secondDone:
		if !errors.Is(got.err, driver.ErrBadConn) {
			t.Errorf("second COPY Exec error = %v; want %v", got.err, driver.ErrBadConn)
		}
	case <-time.After(time.Second):
		t.Error("second COPY Exec remained blocked")
	}
	select {
	case ok := <-secondFrontend:
		if ok {
			t.Error("canceled COPY permitted a second sequential frontend write while its response loop was pending")
		}
	case <-time.After(100 * time.Millisecond):
		// No frontend message is the expected outcome.
	}

	_ = client.Close()
	_ = server.Close()
	copyConcurrencyAwait(t, secondReadDone, "second frontend read cleanup")
}

func TestCopyConcurrencyRegressionCopyDataReturnsContextError(t *testing.T) {
	client, server := net.Pipe()
	wire := &copyConcurrencyWriteTrackingConn{Conn: client, writeStarted: make(chan struct{})}
	cancelDialer := &copyConcurrencySuccessfulCancelDialer{entered: make(chan struct{})}
	cn := &conn{
		c:      wire,
		buf:    bufio.NewReader(wire),
		dialer: cancelDialer,
		cfg: Config{
			Host:    "cancel.invalid",
			Port:    1,
			SSLMode: SSLModeDisable,
		},
	}
	ci := &copyin{
		cn:     cn,
		buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0},
		done:   make(chan bool, 1),
	}

	responseDone := make(chan struct{})
	go func() {
		ci.resploop()
		close(responseDone)
	}()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		copyConcurrencyAwait(t, responseDone, "COPY response loop cleanup")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	copyDone := make(chan error, 1)
	go func() {
		_, err := ci.CopyData(ctx, strings.Repeat("x", ciBufferFlushSize+1))
		copyDone <- err
	}()
	copyConcurrencyAwait(t, wire.writeStarted, "CopyData write to block")
	cancel()
	copyConcurrencyAwait(t, cancelDialer.entered, "CancelRequest dial")
	if !regressionReadFrontendMessage(server) {
		t.Error("failed to drain the CopyData frontend message")
	}
	if err := copyConcurrencyAwaitError(t, copyDone, "canceled CopyData"); err != context.Canceled {
		t.Errorf("CopyData error = %v; want exact %v", err, context.Canceled)
	}
	if err := cn.err.getForNext(); err != context.Canceled {
		t.Errorf("CopyData cancellation stored connection error %v; want exact %v", err, context.Canceled)
	}
}
