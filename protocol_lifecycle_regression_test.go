package pq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

const regressionOperationTimeout = 250 * time.Millisecond

// TestProtocolRegressionMalformedFrames documents that lengths and payloads
// supplied by the backend are untrusted. Neither a length smaller than the
// four-byte length word nor a truncated payload may escape as a panic.
func TestProtocolRegressionMalformedFrames(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		call  func(*conn) error
	}{
		{
			name:  "zero frame length",
			input: []byte{byte(proto.ReadyForQuery), 0, 0, 0, 0},
			call: func(cn *conn) error {
				_, err := cn.recvMessage(new(readBuf))
				return err
			},
		},
		{
			name:  "frame length smaller than header",
			input: []byte{byte(proto.ReadyForQuery), 0, 0, 0, 3},
			call: func(cn *conn) error {
				_, err := cn.recvMessage(new(readBuf))
				return err
			},
		},
		{
			name: "truncated declared payload",
			input: append(
				[]byte{byte(proto.DataRow), 0, 0, 0, 8},
				0,
			),
			call: func(cn *conn) error {
				_, err := cn.recvMessage(new(readBuf))
				return err
			},
		},
		{
			name:  "ready-for-query without status byte",
			input: regressionBackendFrame(proto.ReadyForQuery, nil),
			call: func(cn *conn) error {
				return cn.startup(Config{MaxProtocolVersion: ProtocolVersion30})
			},
		},
		{
			name:  "ready-for-query with invalid status byte",
			input: regressionBackendFrame(proto.ReadyForQuery, []byte{'X'}),
			call: func(cn *conn) error {
				return cn.startup(Config{MaxProtocolVersion: ProtocolVersion30})
			},
		},
		{
			name: "row-description with invalid format code",
			input: bytes.Join([][]byte{
				regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 2)),
				regressionBackendFrame(proto.DataRow, regressionSingleColumnData(make([]byte, 8))),
			}, nil),
			call: regressionReadSingleRow,
		},
		{
			name: "short binary data value",
			input: bytes.Join([][]byte{
				regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 1)),
				regressionBackendFrame(proto.DataRow, regressionSingleColumnData([]byte{0})),
			}, nil),
			call: regressionReadSingleRow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := newRegressionScriptConn(tt.input)
			cn := &conn{c: wire, buf: bufio.NewReader(wire)}

			panicValue, err := regressionCallWithoutPanic(func() error {
				return tt.call(cn)
			})
			if panicValue != nil {
				t.Errorf("malformed backend input caused a panic: %v", panicValue)
			} else if err == nil {
				t.Error("malformed backend input was accepted")
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Errorf("malformed backend input left connection reusable: %v", err)
			}
		})
	}
}

func TestProtocolRegressionOversizedLongFrameRejectedBeforeAllocation(t *testing.T) {
	const childEnvironment = "PQ_PROTOCOL_LIFECYCLE_OVERSIZED_CHILD"
	if os.Getenv(childEnvironment) == "1" {
		previousLimit := debug.SetMemoryLimit(32 << 20)
		defer debug.SetMemoryLimit(previousLimit)

		header := make([]byte, 5)
		header[0] = byte(proto.DataRow) // A type exempt from the small-message limit.
		binary.BigEndian.PutUint32(header[1:], math.MaxUint32)
		cn := &conn{buf: bufio.NewReader(bytes.NewReader(header))}
		_, err := cn.recvMessage(new(readBuf))
		if err == nil {
			t.Fatal("near-uint32-max backend frame was accepted")
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("near-uint32-max frame was read before length validation: %v", err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestProtocolRegressionOversizedLongFrameRejectedBeforeAllocation$",
		"-test.count=1",
	)
	// PQGO_DEBUG suppresses TestMain's process-wide connection leak check. The
	// child shares the parent's test container and may otherwise observe
	// unrelated parallel tests that are legitimately still in flight.
	cmd.Env = append(os.Environ(), childEnvironment+"=1", "GOMEMLIMIT=32MiB", "PQGO_DEBUG=1")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("memory-limited oversized-frame subprocess did not terminate: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("oversized frame was not safely rejected before allocation: %v\n%s", err, output)
	}
}

func TestProtocolRegressionPreProtocolErrorIsBounded(t *testing.T) {
	input := append([]byte{'E'}, bytes.Repeat([]byte{'x'}, proto.MaxMsgLen+100)...)
	wire := newRegressionScriptConn(input)
	cn := &conn{c: wire, buf: bufio.NewReader(wire)}
	if _, err := cn.recvMessage(new(readBuf)); err == nil {
		t.Fatal("pre-protocol backend error was accepted")
	}
	if err := cn.err.get(); err != driver.ErrBadConn {
		t.Errorf("pre-protocol backend error left connection reusable: %v", err)
	}
	if remaining := wire.reader.Len() + cn.buf.Buffered(); remaining < 100 {
		t.Errorf("pre-protocol error consumed unbounded input; only %d bytes remain", remaining)
	}
}

func TestProtocolRegressionCancellationFailureUnblocksQuery(t *testing.T) {
	tests := []struct {
		name   string
		dialer Dialer
	}{
		{
			name:   "cancel dial fails",
			dialer: protocolLifecycleCancelDialer{err: errors.New("cancel dial failed")},
		},
		{
			name:   "cancel write fails",
			dialer: protocolLifecycleCancelDialer{conn: &regressionWriteErrorConn{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			queryReceived := make(chan struct{})
			go func() {
				if regressionReadFrontendMessage(server) {
					close(queryReceived)
				}
				_, _ = io.Copy(io.Discard, server)
			}()

			cn := &conn{
				c:         client,
				buf:       bufio.NewReader(client),
				dialer:    tt.dialer,
				cfg:       Config{Host: "cancel.invalid", Port: 1, SSLMode: SSLModeDisable},
				txnStatus: txnStatusIdle,
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, err := cn.ExecContext(ctx, "select blocked", nil)
				result <- err
			}()

			regressionAwaitSignal(t, queryReceived, "query was not sent")
			cancel()
			regressionExpectResultBeforeTimeout(t, result, func() {
				_ = server.Close()
				_ = client.Close()
			})
		})
	}
}

func TestProtocolRegressionCancelRequestBoundsLegacyDialer(t *testing.T) {
	timeoutUsed := make(chan time.Duration, 1)
	cn := &conn{
		dialer: protocolLifecycleTimeoutDialer{timeoutUsed: timeoutUsed},
		cfg:    Config{Host: "cancel.invalid", Port: 1, SSLMode: SSLModeDisable},
	}
	if err := cn.sendCancelRequest(); err == nil {
		t.Fatal("CancelRequest unexpectedly succeeded")
	}
	select {
	case timeout := <-timeoutUsed:
		if timeout <= 0 || timeout > 10*time.Second {
			t.Errorf("CancelRequest dial timeout = %s; want a positive bound no greater than 10s", timeout)
		}
	case <-time.After(regressionOperationTimeout):
		t.Fatal("CancelRequest did not use the bounded legacy DialTimeout method")
	}
}

func TestProtocolRegressionDialClosesConnectionOnError(t *testing.T) {
	t.Run("dial returns connection and error", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		tracked := &regressionTrackingConn{Conn: client}
		wantErr := errors.New("dial failed after creating socket")

		got, err := dial(context.Background(), protocolLifecycleCancelDialer{
			conn: tracked,
			err:  wantErr,
		}, Config{Host: "dial.invalid", Port: 1})
		if got != nil {
			t.Errorf("dial returned a connection alongside an error: %T", got)
		}
		if !errors.Is(err, wantErr) {
			t.Errorf("dial error = %v; want %v", err, wantErr)
		}
		if !tracked.closed.Load() {
			t.Error("dial left the returned connection open")
		}
	})

	t.Run("setting connection deadline fails", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		tracked := &regressionTrackingConn{Conn: client}
		deadlineConn := &protocolLifecycleDeadlineErrorConn{regressionTrackingConn: tracked}

		got, err := dial(context.Background(), protocolLifecycleCancelDialer{
			conn: deadlineConn,
		}, Config{Host: "dial.invalid", Port: 1, ConnectTimeout: time.Second})
		if got != nil {
			t.Errorf("dial returned a connection after SetDeadline failed: %T", got)
		}
		if err == nil || !strings.Contains(err.Error(), "setting test deadline") {
			t.Errorf("dial error = %v; want SetDeadline error", err)
		}
		if !tracked.closed.Load() {
			t.Error("dial left connection open after SetDeadline failed")
		}
	})
}

// Prepared-statement cancellation must finish sending (or failing to send) the
// CancelRequest before finish returns and permits the connection's next query.
func TestProtocolRegressionPreparedCancelFinishSynchronizes(t *testing.T) {
	dialer := &regressionBlockingDialer{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cn := &conn{
		dialer: dialer,
		cfg:    Config{Host: "cancel.invalid", Port: 1, SSLMode: SSLModeDisable},
	}
	ctx, cancel := context.WithCancel(context.Background())
	finish := cn.watchCancel(ctx, true)
	cancel()
	regressionAwaitSignal(t, dialer.entered, "CancelRequest was not started")

	returned := make(chan struct{})
	go func() {
		finish()
		close(returned)
	}()

	select {
	case <-returned:
		t.Error("prepared statement finish returned while CancelRequest was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(dialer.release)
	regressionAwaitSignal(t, returned, "prepared statement finish did not return after cancellation completed")
}

func TestProtocolRegressionConnectContextBoundsHandshake(t *testing.T) {
	tests := []struct {
		name string
		mode SSLMode
	}{
		{name: "TLS negotiation read", mode: SSLModeRequire},
		{name: "startup read", mode: SSLModeDisable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer := newRegressionDrainingPipeDialer()
			defer dialer.Close()
			connector, err := NewConnectorConfig(Config{
				Host:               "context.invalid",
				Port:               1,
				User:               "test",
				Database:           "test",
				SSLMode:            tt.mode,
				SSLNegotiation:     SSLNegotiationPostgres,
				MaxProtocolVersion: ProtocolVersion30,
			})
			if err != nil {
				t.Fatal(err)
			}
			connector.Dialer(dialer)

			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				cn, err := connector.Connect(ctx)
				if err == nil && cn != nil {
					_ = cn.Close()
				}
				result <- err
			}()

			regressionExpectResultBeforeTimeout(t, result, dialer.Close)
		})
	}
}

func TestProtocolRegressionConnectContextBoundsTargetSessionAttrs(t *testing.T) {
	client, server := net.Pipe()
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
		_, _ = io.Copy(io.Discard, server)
	}()

	connector, err := NewConnectorConfig(Config{
		Host:               "context.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModeDisable,
		MaxProtocolVersion: ProtocolVersion30,
		TargetSessionAttrs: TargetSessionAttrsReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(protocolLifecycleFixedDialer{conn: client})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		cn, err := connector.Connect(ctx)
		if err == nil && cn != nil {
			_ = cn.Close()
		}
		result <- err
	}()

	regressionAwaitSignal(t, queryReceived, "target_session_attrs query was not sent")
	regressionExpectResultBeforeTimeout(t, result, func() {
		_ = server.Close()
		_ = client.Close()
	})
}

func TestProtocolRegressionBeginTxContextCoversBegin(t *testing.T) {
	client, server := net.Pipe()
	beginReceived := make(chan struct{})
	go func() {
		if regressionReadFrontendMessage(server) {
			close(beginReceived)
		}
		_, _ = io.Copy(io.Discard, server)
	}()

	cn := &conn{
		c:         client,
		buf:       bufio.NewReader(client),
		dialer:    protocolLifecycleCancelDialer{err: errors.New("cancel dial failed")},
		cfg:       Config{Host: "cancel.invalid", Port: 1, SSLMode: SSLModeDisable},
		txnStatus: txnStatusIdle,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := cn.BeginTx(ctx, driver.TxOptions{})
		result <- err
	}()

	regressionAwaitSignal(t, beginReceived, "BEGIN was not sent")
	cancel()
	regressionExpectResultBeforeTimeout(t, result, func() {
		_ = server.Close()
		_ = client.Close()
	})
}

func TestProtocolRegressionFallbackClosesAbandonedSocket(t *testing.T) {
	dialer := newRegressionFallbackDialer()
	defer dialer.Close()
	connector, err := NewConnectorConfig(Config{
		Host:               "fallback.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModePrefer,
		SSLNegotiation:     SSLNegotiationPostgres,
		MaxProtocolVersion: ProtocolVersion30,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(dialer)
	cn, err := connector.open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	connections := dialer.Connections()
	if len(connections) != 2 {
		t.Fatalf("fallback opened %d connections; want 2", len(connections))
	}
	if !connections[0].closed.Load() {
		t.Error("TLS fallback left the abandoned socket open")
	}
}

func TestProtocolRegressionCloseOperationsAreBounded(t *testing.T) {
	t.Run("connection", func(t *testing.T) {
		client, server := net.Pipe()
		cn := &conn{c: client, buf: bufio.NewReader(client)}
		regressionExpectCompletionBeforeTimeout(t, regressionAsync(cn.Close), func() {
			_ = server.Close()
			_ = client.Close()
		})
	})

	t.Run("statement", func(t *testing.T) {
		client, server := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, server) }()
		cn := &conn{c: client, buf: bufio.NewReader(client)}
		st := &stmt{cn: cn, name: "regression"}
		regressionExpectCompletionBeforeTimeout(t, regressionAsync(st.Close), func() {
			_ = server.Close()
			_ = client.Close()
		})
	})

	t.Run("copy", func(t *testing.T) {
		client, server := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, server) }()
		cn := &conn{c: client, buf: bufio.NewReader(client)}
		ci := &copyin{
			cn:     cn,
			buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0},
			done:   make(chan bool, 1),
		}
		go ci.resploop()
		regressionExpectCompletionBeforeTimeout(t, regressionAsync(ci.Close), func() {
			_ = server.Close()
			_ = client.Close()
		})
	})
}

func TestProtocolRegressionLegacyDriverMethodsDoNotPanic(t *testing.T) {
	tests := []struct {
		name string
		call func(*conn, *stmt) error
	}{
		{
			name: "Conn.Prepare",
			call: func(cn *conn, _ *stmt) error {
				_, err := cn.Prepare("select 1")
				return err
			},
		},
		{
			name: "Conn.Begin",
			call: func(cn *conn, _ *stmt) error {
				_, err := cn.Begin()
				return err
			},
		},
		{
			name: "Stmt.Exec",
			call: func(_ *conn, st *stmt) error {
				_, err := st.Exec(nil)
				return err
			},
		},
		{
			name: "Stmt.Query",
			call: func(_ *conn, st *stmt) error {
				_, err := st.Query(nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cn := new(conn)
			cn.err.set(errors.New("deliberately unusable test connection"))
			st := &stmt{cn: cn}
			panicValue, _ := regressionCallWithoutPanic(func() error {
				return tt.call(cn, st)
			})
			if panicValue != nil {
				t.Errorf("required database/sql/driver method panicked: %v", panicValue)
			}
		})
	}
}

func TestProtocolRegressionCopyToRejectionDrainsBackend(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.CopyOutResponse, []byte{0, 0, 0}),
		regressionBackendFrame(proto.CopyDataResponse, []byte("row from backend\n")),
		regressionBackendFrame(proto.CopyDoneResponse, nil),
		regressionBackendFrame(proto.CommandComplete, []byte("COPY 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'T'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{
		c:         script,
		buf:       bufio.NewReader(script),
		txnStatus: txnStatusIdleInTransaction,
	}

	_, err := cn.prepareCopyIn("copy tbl to stdout")
	if !errors.Is(err, errCopyToNotSupported) {
		t.Errorf("COPY TO returned %v; want %v", err, errCopyToNotSupported)
	}
	if err := cn.err.get(); err != nil {
		t.Errorf("COPY TO rejection poisoned a synchronized connection: %v", err)
	}
}

func TestProtocolRegressionCopyResponseLoopAcceptsAsyncMessages(t *testing.T) {
	parameterStatus := []byte("server_version\x0017.2\x00")
	notification := binary.BigEndian.AppendUint32(nil, 42)
	notification = append(notification, "regression_channel\x00payload\x00"...)
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.ParameterStatus, parameterStatus),
		regressionBackendFrame(proto.NotificationResponse, notification),
		regressionBackendFrame(proto.CommandComplete, []byte("COPY 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'T'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	var gotNotification *Notification
	cn := &conn{
		c:                   script,
		buf:                 bufio.NewReader(script),
		txnStatus:           txnStatusIdleInTransaction,
		notificationHandler: func(n *Notification) { gotNotification = n },
	}
	ci := &copyin{cn: cn, done: make(chan bool, 1)}

	ci.resploop()
	if err := ci.err(); err != nil {
		t.Errorf("COPY response loop rejected asynchronous messages: %v", err)
	}
	if err := cn.err.get(); err != nil {
		t.Errorf("COPY response loop poisoned the connection: %v", err)
	}
	if cn.parameterStatus.serverVersion != 170200 {
		t.Errorf("server_version was not processed: got %d", cn.parameterStatus.serverVersion)
	}
	if gotNotification == nil || gotNotification.Channel != "regression_channel" || gotNotification.Extra != "payload" {
		t.Errorf("notification was not processed: %#v", gotNotification)
	}
}

func regressionBackendFrame(code proto.ResponseCode, payload []byte) []byte {
	frame := make([]byte, 5, 5+len(payload))
	frame[0] = byte(code)
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)+4))
	return append(frame, payload...)
}

func regressionSingleColumnDescription(typ oid.Oid, formatCode uint16) []byte {
	payload := []byte{0, 1}
	payload = append(payload, "value\x00"...)
	payload = binary.BigEndian.AppendUint32(payload, 0)
	payload = binary.BigEndian.AppendUint16(payload, 0)
	payload = binary.BigEndian.AppendUint32(payload, uint32(typ))
	payload = binary.BigEndian.AppendUint16(payload, 8)
	payload = binary.BigEndian.AppendUint32(payload, math.MaxUint32)
	payload = binary.BigEndian.AppendUint16(payload, formatCode)
	return payload
}

func regressionSingleColumnData(value []byte) []byte {
	payload := []byte{0, 1}
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(value)))
	return append(payload, value...)
}

func regressionReadSingleRow(cn *conn) error {
	rows, err := cn.simpleQuery("select malformed")
	if err != nil {
		return err
	}
	return rows.Next(make([]driver.Value, 1))
}

func regressionCallWithoutPanic(fn func() error) (panicValue any, err error) {
	defer func() { panicValue = recover() }()
	err = fn()
	return nil, err
}

func regressionAsync(fn func() error) <-chan error {
	result := make(chan error, 1)
	go func() { result <- fn() }()
	return result
}

func regressionAwaitSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(regressionOperationTimeout):
		t.Fatal(failure)
	}
}

func regressionExpectResultBeforeTimeout(t *testing.T, result <-chan error, cleanup func()) {
	t.Helper()
	select {
	case err := <-result:
		cleanup()
		if err == nil {
			t.Error("operation unexpectedly succeeded")
		}
		return
	case <-time.After(regressionOperationTimeout):
		t.Error("operation did not return after its connection or context became unusable")
	}

	cleanup()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Error("operation remained blocked after test cleanup")
	}
}

func regressionExpectCompletionBeforeTimeout(t *testing.T, result <-chan error, cleanup func()) {
	t.Helper()
	select {
	case <-result:
		cleanup()
		return
	case <-time.After(regressionOperationTimeout):
		t.Error("close operation did not return after its connection became unusable")
	}

	cleanup()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Error("close operation remained blocked after test cleanup")
	}
}

func regressionReadFrontendMessage(c net.Conn) bool {
	header := make([]byte, 5)
	if _, err := io.ReadFull(c, header); err != nil {
		return false
	}
	length := int(binary.BigEndian.Uint32(header[1:]))
	if length < 4 {
		return false
	}
	_, err := io.CopyN(io.Discard, c, int64(length-4))
	return err == nil
}

type regressionAddr string

func (a regressionAddr) Network() string { return "regression" }
func (a regressionAddr) String() string  { return string(a) }

type regressionScriptConn struct {
	reader *bytes.Reader
	closed atomic.Bool
}

func newRegressionScriptConn(input []byte) *regressionScriptConn {
	return &regressionScriptConn{reader: bytes.NewReader(input)}
}

func (c *regressionScriptConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *regressionScriptConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *regressionScriptConn) Close() error                { c.closed.Store(true); return nil }
func (c *regressionScriptConn) LocalAddr() net.Addr         { return regressionAddr("local") }
func (c *regressionScriptConn) RemoteAddr() net.Addr        { return regressionAddr("remote") }
func (c *regressionScriptConn) SetDeadline(time.Time) error { return nil }
func (c *regressionScriptConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *regressionScriptConn) SetWriteDeadline(time.Time) error {
	return nil
}

type regressionWriteErrorConn struct{ regressionScriptConn }

func (c *regressionWriteErrorConn) Write([]byte) (int, error) {
	return 0, errors.New("cancel write failed")
}

type protocolLifecycleCancelDialer struct {
	conn net.Conn
	err  error
}

type protocolLifecycleFixedDialer struct {
	conn net.Conn
}

type protocolLifecycleTimeoutDialer struct {
	timeoutUsed chan<- time.Duration
}

type protocolLifecycleDeadlineErrorConn struct {
	*regressionTrackingConn
}

func (c *protocolLifecycleDeadlineErrorConn) SetDeadline(time.Time) error {
	return errors.New("setting test deadline failed")
}

func (d protocolLifecycleTimeoutDialer) Dial(string, string) (net.Conn, error) {
	d.timeoutUsed <- 0
	return nil, errors.New("unbounded Dial called")
}

func (d protocolLifecycleTimeoutDialer) DialTimeout(_ string, _ string, timeout time.Duration) (net.Conn, error) {
	d.timeoutUsed <- timeout
	return nil, errors.New("bounded DialTimeout called")
}

func (d protocolLifecycleFixedDialer) Dial(string, string) (net.Conn, error) {
	return d.conn, nil
}

func (d protocolLifecycleFixedDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.conn, nil
}

func (d protocolLifecycleCancelDialer) Dial(string, string) (net.Conn, error) {
	return d.conn, d.err
}

func (d protocolLifecycleCancelDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.conn, d.err
}

type regressionBlockingDialer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *regressionBlockingDialer) dial() (net.Conn, error) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return nil, errors.New("released blocked cancel dial")
}

func (d *regressionBlockingDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *regressionBlockingDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

type regressionDrainingPipeDialer struct {
	mu    sync.Mutex
	conns []net.Conn
}

func newRegressionDrainingPipeDialer() *regressionDrainingPipeDialer {
	return new(regressionDrainingPipeDialer)
}

func (d *regressionDrainingPipeDialer) dial() (net.Conn, error) {
	client, server := net.Pipe()
	d.mu.Lock()
	d.conns = append(d.conns, client, server)
	d.mu.Unlock()
	go func() { _, _ = io.Copy(io.Discard, server) }()
	return client, nil
}

func (d *regressionDrainingPipeDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *regressionDrainingPipeDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func (d *regressionDrainingPipeDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		_ = c.Close()
	}
}

type regressionTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *regressionTrackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type regressionFallbackDialer struct {
	mu      sync.Mutex
	clients []*regressionTrackingConn
	peers   []net.Conn
}

func newRegressionFallbackDialer() *regressionFallbackDialer {
	return new(regressionFallbackDialer)
}

func (d *regressionFallbackDialer) dial() (net.Conn, error) {
	client, server := net.Pipe()
	tracked := &regressionTrackingConn{Conn: client}
	d.mu.Lock()
	index := len(d.clients)
	d.clients = append(d.clients, tracked)
	d.peers = append(d.peers, server)
	d.mu.Unlock()

	switch index {
	case 0:
		go func() {
			if !regressionReadStartupPacket(server) {
				return
			}
			_, _ = server.Write([]byte{'N'})
			_, _ = io.Copy(io.Discard, server)
		}()
	case 1:
		go func() {
			if !regressionReadStartupPacket(server) {
				return
			}
			_, _ = server.Write(bytes.Join([][]byte{
				regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
				regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
			}, nil))
			_, _ = io.Copy(io.Discard, server)
		}()
	default:
		return nil, fmt.Errorf("unexpected fallback dial %d", index+1)
	}
	return tracked, nil
}

func (d *regressionFallbackDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *regressionFallbackDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func (d *regressionFallbackDialer) Connections() []*regressionTrackingConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*regressionTrackingConn(nil), d.clients...)
}

func (d *regressionFallbackDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.clients {
		_ = c.Close()
	}
	for _, c := range d.peers {
		_ = c.Close()
	}
}

func regressionReadStartupPacket(c net.Conn) bool {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c, header); err != nil {
		return false
	}
	length := int(binary.BigEndian.Uint32(header))
	if length < 4 {
		return false
	}
	_, err := io.CopyN(io.Discard, c, int64(length-4))
	return err == nil
}
