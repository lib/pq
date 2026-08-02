package pq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

var errConnectionIOWrite = errors.New("connection I/O regression write failure")

type connectionIOWriteMode uint8

const (
	connectionIOWritePartialError connectionIOWriteMode = iota
	connectionIOWriteShortNilThenError
	connectionIOWriteZeroError
)

// connectionIOWriteConn models Writer results that a frontend-message send
// must not mistake for a complete write. In particular, a conforming net.Conn
// may return both a positive byte count and an error.
type connectionIOWriteConn struct {
	reader *bytes.Reader
	mode   connectionIOWriteMode
	writes atomic.Int64
	closed atomic.Bool
}

func newConnectionIOWriteConn(mode connectionIOWriteMode, input []byte) *connectionIOWriteConn {
	return &connectionIOWriteConn{reader: bytes.NewReader(input), mode: mode}
}

func (c *connectionIOWriteConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *connectionIOWriteConn) Write(p []byte) (int, error) {
	call := c.writes.Add(1)
	switch c.mode {
	case connectionIOWritePartialError:
		return connectionIOPartialCount(len(p)), errConnectionIOWrite
	case connectionIOWriteShortNilThenError:
		if call == 1 {
			return connectionIOPartialCount(len(p)), nil
		}
		return 0, errConnectionIOWrite
	case connectionIOWriteZeroError:
		return 0, errConnectionIOWrite
	default:
		panic("unknown connection I/O regression write mode")
	}
}

func connectionIOPartialCount(length int) int {
	if length <= 1 {
		return 0
	}
	return length / 2
}

func (c *connectionIOWriteConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (*connectionIOWriteConn) LocalAddr() net.Addr  { return regressionAddr("local") }
func (*connectionIOWriteConn) RemoteAddr() net.Addr { return regressionAddr("remote") }
func (*connectionIOWriteConn) SetDeadline(time.Time) error {
	return nil
}
func (*connectionIOWriteConn) SetReadDeadline(time.Time) error {
	return nil
}
func (*connectionIOWriteConn) SetWriteDeadline(time.Time) error {
	return nil
}

type connectionIOFailDialer struct{}

func (connectionIOFailDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("connection I/O regression cancel dial failure")
}

func (connectionIOFailDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return nil, errors.New("connection I/O regression cancel dial failure")
}

func TestConnectionIONetworkRegressionSendPartialWritePoisons(t *testing.T) {
	success := bytes.Join([][]byte{
		regressionBackendFrame(proto.CommandComplete, []byte("UPDATE 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	tests := []struct {
		name  string
		mode  connectionIOWriteMode
		input []byte
	}{
		{
			name: "positive count and error",
			mode: connectionIOWritePartialError,
		},
		{
			name:  "short count without error",
			mode:  connectionIOWriteShortNilThenError,
			input: success,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := newConnectionIOWriteConn(tt.mode, tt.input)
			cn := &conn{
				c:         wire,
				buf:       bufio.NewReader(wire),
				txnStatus: txnStatusIdle,
			}

			if _, err := cn.ExecContext(context.Background(), "UPDATE regression SET value = 1", nil); err == nil {
				t.Error("partial frontend write was reported as a successful execution")
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Errorf("partial frontend write left connection reusable: %v", err)
			}

			writes := wire.writes.Load()
			if _, err := cn.ExecContext(context.Background(), "UPDATE regression SET value = 2", nil); !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("operation after partial write returned %v; want %v", err, driver.ErrBadConn)
			}
			if got := wire.writes.Load(); got != writes {
				t.Errorf("bad connection performed another frontend write: writes changed from %d to %d", writes, got)
			}
		})
	}
}

func TestConnectionIONetworkRegressionCopyFlushPoisonsPartialWrite(t *testing.T) {
	tests := []struct {
		name string
		mode connectionIOWriteMode
	}{
		{name: "positive count and error", mode: connectionIOWritePartialError},
		{name: "short count without error", mode: connectionIOWriteShortNilThenError},
		{name: "zero count generic error", mode: connectionIOWriteZeroError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := newConnectionIOWriteConn(tt.mode, nil)
			cn := &conn{c: wire, buf: bufio.NewReader(wire)}
			ci := &copyin{
				cn:     cn,
				buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0},
			}
			value := strings.Repeat("x", ciBufferFlushSize+1)

			if _, err := ci.Exec([]driver.Value{value}); err == nil {
				t.Error("partial COPY write was reported as successful")
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Errorf("partial COPY write left connection reusable: %v", err)
			}

			writes := wire.writes.Load()
			if _, err := ci.Exec([]driver.Value{value}); !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("COPY after partial write returned %v; want %v", err, driver.ErrBadConn)
			}
			if got := wire.writes.Load(); got != writes {
				t.Errorf("bad COPY connection performed another write: writes changed from %d to %d", writes, got)
			}
		})
	}
}

func TestConnectionIONetworkRegressionCopyExecContextInterruptsBlockedFlush(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	cn := &conn{
		c:      client,
		buf:    bufio.NewReader(client),
		dialer: connectionIOFailDialer{},
		cfg: Config{
			Host:    "cancel.invalid",
			Port:    1,
			SSLMode: SSLModeDisable,
		},
	}
	ci := &copyin{
		cn:     cn,
		buffer: []byte{byte(proto.CopyDataRequest), 0, 0, 0, 0},
	}
	var st driver.Stmt = ci
	if _, ok := st.(driver.StmtExecContext); !ok {
		t.Error("*copyin does not implement driver.StmtExecContext")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	type outcome struct {
		result driver.Result
		err    error
	}
	result := make(chan outcome, 1)
	go func() {
		r, err := connectionIOStmtExecContext(ctx, st, []driver.NamedValue{{
			Ordinal: 1,
			Value:   strings.Repeat("x", ciBufferFlushSize+1),
		}})
		result <- outcome{result: r, err: err}
	}()

	var got outcome
	select {
	case got = <-result:
	case <-time.After(500 * time.Millisecond):
		t.Error("COPY ExecContext remained blocked after its context deadline")
		_ = client.Close()
		_ = server.Close()
		select {
		case got = <-result:
		case <-time.After(time.Second):
			t.Fatal("blocked COPY write did not return after test cleanup")
		}
	}
	if got.err == nil {
		t.Errorf("blocked COPY ExecContext returned success: %v", got.result)
	}
	if err := cn.err.get(); err != driver.ErrBadConn {
		t.Errorf("canceled blocked COPY left connection reusable: %v", err)
	}
}

// connectionIOStmtExecContext mirrors database/sql's compatibility dispatch:
// without driver.StmtExecContext, the context is checked only before calling
// the legacy, non-context-aware Exec method.
func connectionIOStmtExecContext(ctx context.Context, st driver.Stmt, args []driver.NamedValue) (driver.Result, error) {
	if stCtx, ok := st.(driver.StmtExecContext); ok {
		return stCtx.ExecContext(ctx, args)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	return st.Exec(values)
}

func TestConnectionIONetworkRegressionPingPropagatesDrainFailure(t *testing.T) {
	t.Run("EOF", func(t *testing.T) {
		wire := bytes.Join([][]byte{
			regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
			regressionBackendFrame(proto.DataRow, regressionNullColumnData(1)),
		}, nil)
		script := newRegressionScriptConn(wire)
		cn := &conn{c: script, buf: bufio.NewReader(script), txnStatus: txnStatusIdle}

		if err := cn.Ping(context.Background()); err == nil {
			t.Error("Ping discarded EOF while draining its response")
		}
		if err := cn.err.get(); err != driver.ErrBadConn {
			t.Errorf("Ping drain EOF left connection reusable: %v", err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		delivered := make(chan struct{})
		go func() {
			if !regressionReadFrontendMessage(server) {
				return
			}
			wire := bytes.Join([][]byte{
				regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
				regressionBackendFrame(proto.DataRow, regressionNullColumnData(1)),
			}, nil)
			if _, err := server.Write(wire); err == nil {
				close(delivered)
			}
		}()

		cn := &conn{
			c:         client,
			buf:       bufio.NewReader(client),
			dialer:    connectionIOFailDialer{},
			txnStatus: txnStatusIdle,
			cfg: Config{
				Host:    "cancel.invalid",
				Port:    1,
				SSLMode: SSLModeDisable,
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- cn.Ping(ctx) }()

		select {
		case <-delivered:
		case <-time.After(time.Second):
			_ = client.Close()
			_ = server.Close()
			t.Fatal("Ping did not begin draining the row-producing response")
		}
		cancel()

		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Ping after context cancellation returned %v; want %v", err, context.Canceled)
			}
		case <-time.After(time.Second):
			_ = client.Close()
			_ = server.Close()
			t.Fatal("Ping remained blocked after context cancellation")
		}
		if err := cn.err.get(); err != driver.ErrBadConn {
			t.Errorf("canceled Ping left connection reusable: %v", err)
		}
	})
}

func TestConnectionIONetworkRegressionPingRejectsBareReadyForQuery(t *testing.T) {
	script := newRegressionScriptConn(
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	)
	cn := &conn{c: script, buf: bufio.NewReader(script), txnStatus: txnStatusIdle}

	if err := cn.Ping(context.Background()); err == nil {
		t.Fatal("Ping accepted ReadyForQuery without the expected EmptyQueryResponse")
	}
	if err := cn.err.get(); err != driver.ErrBadConn {
		t.Errorf("bare ReadyForQuery left Ping connection reusable: %v", err)
	}
}

func TestConnectionIONetworkRegressionRollbackUnexpectedTagPoisons(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.CommandComplete, []byte("SELECT 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'T'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{
		c:         script,
		buf:       bufio.NewReader(script),
		txnStatus: txnStatusIdleInTransaction,
	}

	if err := cn.Rollback(); err == nil {
		t.Fatal("Rollback accepted an unexpected command tag")
	}
	if err := cn.err.get(); err != driver.ErrBadConn {
		t.Errorf("unexpected Rollback tag left connection reusable: %v", err)
	}
	if cn.IsValid() {
		t.Error("connection left in a transaction remained valid after malformed Rollback completion")
	}
	if err := cn.ResetSession(context.Background()); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("ResetSession after malformed Rollback returned %v; want %v", err, driver.ErrBadConn)
	}
}
