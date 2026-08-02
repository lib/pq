package pq

import (
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type regressionPreCanceledConn struct {
	written atomic.Int64
	closed  atomic.Bool
}

func (*regressionPreCanceledConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *regressionPreCanceledConn) Write(p []byte) (int, error) {
	c.written.Add(int64(len(p)))
	return len(p), nil
}
func (c *regressionPreCanceledConn) Close() error {
	c.closed.Store(true)
	return nil
}
func (*regressionPreCanceledConn) LocalAddr() net.Addr              { return regressionAddr("local") }
func (*regressionPreCanceledConn) RemoteAddr() net.Addr             { return regressionAddr("remote") }
func (*regressionPreCanceledConn) SetDeadline(time.Time) error      { return nil }
func (*regressionPreCanceledConn) SetReadDeadline(time.Time) error  { return nil }
func (*regressionPreCanceledConn) SetWriteDeadline(time.Time) error { return nil }

type regressionPreCanceledDialer struct {
	calls atomic.Int64
}

func (d *regressionPreCanceledDialer) fail() (net.Conn, error) {
	d.calls.Add(1)
	return nil, errors.New("unexpected cancellation dial")
}

func (d *regressionPreCanceledDialer) Dial(string, string) (net.Conn, error) {
	return d.fail()
}

func (d *regressionPreCanceledDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.fail()
}

func TestProtocolRegressionPreCanceledContextStartsNoOperation(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *conn, *stmt, *copyin) error
	}{
		{"Conn.PrepareContext", func(ctx context.Context, cn *conn, _ *stmt, _ *copyin) error {
			_, err := cn.PrepareContext(ctx, "select 1")
			return err
		}},
		{"Conn.QueryContext", func(ctx context.Context, cn *conn, _ *stmt, _ *copyin) error {
			rows, err := cn.QueryContext(ctx, "select 1", nil)
			if rows != nil {
				_ = rows.Close()
			}
			return err
		}},
		{"Conn.ExecContext", func(ctx context.Context, cn *conn, _ *stmt, _ *copyin) error {
			_, err := cn.ExecContext(ctx, "select 1", nil)
			return err
		}},
		{"Conn.Ping", func(ctx context.Context, cn *conn, _ *stmt, _ *copyin) error {
			return cn.Ping(ctx)
		}},
		{"Conn.BeginTx", func(ctx context.Context, cn *conn, _ *stmt, _ *copyin) error {
			_, err := cn.BeginTx(ctx, driver.TxOptions{})
			return err
		}},
		{"Stmt.QueryContext", func(ctx context.Context, _ *conn, st *stmt, _ *copyin) error {
			rows, err := st.QueryContext(ctx, nil)
			if rows != nil {
				_ = rows.Close()
			}
			return err
		}},
		{"Stmt.ExecContext", func(ctx context.Context, _ *conn, st *stmt, _ *copyin) error {
			_, err := st.ExecContext(ctx, nil)
			return err
		}},
		{"CopyData", func(ctx context.Context, _ *conn, _ *stmt, ci *copyin) error {
			_, err := ci.CopyData(ctx, "row")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := new(regressionPreCanceledConn)
			dialer := new(regressionPreCanceledDialer)
			cn := &conn{
				c:      wire,
				buf:    bufio.NewReader(wire),
				dialer: dialer,
				cfg: Config{
					Host:               "cancel.invalid",
					Port:               1,
					SSLMode:            SSLModeDisable,
					MaxProtocolVersion: ProtocolVersion30,
				},
			}
			st := &stmt{cn: cn}
			ci := &copyin{cn: cn, buffer: []byte{byte('d'), 0, 0, 0, 0}, done: make(chan bool, 1)}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := tt.call(ctx, cn, st, ci)

			if !errors.Is(err, context.Canceled) {
				t.Errorf("operation error = %v; want %v", err, context.Canceled)
			}
			if written := wire.written.Load(); written != 0 {
				t.Errorf("pre-canceled operation wrote %d frontend bytes", written)
			}
			if calls := dialer.calls.Load(); calls != 0 {
				t.Errorf("pre-canceled operation opened %d cancellation connections", calls)
			}
			if wire.closed.Load() {
				t.Error("pre-canceled operation closed the primary connection")
			}
			if len(ci.buffer) != 5 {
				t.Errorf("pre-canceled operation mutated COPY buffer to %d bytes", len(ci.buffer))
			}
		})
	}
}
