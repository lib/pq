package pq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

func TestConnectionStartupRegressionSQLContextCancelsOpen(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	startupRead := make(chan struct{})
	release := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		server, err := listener.Accept()
		if err != nil {
			return
		}
		defer server.Close()
		if !regressionReadStartupPacket(server) {
			return
		}
		close(startupRead)
		<-release
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%s user=test dbname=test sslmode=disable connect_timeout=0", host, port,
	))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- db.PingContext(ctx) }()
	regressionAwaitSignal(t, startupRead, "database/sql did not begin connection startup")
	cancel()

	returned := false
	select {
	case err := <-result:
		returned = true
		if !errors.Is(err, context.Canceled) {
			t.Errorf("PingContext error = %v; want context.Canceled", err)
		}
	case <-time.After(regressionOperationTimeout):
		t.Error("PingContext remained blocked in connection startup after its context was canceled")
	}

	close(release)
	_ = listener.Close()
	if !returned {
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("PingContext did not unblock after releasing the test peer")
		}
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("silent startup peer did not stop")
	}
}

func TestConnectionStartupRegressionSQLDriverTypePreserved(t *testing.T) {
	db, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, ok := db.Driver().(*Driver); !ok {
		t.Fatalf("sql.DB.Driver type = %T; want *pq.Driver", db.Driver())
	}
}

func TestConnectionStartupRegressionRejectsLongFrameBeforeAllocation(t *testing.T) {
	const childEnvironment = "PQ_CONNECTION_STARTUP_LONG_FRAME_CHILD"
	if os.Getenv(childEnvironment) == "1" {
		previousLimit := debug.SetMemoryLimit(128 << 20)
		defer debug.SetMemoryLimit(previousLimit)
		runtime.GC()

		const payloadLength = 64 << 20
		header := make([]byte, 5)
		header[0] = byte(proto.DataRow)
		binary.BigEndian.PutUint32(header[1:], payloadLength+4)
		script := newRegressionScriptConn(header)
		cn := &conn{c: script, buf: bufio.NewReader(script)}

		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := cn.startup(Config{MaxProtocolVersion: ProtocolVersion30})
		runtime.ReadMemStats(&after)
		if err == nil {
			t.Fatal("invalid startup DataRow was accepted")
		}
		if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
			t.Fatalf("invalid startup frame allocated %d bytes before rejection", allocated)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestConnectionStartupRegressionRejectsLongFrameBeforeAllocation$",
		"-test.count=1",
	)
	cmd.Env = append(os.Environ(), childEnvironment+"=1", "GOMEMLIMIT=128MiB",
		"PQGO_DEBUG=1", "PQTEST_USE_TESTCONTAINERS=false")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("long-frame subprocess did not terminate: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("startup frame was not rejected before allocation: %v\n%s", err, output)
	}
}

func TestConnectionStartupRegressionTSADrainFailureRejectsConnection(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_bool, 0)),
		regressionBackendFrame(proto.DataRow, regressionSingleColumnData([]byte("f"))),
	}, nil)
	script := newRegressionScriptConn(wire)
	connector, err := NewConnectorConfig(Config{
		Host:               "tsa.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModeDisable,
		MaxProtocolVersion: ProtocolVersion30,
		TargetSessionAttrs: TargetSessionAttrsPrimary,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(protocolLifecycleFixedDialer{conn: script})

	got, err := connector.Connect(context.Background())
	if err == nil {
		if got != nil {
			_ = got.Close()
		}
		t.Fatal("target-session check returned a connection after its result drain failed")
	}
}

func TestConnectionStartupRegressionDirectTLSRequiresALPN(t *testing.T) {
	certificate, err := tls.LoadX509KeyPair("testdata/ssl/server.crt", "testdata/ssl/server.key")
	if err != nil {
		t.Fatal(err)
	}
	client, serverRaw := net.Pipe()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		defer serverRaw.Close()
		server := tls.Server(serverRaw, &tls.Config{Certificates: []tls.Certificate{certificate}})
		if err := server.Handshake(); err != nil {
			return
		}
		if !regressionReadStartupPacket(server) {
			return
		}
		_, _ = server.Write(bytes.Join([][]byte{
			regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
			regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		}, nil))
		_, _ = io.Copy(io.Discard, server)
	}()

	connector, err := NewConnectorConfig(Config{
		Host:               "direct.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModeRequire,
		SSLNegotiation:     SSLNegotiationDirect,
		MaxProtocolVersion: ProtocolVersion30,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(protocolLifecycleFixedDialer{conn: client})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := connector.Connect(ctx)
	if err == nil {
		if got != nil {
			_ = got.Close()
		}
		t.Error("direct TLS accepted a peer that did not negotiate PostgreSQL ALPN")
	}
	_ = client.Close()
	_ = serverRaw.Close()
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("direct TLS test peer did not stop")
	}
}

func TestConnectionStartupRegressionAuthenticationChallengeRequiresOK(t *testing.T) {
	payload := binary.BigEndian.AppendUint32(nil, uint32(proto.AuthReqPassword))
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.AuthenticationRequest, payload),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	if err := cn.startup(Config{
		User: "test", Password: "secret", MaxProtocolVersion: ProtocolVersion30,
	}); err == nil {
		t.Fatal("startup accepted ReadyForQuery after an authentication challenge without AuthenticationOk")
	}
}

func TestConnectionStartupRegressionAuthenticationOKDoesNotCoverLaterChallenge(t *testing.T) {
	payload := binary.BigEndian.AppendUint32(nil, uint32(proto.AuthReqPassword))
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
		regressionBackendFrame(proto.AuthenticationRequest, payload),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	if err := cn.startup(Config{
		User: "test", Password: "secret", MaxProtocolVersion: ProtocolVersion30,
	}); err == nil {
		t.Fatal("startup treated an earlier AuthenticationOk as acknowledgement of a later challenge")
	}
}

type connectionStartupDeadlineRejectConn struct{ net.Conn }

func (c *connectionStartupDeadlineRejectConn) SetDeadline(time.Time) error {
	return errors.New("deadlines are not supported")
}

func TestConnectionStartupRegressionCancellationClosesDeadlineRejectingConn(t *testing.T) {
	clientRaw, server := net.Pipe()
	client := &connectionStartupDeadlineRejectConn{Conn: clientRaw}
	startupRead := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if regressionReadStartupPacket(server) {
			close(startupRead)
		}
		_, _ = io.Copy(io.Discard, server)
	}()

	connector, err := NewConnectorConfig(Config{
		Host:               "deadline.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModeDisable,
		MaxProtocolVersion: ProtocolVersion30,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(protocolLifecycleFixedDialer{conn: client})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := connector.Connect(ctx)
		result <- err
	}()
	regressionAwaitSignal(t, startupRead, "connection startup packet was not sent")
	cancel()

	returned := false
	select {
	case err := <-result:
		returned = true
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Connect error = %v; want context.Canceled", err)
		}
	case <-time.After(regressionOperationTimeout):
		t.Error("Connect remained blocked after cancellation because SetDeadline failed")
	}
	_ = clientRaw.Close()
	_ = server.Close()
	if !returned {
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("Connect did not unblock after closing the deadline-rejecting connection")
		}
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("deadline-rejecting test peer did not stop")
	}
}

type connectionStartupPreferFallbackDialer struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (d *connectionStartupPreferFallbackDialer) dial() (net.Conn, error) {
	client, server := net.Pipe()
	d.mu.Lock()
	index := len(d.conns) / 2
	d.conns = append(d.conns, client, server)
	d.mu.Unlock()

	switch index {
	case 0:
		go func() {
			defer server.Close()
			if !regressionReadStartupPacket(server) {
				return
			}
			_, _ = server.Write([]byte{'S'})
			recordHeader := make([]byte, 5)
			if _, err := io.ReadFull(server, recordHeader); err != nil {
				return
			}
			recordLength := int64(binary.BigEndian.Uint16(recordHeader[3:]))
			if _, err := io.CopyN(io.Discard, server, recordLength); err != nil {
				return
			}
			_, _ = server.Write([]byte("abcde"))
		}()
	case 1:
		go func() {
			defer server.Close()
			if !regressionReadStartupPacket(server) {
				return
			}
			_, _ = server.Write(bytes.Join([][]byte{
				regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
				regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
			}, nil))
			// Keep the successful fallback connection alive until the client
			// closes it. Closing immediately after ReadyForQuery races with the
			// connector resetting its startup deadline.
			_, _ = io.Copy(io.Discard, server)
		}()
	default:
		_ = client.Close()
		_ = server.Close()
		return nil, fmt.Errorf("unexpected prefer fallback dial %d", index+1)
	}
	return client, nil
}

func (d *connectionStartupPreferFallbackDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *connectionStartupPreferFallbackDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func (d *connectionStartupPreferFallbackDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, cn := range d.conns {
		_ = cn.Close()
	}
}

func (d *connectionStartupPreferFallbackDialer) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns) / 2
}

func TestConnectionStartupRegressionPreferFallsBackAfterTLSHandshakeFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dialer := new(connectionStartupPreferFallbackDialer)
	defer dialer.Close()
	connector, err := NewConnectorConfig(Config{
		Host:               "prefer.invalid",
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

	got, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("sslmode=prefer did not fall back after TLS handshake failure: %v", err)
	}
	defer got.Close()
	if count := dialer.Count(); count != 2 {
		t.Fatalf("sslmode=prefer used %d connections; want TLS attempt plus plaintext fallback", count)
	}
}

var errConnectionStartupPreferStandbyDial = errors.New("intentional prefer-standby dial failure")

type connectionStartupPreferStandbyLoopDialer struct {
	mu    sync.Mutex
	calls int
	third net.Conn
}

func (d *connectionStartupPreferStandbyLoopDialer) dial() (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls <= 2 {
		return nil, errConnectionStartupPreferStandbyDial
	}
	return d.third, nil
}

func (d *connectionStartupPreferStandbyLoopDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *connectionStartupPreferStandbyLoopDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.dial()
}

func (d *connectionStartupPreferStandbyLoopDialer) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func TestConnectionStartupRegressionPreferStandbyFallbackIsOneShot(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	dialer := &connectionStartupPreferStandbyLoopDialer{third: script}
	connector, err := NewConnectorConfig(Config{
		Host:               "prefer-standby.invalid",
		Port:               1,
		User:               "test",
		Database:           "test",
		SSLMode:            SSLModeDisable,
		MaxProtocolVersion: ProtocolVersion30,
		TargetSessionAttrs: TargetSessionAttrsPreferStandby,
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(dialer)

	got, err := connector.Connect(context.Background())
	if got != nil {
		_ = got.Close()
	}
	if err == nil {
		t.Error("prefer-standby kept restarting after the any-host fallback pass failed")
	}
	if !errors.Is(err, errConnectionStartupPreferStandbyDial) {
		t.Errorf("Connect error = %v; want intentional dial failure", err)
	}
	if calls := dialer.Count(); calls != 2 {
		t.Errorf("prefer-standby made %d dial attempts; want one preferred pass plus one any-host pass", calls)
	}
}

func TestConnectionStartupRegressionTSAQueriesReadOnlyOnce(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0}),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_bool, 0)),
		regressionBackendFrame(proto.DataRow, regressionSingleColumnData([]byte("t"))),
		regressionBackendFrame(proto.CommandComplete, []byte("SHOW\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	connector, err := NewConnectorConfig(Config{
		Host:               "tsa.invalid",
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
	connector.Dialer(protocolLifecycleFixedDialer{conn: script})

	got, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("target-session check issued more than one transaction_read_only query: %v", err)
	}
	_ = got.Close()
}

func TestConnectionStartupRegressionConnectTimeoutOverflowRejected(t *testing.T) {
	if _, err := newConfig("connect_timeout=9223372037", []string{"PGUSER=test"}); err == nil {
		t.Fatal("positive connect_timeout overflow was accepted as an unbounded timeout")
	}
}
