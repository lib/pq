package pq

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

var (
	benchmarkBoolSink      bool
	benchmarkCodeSink      proto.ResponseCode
	benchmarkConnectorSink *Connector
	benchmarkNotifySink    *Notification
	benchmarkResultSink    driver.Result
)

// benchmarkCircularConn repeats a scripted backend transcript indefinitely.
// It deliberately implements deadline methods so lifecycle benchmarks include
// the driver's deadline bookkeeping without involving a kernel socket.
type benchmarkCircularConn struct {
	content   []byte
	prefixLen int
	pos       int
}

func (c *benchmarkCircularConn) Read(p []byte) (int, error) {
	n := copy(p, c.content[c.pos:])
	c.pos += n
	if c.pos >= len(c.content) {
		c.pos = c.prefixLen
	}
	return n, nil
}

func (*benchmarkCircularConn) Write(p []byte) (int, error) { return len(p), nil }
func (*benchmarkCircularConn) Close() error                { return nil }
func (*benchmarkCircularConn) LocalAddr() net.Addr         { return nil }
func (*benchmarkCircularConn) RemoteAddr() net.Addr        { return nil }
func (*benchmarkCircularConn) SetDeadline(time.Time) error { return nil }
func (*benchmarkCircularConn) SetReadDeadline(time.Time) error {
	return nil
}
func (*benchmarkCircularConn) SetWriteDeadline(time.Time) error {
	return nil
}

// benchmarkListenerConn is a minimal PostgreSQL peer for measuring listener
// dispatch after bytes are available to the driver. Writes enqueue the normal
// startup and LISTEN acknowledgements; benchmarks inject notifications through
// the same read path.
type benchmarkListenerConn struct {
	responses chan []byte
	pending   []byte
	closeOnce sync.Once
}

func newBenchmarkListenerConn() *benchmarkListenerConn {
	return &benchmarkListenerConn{responses: make(chan []byte, 4)}
}

func (c *benchmarkListenerConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		response, ok := <-c.responses
		if !ok {
			return 0, io.EOF
		}
		c.pending = response
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *benchmarkListenerConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	switch proto.RequestCode(p[0]) {
	case proto.Query:
		c.responses <- benchmarkCommandResponse("LISTEN")
	case proto.Terminate:
	default:
		// Startup packets begin with their length rather than a request code.
		startup := benchmarkBackendFrame(proto.AuthenticationRequest, []byte{0, 0, 0, 0})
		startup = append(startup, benchmarkBackendFrame(proto.BackendKeyData,
			[]byte{0, 0, 0, 1, 0, 0, 0, 2})...)
		startup = append(startup, benchmarkBackendFrame(proto.ReadyForQuery, []byte{'I'})...)
		c.responses <- startup
	}
	return len(p), nil
}

func (c *benchmarkListenerConn) Close() error {
	c.closeOnce.Do(func() { close(c.responses) })
	return nil
}

func (*benchmarkListenerConn) LocalAddr() net.Addr               { return nil }
func (*benchmarkListenerConn) RemoteAddr() net.Addr              { return nil }
func (*benchmarkListenerConn) SetDeadline(time.Time) error       { return nil }
func (*benchmarkListenerConn) SetReadDeadline(time.Time) error   { return nil }
func (*benchmarkListenerConn) SetWriteDeadline(time.Time) error  { return nil }
func (c *benchmarkListenerConn) injectNotification(frame []byte) { c.responses <- frame }

type benchmarkListenerDialer struct{ conn net.Conn }

func (d benchmarkListenerDialer) Dial(string, string) (net.Conn, error) {
	return d.conn, nil
}

func (d benchmarkListenerDialer) DialTimeout(string, string, time.Duration) (net.Conn, error) {
	return d.conn, nil
}

func newBenchmarkConn(prefixLen int, transcript []byte) *conn {
	c := &benchmarkCircularConn{content: transcript, prefixLen: prefixLen}
	return &conn{
		c:         c,
		buf:       bufio.NewReader(c),
		txnStatus: txnStatusIdle,
	}
}

func benchmarkBackendFrame(code proto.ResponseCode, payload []byte) []byte {
	frame := make([]byte, 5, len(payload)+5)
	frame[0] = byte(code)
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)+4))
	return append(frame, payload...)
}

func benchmarkDataRow(columns, width int) []byte {
	payload := make([]byte, 2, 2+columns*(4+width))
	binary.BigEndian.PutUint16(payload, uint16(columns))
	value := strings.Repeat("x", width)
	for range columns {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(width))
		payload = append(payload, length[:]...)
		payload = append(payload, value...)
	}
	return benchmarkBackendFrame(proto.DataRow, payload)
}

func benchmarkRowDescription(columns int, typ oid.Oid) []byte {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(columns))
	for i := range columns {
		payload = append(payload, "column_"...)
		payload = strconv.AppendInt(payload, int64(i), 10)
		payload = append(payload, 0)
		payload = binary.BigEndian.AppendUint32(payload, 0) // table OID
		payload = binary.BigEndian.AppendUint16(payload, 0) // column attribute
		payload = binary.BigEndian.AppendUint32(payload, uint32(typ))
		payload = binary.BigEndian.AppendUint16(payload, 0xffff)     // variable length
		payload = binary.BigEndian.AppendUint32(payload, 0xffffffff) // no type modifier
		payload = binary.BigEndian.AppendUint16(payload, 0)          // text format
	}
	return benchmarkBackendFrame(proto.RowDescription, payload)
}

func benchmarkWithoutProtocolDebug(b *testing.B) {
	b.Helper()
	wasDebugging := debugProto
	debugProto = false
	b.Cleanup(func() { debugProto = wasDebugging })
}

// BenchmarkBackendFrameProcessing measures the steady-state cost added to
// every backend message. DataRow/1x188 and DataRow/16x8 have identical frame
// sizes, isolating the effect of validating more columns.
func BenchmarkBackendFrameProcessing(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)

	benchmarks := []struct {
		name  string
		frame []byte
	}{
		{"ReadyForQuery", benchmarkBackendFrame(proto.ReadyForQuery, []byte{'I'})},
		{"DataRow/1x32", benchmarkDataRow(1, 32)},
		{"DataRow/1x188", benchmarkDataRow(1, 188)},
		{"DataRow/16x8", benchmarkDataRow(16, 8)},
		{"RowDescription/16", benchmarkRowDescription(16, oid.T_text)},
		{"ErrorResponse", benchmarkBackendFrame(proto.ErrorResponse,
			[]byte("SERROR\x00C23505\x00Mduplicate key value violates unique constraint\x00\x00"))},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			cn := newBenchmarkConn(0, benchmark.frame)
			var rb readBuf
			b.ReportAllocs()
			b.SetBytes(int64(len(benchmark.frame)))
			b.ResetTimer()
			for range b.N {
				code, err := cn.recvMessage(&rb)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkCodeSink = code
			}
		})
	}
}

// BenchmarkFrontendMessageWrite isolates the checked-write path used by all
// frontend protocol messages.
func BenchmarkFrontendMessageWrite(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	cn := newBenchmarkConn(0, nil)
	message := cn.writeBuf(proto.Query)
	message.string("select 1")

	b.ReportAllocs()
	b.SetBytes(int64(len(message.buf)))
	b.ResetTimer()
	for range b.N {
		if err := cn.send(message); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCommandResponse(tag string) []byte {
	response := benchmarkBackendFrame(proto.CommandComplete, append([]byte(tag), 0))
	return append(response, benchmarkBackendFrame(proto.ReadyForQuery, []byte{'I'})...)
}

// BenchmarkConnectionExec covers the normal query fast path, including
// connection-state checks, cancellation setup, writes, and response parsing.
func BenchmarkConnectionExec(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	response := benchmarkCommandResponse("UPDATE 1")

	benchmarks := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{"Background", func() (context.Context, context.CancelFunc) {
			return context.Background(), func() {}
		}},
		{"Cancelable", func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			cn := newBenchmarkConn(0, response)
			ctx, cancel := benchmark.ctx()
			defer cancel()
			baselineGoroutines := runtime.NumGoroutine()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := cn.ExecContext(ctx, "update benchmark set n=n+1", nil)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkResultSink = result
				if benchmark.name == "Cancelable" && (i&255 == 255 || i == b.N-1) {
					// Upstream returns before its successful watcher goroutine has
					// necessarily exited. Drain it outside the timed caller-latency
					// measurement so its work cannot spill into another benchmark.
					b.StopTimer()
					benchmarkDrainWatcherGoroutines(b, baselineGoroutines)
					if i != b.N-1 {
						b.StartTimer()
					}
				}
			}
		})
	}
}

// BenchmarkConnectionStateChecks measures hooks database/sql invokes while
// checking out and returning pooled connections.
func BenchmarkConnectionStateChecks(b *testing.B) {
	cn := &conn{}

	b.Run("IsValid", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBoolSink = cn.IsValid()
		}
	})
	b.Run("ResetSession", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := cn.ResetSession(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCancellationWatcher isolates successful cancellation-watcher
// cleanup, which is paid by every operation using a context with a Done channel.
func BenchmarkCancellationWatcher(b *testing.B) {
	cn := &conn{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	baselineGoroutines := runtime.NumGoroutine()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cn.watchCancel(ctx, false)()
		if i&255 == 255 || i == b.N-1 {
			b.StopTimer()
			benchmarkDrainWatcherGoroutines(b, baselineGoroutines)
			if i != b.N-1 {
				b.StartTimer()
			}
		}
	}
}

func benchmarkDrainWatcherGoroutines(b *testing.B, baseline int) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			b.Fatal("successful cancellation watcher goroutines did not exit")
		}
		runtime.Gosched()
	}
}

func BenchmarkConfigParsing(b *testing.B) {
	benchmarks := []struct {
		name string
		dsn  string
	}{
		{"SingleHost", "host=localhost port=5432 user=benchmark dbname=benchmark sslmode=disable"},
		{"MultiHost", "host=one,two,three hostaddr=127.0.0.1,127.0.0.2,127.0.0.3 " +
			"port=5432,5433,5434 user=benchmark dbname=benchmark sslmode=disable " +
			"target_session_attrs=any"},
		{"URL", "postgres://benchmark:password@localhost:5432/benchmark?" +
			"sslmode=disable&application_name=pq-benchmark&connect_timeout=5"},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				cfg, err := newConfig(benchmark.dsn, nil)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBoolSink = cfg.Port != 0
			}
		})
	}
}

func BenchmarkConnectorConstruction(b *testing.B) {
	cfg, err := newConfig("host=one,two,three port=5432,5433,5434 "+
		"user=benchmark dbname=benchmark sslmode=disable target_session_attrs=any", nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		connector, err := NewConnectorConfig(cfg)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkConnectorSink = connector
	}
}

// BenchmarkCopyInBuffering isolates per-row COPY synchronization and encoding
// while keeping the buffer below the network flush threshold.
func BenchmarkCopyInBuffering(b *testing.B) {
	benchmarks := []struct {
		name   string
		values []driver.Value
		line   string
	}{
		{"Values", []driver.Value{int64(42), "hello\tworld", []byte("payload"), true}, ""},
		{"RawData", nil, strings.Repeat("x", 128)},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			ci := &copyin{
				cn:     &conn{},
				buffer: make([]byte, 5, ciBufferSize),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var (
					result driver.Result
					err    error
				)
				if benchmark.values != nil {
					result, err = ci.Exec(benchmark.values)
				} else {
					result, err = ci.CopyData(context.Background(), benchmark.line)
				}
				if err != nil {
					b.Fatal(err)
				}
				benchmarkResultSink = result
				ci.buffer = ci.buffer[:5]
			}
		})
	}
}

// BenchmarkPreparedStatementLifecycle exercises the extra synchronization,
// private buffer construction, time lookup, and deadline method calls in
// statement shutdown. The scripted connection's no-op deadline method
// deliberately excludes netpoll or kernel deadline costs.
func BenchmarkPreparedStatementLifecycle(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	prepare := benchmarkBackendFrame(proto.ParseComplete, nil)
	prepare = append(prepare, benchmarkBackendFrame(proto.ParameterDescription, []byte{0, 0})...)
	prepare = append(prepare, benchmarkRowDescription(1, oid.T_int4)...)
	prepare = append(prepare, benchmarkBackendFrame(proto.ReadyForQuery, []byte{'I'})...)
	closeResponse := benchmarkBackendFrame(proto.CloseComplete, nil)
	closeResponse = append(closeResponse, benchmarkBackendFrame(proto.ReadyForQuery, []byte{'I'})...)
	transcript := append(prepare, closeResponse...)
	cn := newBenchmarkConn(0, transcript)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stmt, err := cn.PrepareContext(context.Background(), "select 1")
		if err != nil {
			b.Fatal(err)
		}
		if err := stmt.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListenerNotificationDelivery measures the forwarding path from a
// scripted net.Conn through ListenerConn and Listener to the public channel.
// The timed interval includes the scripted peer's channel handoff. PostgreSQL
// execution, socket I/O, and network latency are intentionally outside its
// scope.
func BenchmarkListenerNotificationDelivery(b *testing.B) {
	benchmarkWithoutProtocolDebug(b)
	peer := newBenchmarkListenerConn()
	connected := make(chan struct{}, 1)
	listener := NewDialListener(
		benchmarkListenerDialer{conn: peer},
		"host=benchmark port=5432 user=benchmark dbname=benchmark sslmode=disable",
		time.Hour,
		time.Hour,
		func(event ListenerEventType, _ error) {
			if event == ListenerEventConnected {
				select {
				case connected <- struct{}{}:
				default:
				}
			}
		},
	)

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		b.Fatal("listener did not connect to scripted backend")
	}
	if err := listener.Listen("benchmark_notifications"); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := listener.Close(); err != nil {
			b.Errorf("close listener: %v", err)
		}
		for range listener.Notify {
		}
	})

	payload := binary.BigEndian.AppendUint32(nil, 1)
	payload = append(payload, "benchmark_notifications"...)
	payload = append(payload, 0)
	payload = append(payload, "payload"...)
	payload = append(payload, 0)
	frame := benchmarkBackendFrame(proto.NotificationResponse, payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for range b.N {
		peer.injectNotification(frame)
		notification, ok := <-listener.Notify
		if !ok {
			b.Fatal("listener closed during notification delivery")
		}
		benchmarkNotifySink = notification
	}
}
