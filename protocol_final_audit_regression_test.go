package pq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

func TestProtocolRegressionListenerPingReturnsBackendError(t *testing.T) {
	want := errors.New("backend rejected ping")
	script := newRegressionScriptConn(nil)
	l := &ListenerConn{
		cn:        &conn{c: script},
		connState: connStateIdle,
		replyChan: make(chan message, 2),
		done:      make(chan struct{}),
	}
	l.replyChan <- message{typ: proto.ErrorResponse, err: want}
	l.replyChan <- message{typ: proto.ReadyForQuery}

	panicValue, err := regressionCallWithoutPanic(l.Ping)
	if panicValue != nil {
		t.Fatalf("ListenerConn.Ping panicked on a backend error: %v", panicValue)
	}
	if !errors.Is(err, want) {
		t.Fatalf("ListenerConn.Ping error = %v; want %v", err, want)
	}
}

func TestProtocolRegressionCopyRejectsBinaryColumnFormat(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.CopyInResponse, regressionCopyResponse(0, 1)),
		regressionBackendFrame(proto.ErrorResponse, regressionBackendError("57014", "COPY aborted")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'T'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script), txnStatus: txnStatusIdleInTransaction}
	st, err := cn.prepareCopyIn("COPY regression FROM STDIN")
	if st != nil {
		_ = st.Close()
	}
	if !errors.Is(err, errBinaryCopyNotSupported) {
		t.Fatalf("COPY with a binary column format returned %v; want %v", err, errBinaryCopyNotSupported)
	}
}

func TestProtocolRegressionCopyRejectsInvalidFormatEnums(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"global format", regressionCopyResponse(2)},
		{"column format", regressionCopyResponse(0, 2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := newRegressionScriptConn(regressionBackendFrame(proto.CopyInResponse, tt.payload))
			cn := &conn{c: script, buf: bufio.NewReader(script)}
			if _, err := cn.recvMessage(new(readBuf)); err == nil {
				t.Fatal("invalid COPY format code was accepted")
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Errorf("invalid COPY format code left connection reusable: %v", err)
			}
		})
	}
}

func TestProtocolRegressionCopyRejectsResponseAfterError(t *testing.T) {
	tests := []struct {
		name     string
		response proto.ResponseCode
	}{
		{"copy in", proto.CopyInResponse},
		{"copy out", proto.CopyOutResponse},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := bytes.Join([][]byte{
				regressionBackendFrame(proto.ErrorResponse, regressionBackendError("XX000", "COPY failed")),
				regressionBackendFrame(tt.response, regressionCopyResponse(0)),
				regressionBackendFrame(proto.ReadyForQuery, []byte{'T'}),
			}, nil)
			script := newRegressionScriptConn(wire)
			cn := &conn{c: script, buf: bufio.NewReader(script), txnStatus: txnStatusIdleInTransaction}
			st, err := cn.prepareCopyIn("COPY regression FROM STDIN")
			if st != nil {
				_ = st.Close()
			}
			if err == nil {
				t.Fatal("COPY response after ErrorResponse discarded the server error")
			}
			if bad := cn.err.get(); bad != driver.ErrBadConn {
				t.Errorf("invalid COPY response transition left connection reusable: %v", bad)
			}
		})
	}
}

func TestProtocolRegressionSimpleQueryRejectsErrorThenDescription(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.ErrorResponse, regressionBackendError("XX000", "query failed")),
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
		regressionBackendFrame(proto.DataRow, regressionNullColumnData(1)),
		regressionBackendFrame(proto.CommandComplete, []byte("SELECT 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	rows, err := cn.simpleQuery("malformed query transition")
	if rows != nil {
		_ = rows.Close()
	}
	if err == nil {
		t.Fatal("RowDescription after ErrorResponse discarded the server error")
	}
	if bad := cn.err.get(); bad != driver.ErrBadConn {
		t.Errorf("invalid simple-query transition left connection reusable: %v", bad)
	}
}

func TestProtocolRegressionSimpleQueryRejectsPrematureReady(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	rows, err := cn.simpleQuery("select regression")
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, errUnexpectedReady) {
		t.Fatalf("premature ReadyForQuery returned %v; want %v", err, errUnexpectedReady)
	}
}

func TestProtocolRegressionCancellationUnblocksUnresponsivePrimary(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *conn) error
	}{
		{
			name: "connection",
			run: func(ctx context.Context, cn *conn) error {
				_, err := cn.ExecContext(ctx, "select blocked", nil)
				return err
			},
		},
		{
			name: "prepared statement",
			run: func(ctx context.Context, cn *conn) error {
				_, err := (&stmt{cn: cn}).ExecContext(ctx, nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			requestReceived := make(chan struct{})
			go func() {
				buf := make([]byte, 1024)
				_, _ = server.Read(buf)
				close(requestReceived)
				_, _ = io.Copy(io.Discard, server)
			}()

			cancelConn := newRegressionScriptConn(nil)
			cn := &conn{
				c:      client,
				buf:    bufio.NewReader(client),
				dialer: protocolLifecycleFixedDialer{conn: cancelConn},
				cfg:    Config{Host: "cancel.invalid", Port: 1, SSLMode: SSLModeDisable},
			}
			cleanup := func() {
				_ = client.Close()
				_ = server.Close()
			}
			defer cleanup()

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- tt.run(ctx, cn) }()
			regressionAwaitSignal(t, requestReceived, "primary query was not sent")
			cancel()
			regressionExpectResultBeforeTimeout(t, result, cleanup)
		})
	}
}

func TestProtocolRegressionLargeParameterDescriptionAccepted(t *testing.T) {
	const count = 7500
	payload := binary.BigEndian.AppendUint16(nil, count)
	for range count {
		payload = binary.BigEndian.AppendUint32(payload, uint32(oid.T_int4))
	}
	if len(payload) <= proto.MaxMsgLen {
		t.Fatalf("test payload length %d does not exceed the ordinary message limit", len(payload))
	}

	script := newRegressionScriptConn(regressionBackendFrame(proto.ParameterDescription, payload))
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	var got readBuf
	typ, err := cn.recvMessage(&got)
	if err != nil {
		t.Fatalf("legal large ParameterDescription was rejected: %v", err)
	}
	if typ != proto.ParameterDescription || !bytes.Equal(got, payload) {
		t.Fatalf("large ParameterDescription changed in transit: type=%v bytes=%d", typ, len(got))
	}
	if err := cn.err.get(); err != nil {
		t.Errorf("legal large ParameterDescription poisoned connection: %v", err)
	}
}

func regressionCopyResponse(overall byte, columns ...uint16) []byte {
	payload := []byte{overall}
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(columns)))
	for _, format := range columns {
		payload = binary.BigEndian.AppendUint16(payload, format)
	}
	return payload
}
