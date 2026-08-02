package pq

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

func TestProtocolRegressionFixedAuthenticationPayloadLengths(t *testing.T) {
	tests := []struct {
		name    string
		code    proto.AuthCode
		trailer []byte
	}{
		{"authentication ok", proto.AuthReqOk, []byte{0}},
		{"cleartext password", proto.AuthReqPassword, []byte{0}},
		{"md5 password", proto.AuthReqMD5, []byte{1, 2, 3, 4, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := binary.BigEndian.AppendUint32(nil, uint32(tt.code))
			payload = append(payload, tt.trailer...)
			wire := bytes.Join([][]byte{
				regressionBackendFrame(proto.AuthenticationRequest, payload),
				regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
			}, nil)
			script := newRegressionScriptConn(wire)
			cn := &conn{c: script, buf: bufio.NewReader(script)}

			if err := cn.startup(Config{MaxProtocolVersion: ProtocolVersion30}); err == nil {
				t.Fatal("malformed authentication request was accepted")
			}
			if err := cn.err.get(); err != driver.ErrBadConn {
				t.Errorf("malformed authentication request left connection reusable: %v", err)
			}
		})
	}
}

func TestProtocolRegressionSASLMechanismNegotiation(t *testing.T) {
	tests := []struct {
		name       string
		mechanisms string
	}{
		{"unterminated list", "SCRAM-SHA-256\x00"},
		{"unsupported mechanism", "SCRAM-SHA-256-PLUS\x00\x00"},
		{"empty list", "\x00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := binary.BigEndian.AppendUint32(nil, uint32(proto.AuthReqSASL))
			payload = append(payload, tt.mechanisms...)
			script := &regressionRecordingConn{
				regressionScriptConn: newRegressionScriptConn(
					regressionBackendFrame(proto.AuthenticationRequest, payload),
				),
			}
			cn := &conn{c: script, buf: bufio.NewReader(script)}

			if err := cn.startup(Config{MaxProtocolVersion: ProtocolVersion30}); err == nil {
				t.Fatal("invalid SASL mechanism advertisement was accepted")
			}
			if len(script.writes) != 1 {
				t.Fatalf("client responded with an unadvertised SASL mechanism: got %d writes, want only the startup packet", len(script.writes))
			}
		})
	}
}

func TestProtocolRegressionRowsRejectDescriptionAfterError(t *testing.T) {
	wire := bytes.Join([][]byte{
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
		regressionBackendFrame(proto.DataRow, regressionNullColumnData(1)),
		regressionBackendFrame(proto.ErrorResponse, regressionBackendError("XX000", "regression failure")),
		regressionBackendFrame(proto.RowDescription, regressionSingleColumnDescription(oid.T_int8, 0)),
		regressionBackendFrame(proto.DataRow, regressionNullColumnData(1)),
		regressionBackendFrame(proto.CommandComplete, []byte("SELECT 1\x00")),
		regressionBackendFrame(proto.ReadyForQuery, []byte{'I'}),
	}, nil)
	script := newRegressionScriptConn(wire)
	cn := &conn{c: script, buf: bufio.NewReader(script)}
	rows, err := cn.simpleQuery("select one; malformed transition")
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(make([]driver.Value, 1)); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := rows.Next(make([]driver.Value, 1)); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("RowDescription after ErrorResponse did not return the server error: %v", err)
	}
	if err := cn.err.get(); err != driver.ErrBadConn {
		t.Errorf("invalid result transition left connection reusable: %v", err)
	}
}

type regressionRecordingConn struct {
	*regressionScriptConn
	writes [][]byte
}

func (c *regressionRecordingConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, bytes.Clone(p))
	return len(p), nil
}

func regressionBackendError(code, message string) []byte {
	payload := []byte{'S'}
	payload = append(payload, "ERROR\x00"...)
	payload = append(payload, 'C')
	payload = append(payload, code...)
	payload = append(payload, 0, 'M')
	payload = append(payload, message...)
	return append(payload, 0, 0)
}
