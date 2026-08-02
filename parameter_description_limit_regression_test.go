package pq

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/lib/pq/internal/proto"
)

func TestProtocolRegressionParameterDescriptionCappedBeforeAllocation(t *testing.T) {
	const maxParameterDescriptionPayload = 2 + 65535*4
	header := make([]byte, 5)
	header[0] = byte(proto.ParameterDescription)
	binary.BigEndian.PutUint32(header[1:], maxParameterDescriptionPayload+4+1)

	cn := &conn{buf: bufio.NewReader(newRegressionScriptConn(header))}
	_, err := cn.recvMessage(new(readBuf))
	if err == nil {
		t.Fatal("oversized ParameterDescription was accepted")
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("oversized ParameterDescription was read before its type-specific length cap: %v", err)
	}
}
