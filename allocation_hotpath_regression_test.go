package pq

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestAllocationCopyExecContextWideRow(t *testing.T) {
	ci := &copyin{
		cn:     &conn{},
		buffer: make([]byte, 5, ciBufferSize),
	}
	values := make([]driver.NamedValue, 9)
	for i := range values {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: int64(i)}
	}

	if _, err := ci.ExecContext(context.Background(), values); err != nil {
		t.Fatal(err)
	}
	if got, want := string(ci.buffer[5:]), "0\t1\t2\t3\t4\t5\t6\t7\t8\n"; got != want {
		t.Fatalf("encoded COPY row = %q; want %q", got, want)
	}
}

func TestAllocationAppendEncodedTimestampText(t *testing.T) {
	value := time.Date(2026, time.August, 2, 12, 34, 56, 789123000, time.FixedZone("test", -7*60*60))
	prefix := []byte("prefix:")
	got, err := appendEncodedText(prefix, value)
	if err != nil {
		t.Fatal(err)
	}
	if want := "prefix:2026-08-02 12:34:56.789123-07:00"; string(got) != want {
		t.Fatalf("encoded timestamp = %q; want %q", got, want)
	}
}

func TestAllocationRecvNotificationCopiesCombinedFields(t *testing.T) {
	payload := binary.BigEndian.AppendUint32(nil, 1234)
	payload = append(payload, "channel"...)
	payload = append(payload, 0)
	payload = append(payload, "payload"...)
	payload = append(payload, 0)
	payload = append(payload, "trailing"...)
	r := readBuf(payload)

	notification := recvNotification(&r)
	if notification.BePid != 1234 || notification.Channel != "channel" || notification.Extra != "payload" {
		t.Fatalf("notification = %#v", notification)
	}
	if got := string(r); got != "trailing" {
		t.Fatalf("unconsumed notification payload = %q; want trailing", got)
	}

	for i := 4; i < len(payload)-len("trailing"); i++ {
		payload[i] = 'x'
	}
	if notification.Channel != "channel" || notification.Extra != "payload" {
		t.Fatal("notification fields retained the reusable protocol read buffer")
	}
}

func TestAllocationRecvNotificationEmptyFields(t *testing.T) {
	payload := binary.BigEndian.AppendUint32(nil, 1234)
	payload = append(payload, 0, 0)
	r := readBuf(payload)
	notification := recvNotification(&r)
	if notification.Channel != "" || notification.Extra != "" || len(r) != 0 {
		t.Fatalf("empty notification = %#v with %q remaining", notification, r)
	}
}

func TestAllocationRecvNotificationRequiresBothTerminators(t *testing.T) {
	for _, payload := range []string{"unterminated", "channel\x00unterminated"} {
		t.Run(strings.ReplaceAll(payload, "\x00", "_nul_"), func(t *testing.T) {
			raw := binary.BigEndian.AppendUint32(nil, 1234)
			raw = append(raw, payload...)
			r := readBuf(raw)
			defer func() {
				if recover() == nil {
					t.Fatal("recvNotification accepted an unterminated field")
				}
			}()
			recvNotification(&r)
		})
	}
}
