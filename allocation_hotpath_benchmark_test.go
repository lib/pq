package pq

// Run the in-memory allocation suite without starting PostgreSQL with:
//
//	PQTEST_PURE_BENCHMARK=1 go test -run '^$' -bench Allocation -benchmem ./...

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"testing"
	"time"
)

var (
	allocationBufferSink       []byte
	allocationNotificationSink *Notification
	allocationResultSink       driver.Result
)

func BenchmarkAllocationCopyExecContext(b *testing.B) {
	values := []driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: "hello\tworld"},
		{Ordinal: 3, Value: []byte("payload")},
		{Ordinal: 4, Value: true},
	}
	ci := &copyin{
		cn:     &conn{},
		buffer: make([]byte, 5, ciBufferSize),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ci.ExecContext(context.Background(), values)
		if err != nil {
			b.Fatal(err)
		}
		allocationResultSink = result
		ci.buffer = ci.buffer[:5]
	}
}

func BenchmarkAllocationAppendEncodedByteaText(b *testing.B) {
	value := make([]byte, 128)
	buf := make([]byte, 0, 3+2*len(value))

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for range b.N {
		var err error
		allocationBufferSink, err = appendEncodedText(buf[:0], value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocationAppendEncodedTimestampText(b *testing.B) {
	var value any = time.Date(2026, time.August, 2, 12, 34, 56, 789123000, time.FixedZone("benchmark", -7*60*60))
	buf := make([]byte, 0, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		allocationBufferSink, err = appendEncodedText(buf[:0], value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocationRecvNotification(b *testing.B) {
	payload := binary.BigEndian.AppendUint32(nil, 1234)
	payload = append(payload, "benchmark_channel"...)
	payload = append(payload, 0)
	payload = append(payload, "benchmark payload"...)
	payload = append(payload, 0)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		r := readBuf(payload)
		allocationNotificationSink = recvNotification(&r)
	}
}

func BenchmarkAllocationListenerReconnectQueue(b *testing.B) {
	listener := &Listener{
		Notify:            make(chan *Notification),
		done:              make(chan struct{}),
		notificationQueue: make(chan listenerNotification, listenerChannelCapacity),
	}
	dispatcherDone := make(chan struct{})
	go func() {
		listener.notificationDispatcher()
		close(dispatcherDone)
	}()
	b.Cleanup(func() {
		close(listener.done)
		<-dispatcherDone
	})

	barrier := newListenerEventBarrier()
	barrier.complete()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !listener.sendReconnectNotification(barrier) {
			b.Fatal("notification dispatcher stopped")
		}
		if notification := <-listener.Notify; notification != nil {
			b.Fatalf("reconnect marker = %#v; want nil", notification)
		}
	}
}
