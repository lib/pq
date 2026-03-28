package pq

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

// TestConnectionPoisoningUnexpectedEOF is a regression test for connection
// poisoning caused by io.ErrUnexpectedEOF.
//
// When a DataRow message is truncated mid-body (partial TCP read), pq's
// recvMessage returns io.ErrUnexpectedEOF. Prior to the fix, handleError did
// not classify this as driver.ErrBadConn, so cn.err was never set, IsValid()
// returned true, and database/sql kept handing out the broken connection. The
// inProgress atomic flag remained stuck at true, causing every subsequent query
// to fail with "there is already a query being processed on this connection".
//
// This test uses a TCP fault-injection proxy between the test client and the
// real PostgreSQL to truncate a single DataRow response and verify that the
// pool recovers.
func TestConnectionPoisoningUnexpectedEOF(t *testing.T) {
	db := pqtest.MustDB(t)

	var pgHost, pgPort string
	for _, kv := range strings.Fields(pqtest.DSN("")) {
		if strings.HasPrefix(kv, "host=") {
			pgHost = strings.TrimPrefix(kv, "host=")
		}
		if strings.HasPrefix(kv, "port=") {
			pgPort = strings.TrimPrefix(kv, "port=")
		}
	}
	if pgHost == "" {
		pgHost = "localhost"
	}
	if pgPort == "" {
		pgPort = "5432"
	}
	pgAddr := net.JoinHostPort(pgHost, pgPort)

	// Verify PostgreSQL is reachable.
	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not reachable at %s: %v", pgAddr, err)
	}
	db.Close()

	proxy := newFaultInjectionProxy(pgAddr, 3)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go proxy.serve(ln)

	proxyHost, proxyPort, _ := net.SplitHostPort(ln.Addr().String())
	proxyDSN := pqtest.DSN(fmt.Sprintf("host=%s port=%s", proxyHost, proxyPort))

	proxyDB, err := sql.Open("postgres", proxyDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer proxyDB.Close()

	proxyDB.SetMaxOpenConns(1)
	proxyDB.SetMaxIdleConns(1)
	proxyDB.SetConnMaxLifetime(time.Hour)
	proxyDB.SetConnMaxIdleTime(time.Hour)

	// Query 1: warmup — establishes the pooled connection.
	if err := queryAndDrain(proxyDB, "SELECT 1"); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// Query 2: large result set — exercises the proxy without fault.
	if err := queryAndDrain(proxyDB, "SELECT generate_series(1, 500)"); err != nil {
		t.Fatalf("pre-fault query: %v", err)
	}

	// Query 3: fault injection — proxy truncates DataRow mid-body.
	// With the fix, database/sql sees driver.ErrBadConn and retries on a new
	// connection, so this succeeds transparently.
	if err := queryAndDrain(proxyDB, "SELECT generate_series(1, 5000)"); err != nil {
		t.Fatalf("fault query should have been retried transparently: %v", err)
	}

	// Queries 4–8: must succeed — pool must not be permanently poisoned.
	for i := 4; i <= 8; i++ {
		if err := queryAndDrain(proxyDB, "SELECT 1"); err != nil {
			if strings.Contains(err.Error(), "already a query") {
				t.Fatalf("query %d: connection pool is permanently poisoned: %v", i, err)
			}
			t.Fatalf("query %d: %v", i, err)
		}
	}
}

func queryAndDrain(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("Query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows.Err: %w", err)
	}
	return nil
}

// faultInjectionProxy is a TCP proxy that transparently forwards PostgreSQL
// wire-protocol traffic, except on the faultAt-th Query ('Q') message, where
// it truncates a DataRow ('D') response mid-body to produce
// io.ErrUnexpectedEOF on the client side.
type faultInjectionProxy struct {
	pgAddr  string
	faultAt int

	queryNum atomic.Int32
}

func newFaultInjectionProxy(pgAddr string, faultAt int) *faultInjectionProxy {
	return &faultInjectionProxy{pgAddr: pgAddr, faultAt: faultAt}
}

func (p *faultInjectionProxy) serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *faultInjectionProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.pgAddr)
	if err != nil {
		client.Close()
		return
	}

	var injectFault atomic.Bool
	done := make(chan struct{})

	go func() {
		defer close(done)
		p.serverToClient(server, client, &injectFault)
	}()

	p.clientToServer(client, server, &injectFault)
	<-done
	client.Close()
	server.Close()
}

func (p *faultInjectionProxy) clientToServer(client, server net.Conn, injectFault *atomic.Bool) {
	// Forward the startup message (length-prefixed, no type byte).
	if !proxyForwardStartup(client, server) {
		return
	}

	for {
		hdr := make([]byte, 5)
		if _, err := io.ReadFull(client, hdr); err != nil {
			return
		}
		bodyLen := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		var body []byte
		if bodyLen > 0 {
			body = make([]byte, bodyLen)
			if _, err := io.ReadFull(client, body); err != nil {
				return
			}
		}

		server.Write(hdr)
		if len(body) > 0 {
			server.Write(body)
		}

		if hdr[0] == 'Q' {
			qn := int(p.queryNum.Add(1))
			if qn == p.faultAt {
				injectFault.Store(true)
			}
		}
		if hdr[0] == 'X' {
			return
		}
	}
}

// serverToClient forwards server responses to the client. When the fault flag
// is active and a DataRow ('D') message arrives, it sends the full 5-byte
// header (declaring how many body bytes to expect) but only writes a fraction
// of the body before closing the connection. This forces io.ReadFull in pq's
// recvMessage to return io.ErrUnexpectedEOF.
func (p *faultInjectionProxy) serverToClient(server, client net.Conn, injectFault *atomic.Bool) {
	for {
		hdr := make([]byte, 5)
		if _, err := io.ReadFull(server, hdr); err != nil {
			return
		}
		msgType := hdr[0]
		bodyLen := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		if bodyLen < 0 {
			bodyLen = 0
		}
		body := make([]byte, bodyLen)
		if bodyLen > 0 {
			if _, err := io.ReadFull(server, body); err != nil {
				return
			}
		}

		if injectFault.Load() && msgType == 'D' && bodyLen > 4 {
			client.Write(hdr)
			cut := bodyLen / 3
			if cut < 2 {
				cut = 2
			}
			client.Write(body[:cut])
			client.Close()
			return
		}

		client.Write(hdr)
		if bodyLen > 0 {
			client.Write(body)
		}
	}
}

func proxyForwardStartup(src, dst net.Conn) bool {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(src, lenBuf); err != nil {
		return false
	}
	bodyLen := int(binary.BigEndian.Uint32(lenBuf)) - 4
	if bodyLen < 0 || bodyLen > 10000 {
		return false
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(src, body); err != nil {
		return false
	}
	dst.Write(lenBuf)
	dst.Write(body)
	return true
}
