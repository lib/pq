package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

type benchmarkCopyConnector struct{ conn driver.Conn }

func (c benchmarkCopyConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (benchmarkCopyConnector) Driver() driver.Driver { return benchmarkCopyDriver{} }

type benchmarkCopyDriver struct{}

func (benchmarkCopyDriver) Open(string) (driver.Conn, error) {
	return nil, ErrNotSupported
}

type benchmarkCopyConn struct{ stmt driver.Stmt }

func (c *benchmarkCopyConn) Prepare(string) (driver.Stmt, error) { return c.stmt, nil }
func (*benchmarkCopyConn) Close() error                          { return nil }
func (*benchmarkCopyConn) Begin() (driver.Tx, error)             { return nil, ErrNotSupported }

// BenchmarkCopyInDatabaseSQL measures the interface path selected by
// database/sql for a COPY statement. In particular, it captures the per-call
// conversion cost when copyin implements driver.StmtExecContext.
func BenchmarkCopyInDatabaseSQL(b *testing.B) {
	copyStatement := &copyin{
		cn:     &conn{},
		buffer: make([]byte, 5, ciBufferSize),
	}
	db := sql.OpenDB(benchmarkCopyConnector{
		conn: &benchmarkCopyConn{stmt: copyStatement},
	})
	db.SetMaxOpenConns(1)
	stmt, err := db.Prepare("copy benchmark from stdin")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		// Avoid a protocol finish: this benchmark intentionally measures only
		// buffered rows, and its connection is synthetic.
		copyStatement.closed = true
		if err := stmt.Close(); err != nil {
			b.Errorf("close statement: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Errorf("close database: %v", err)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := stmt.Exec(int64(42), "hello world", []byte("payload"), true)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkResultSink = result
		copyStatement.buffer = copyStatement.buffer[:5]
	}
}
