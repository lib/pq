package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"time"

	"github.com/lib/pq/oid"
)

// Implement the "QueryerContext" interface
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}
	finish := cn.watchCancel(ctx)
	r, err := cn.query(query, list)
	if err != nil {
		if finish != nil {
			finish()
		}
		return nil, err
	}
	r.finish = finish
	return r, nil
}

// Implement the "ExecerContext" interface
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}

	if finish := cn.watchCancel(ctx); finish != nil {
		defer finish()
	}

	return cn.Exec(query, list)
}

// Implement the "ConnBeginTx" interface
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var mode string

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
		// Don't touch mode: use the server's default
	case sql.LevelReadUncommitted:
		mode = " ISOLATION LEVEL READ UNCOMMITTED"
	case sql.LevelReadCommitted:
		mode = " ISOLATION LEVEL READ COMMITTED"
	case sql.LevelRepeatableRead:
		mode = " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		mode = " ISOLATION LEVEL SERIALIZABLE"
	default:
		return nil, fmt.Errorf("pq: isolation level not supported: %d", opts.Isolation)
	}

	if opts.ReadOnly {
		mode += " READ ONLY"
	} else {
		mode += " READ WRITE"
	}

	tx, err := cn.begin(mode)
	if err != nil {
		return nil, err
	}
	cn.txnFinish = cn.watchCancel(ctx)
	return tx, nil
}

func (cn *conn) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				_ = cn.cancel()
				finished <- struct{}{}
			case <-finished:
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}

func (cn *conn) cancel() error {
	c, err := dial(cn.dialer, cn.opts)
	if err != nil {
		return err
	}
	defer c.Close()

	{
		can := conn{
			c: c,
		}
		err = can.ssl(cn.opts)
		if err != nil {
			return err
		}

		w := can.writeBuf(0)
		w.int32(80877102) // cancel request code
		w.int32(cn.processID)
		w.int32(cn.secretKey)

		if err := can.sendStartupPacket(w); err != nil {
			return err
		}
	}

	// Read until EOF to ensure that the server received the cancel.
	{
		_, err := io.Copy(ioutil.Discard, c)
		return err
	}
}

func (rs *rows) ColumnTypeScanType(index int) reflect.Type {
	switch rs.colTyps[index] {
	case oid.T_int8:
		return reflect.TypeOf(int64(0))

	case oid.T_int4:
		return reflect.TypeOf(int32(0))

	case oid.T_int2:
		return reflect.TypeOf(int16(0))

	case oid.T_varchar, oid.T_text:
		return reflect.TypeOf("")

	case oid.T_bool:
		return reflect.TypeOf(false)

	case oid.T_numeric:
		return reflect.TypeOf(float64(0))

	case oid.T_date, oid.T_time, oid.T_timetz, oid.T_timestamp, oid.T_timestamptz:
		return reflect.TypeOf(time.Time{})

	case oid.T_bytea:
		return reflect.TypeOf([]byte{})

	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}
