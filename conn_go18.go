// +build go1.8

package pq

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Implement the "QueryerContext" interface
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}
	return cn.query(ctx, query, list)
}

// Implement the "ExecerContext" interface
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}
	return cn.exec(ctx, query, list)
}

// Implement the "ConnBeginContext" interface
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels not supported")
	}
	if opts.ReadOnly {
		return nil, errors.New("read-only transactions not supported")
	}
	return cn.begin(ctx)
}
