// +build go1.8
package pq

import (
	"context"
	"database/sql/driver"
)

// Implement the "ExecerContext" interface
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}
	return cn.exec(ctx, query, list)
}
