package pq

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type stmt struct {
	cn   *conn
	name string
	rowsHeader
	colFmtData []byte
	paramTyps  []oid.Oid
	closed     bool
}

func (st *stmt) Close() error {
	if st.closed {
		return nil
	}
	if err := st.cn.err.get(); err != nil {
		return err
	}
	if st.cn.deferStmtClose(st.name) {
		// COPY's response goroutine owns backend reads. Let it close the
		// prepared statement after consuming COPY's ReadyForQuery so the
		// protocol exchange cannot overlap, and report success here because
		// database/sql will not retry a driver's Stmt.Close call.
		st.closed = true
		return nil
	}
	if err := st.cn.c.SetDeadline(st.cn.closeDeadline()); err != nil {
		return st.closeError(err)
	}

	if err := st.cn.closePreparedStatements([]string{st.name}); err != nil {
		return st.closeError(err)
	}
	if err := st.cn.c.SetDeadline(time.Time{}); err != nil {
		return st.closeError(err)
	}
	st.closed = true

	return nil
}

// closePreparedStatements sends one extended-protocol synchronization for a
// batch of named statements. It deliberately uses private buffers because it
// can run from COPY's response goroutine while conn.scratch still belongs to a
// backend read.
func (cn *conn) closePreparedStatements(names []string) error {
	for _, name := range names {
		w := &writeBuf{
			buf: []byte{byte(proto.Close), 0, 0, 0, 0},
			pos: 1,
		}
		w.byte(proto.Sync) // 'S' selects a prepared statement, not a portal.
		w.string(name)
		if err := cn.send(w); err != nil {
			return err
		}
	}
	if err := cn.sendSimpleMessage(proto.Sync); err != nil {
		return err
	}

	for range names {
		t, _, err := cn.recv1()
		if err != nil {
			return err
		}
		if t != proto.CloseComplete {
			return fmt.Errorf("pq: unexpected close response: %q", t)
		}
	}

	t, r, err := cn.recv1()
	if err != nil {
		return err
	}
	if t != proto.ReadyForQuery {
		return fmt.Errorf("pq: expected ready for query, but got: %q", t)
	}
	cn.processReadyForQuery(r)
	return nil
}

func (st *stmt) closeError(err error) error {
	st.cn.err.set(driver.ErrBadConn)
	_ = st.cn.c.Close()
	return st.cn.handleError(err)
}

// Implement [driver.StmtQueryContext].
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	if err := st.cn.checkCopyInactive(); err != nil {
		return nil, err
	}
	finish := st.cn.watchCancel(ctx, true)

	err := st.exec(args)
	if err != nil {
		finish()
		return nil, st.cn.handleError(err)
	}

	return &rows{
		cn:         st.cn,
		rowsHeader: st.rowsHeader,
		finish:     finish,
	}, nil
}

// Implement [driver.StmtExecContext].
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	if err := st.cn.checkCopyInactive(); err != nil {
		return nil, err
	}
	defer st.cn.watchCancel(ctx, true)()

	err := st.exec(args)
	if err != nil {
		return nil, st.cn.handleError(err)
	}
	res, _, err := st.cn.readExecuteResponse("simple query")
	return res, st.cn.handleError(err)
}

func (st *stmt) exec(v []driver.NamedValue) error {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START stmt.exec\n")
		defer fmt.Fprintf(os.Stderr, "         END stmt.exec\n")
	}
	if len(v) >= 65536 {
		return fmt.Errorf("pq: got %d parameters but PostgreSQL only supports 65535 parameters", len(v))
	}
	if len(v) != len(st.paramTyps) {
		return fmt.Errorf("pq: got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	cn := st.cn
	w := cn.writeBuf(proto.Bind)
	w.byte(0) // unnamed portal
	w.string(st.name)

	if cn.cfg.BinaryParameters {
		err := cn.sendBinaryParameters(w, v)
		if err != nil {
			return err
		}
	} else {
		w.int16(0)
		w.int16(len(v))
		for i, x := range v {
			if x.Value == nil {
				w.int32(-1)
			} else {
				b, err := encode(x.Value, st.paramTyps[i])
				if err != nil {
					return err
				}
				if b == nil {
					w.int32(-1)
				} else {
					w.int32(len(b))
					w.bytes(b)
				}
			}
		}
	}
	w.bytes(st.colFmtData)

	w.next(proto.Execute)
	w.byte(0)
	w.int32(0)

	w.next(proto.Sync)
	err := cn.send(w)
	if err != nil {
		return err
	}
	err = cn.readBindResponse()
	if err != nil {
		return err
	}
	return cn.postExecuteWorkaround()
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}
