package pq

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"

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

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}
	if err := st.cn.err.get(); err != nil {
		return err
	}
	defer st.cn.errRecover(&err)

	w := st.cn.writeBuf('C')
	w.byte('S')
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	t, _ := st.cn.recv1()
	if t != '3' {
		st.cn.err.set(driver.ErrBadConn)
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, r := st.cn.recv1()
	if t != 'Z' {
		st.cn.err.set(driver.ErrBadConn)
		errorf("expected ready for query, but got: %q", t)
	}
	st.cn.processReadyForQuery(r)

	return nil
}

func (st *stmt) Query(v []driver.Value) (r driver.Rows, err error) {
	return st.query(toNamedValue(v))
}

func (st *stmt) query(v []driver.NamedValue) (r *rows, err error) {
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	defer st.cn.errRecover(&err)

	st.exec(v)
	return &rows{
		cn:         st.cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

func (st *stmt) Exec(v []driver.Value) (driver.Result, error) {
	return st.ExecContext(context.Background(), toNamedValue(v))
}

func (st *stmt) exec(v []driver.NamedValue) {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START stmt.exec\n")
		defer fmt.Fprintf(os.Stderr, "         END stmt.exec\n")
	}
	if len(v) >= 65536 {
		errorf("got %d parameters but PostgreSQL only supports 65535 parameters", len(v))
	}
	if len(v) != len(st.paramTyps) {
		errorf("got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	cn := st.cn
	w := cn.writeBuf('B')
	w.byte(0) // unnamed portal
	w.string(st.name)

	if cn.binaryParameters {
		cn.sendBinaryParameters(w, v)
	} else {
		w.int16(0)
		w.int16(len(v))
		for i, x := range v {
			if x.Value == nil {
				w.int32(-1)
			} else {
				b := encode(&cn.parameterStatus, x.Value, st.paramTyps[i])
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

	w.next('E')
	w.byte(0)
	w.int32(0)

	w.next('S')
	cn.send(w)

	cn.readBindResponse()
	cn.postExecuteWorkaround()
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}
