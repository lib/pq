package pq

import (
	"database/sql/driver"
	"sync/atomic"
)

type copyin struct {
	cn      *conn
	buffer  []byte
	rowData chan []byte
	done    chan bool

	closed   bool
	err      error
	errorset int32
}

const ciBufferSize = 64 * 1024

func (cn *conn) prepareCopyIn(q string) (_ driver.Stmt, err error) {
	defer errRecover(&err)

	ci := &copyin{
		cn:      cn,
		buffer:  make([]byte, 0, ciBufferSize+1024),
		rowData: make(chan []byte),
		done:    make(chan bool),
	}

	b := newWriteBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'G':
			if r.byte() != 0 {
				errorf("only text format supported for COPY")
			}
			go ci.resploop()
			return ci, err
		case 'Z':
			// done
			return
		case 'E':
			err = parseError(r)
		case 'N', 'S':
			// ignore
		default:
			errorf("unknown response for copy query: %q", t)
		}
	}
	panic("not reached")
}

func (ci *copyin) flush(buf []byte) {
	b := newWriteBuf('d')
	b.bytes(buf)
	ci.cn.send(b)
}

func (ci *copyin) resploop() {
	for {
		t, r := ci.cn.recv1()
		switch t {
		case 'C':
			// complete
		case 'Z':
			ci.done <- true
			return
		case 'E':
			err := parseError(r)
			ci.seterror(err)
		default:
			errorf("unknown response: %q", t)
		}
	}
}

func (ci *copyin) isErrorSet() bool {
	return atomic.LoadInt32(&ci.errorset) != 0
}

func (ci *copyin) seterror(err error) {
	ci.err = err
	atomic.StoreInt32(&ci.errorset, 1)
}

func (ci *copyin) NumInput() int {
	return -1
}

func (ci *copyin) Query(v []driver.Value) (r driver.Rows, err error) {
	return nil, ErrNotSupported
}

// Exec inserts values into the COPY stream. The insert is asynchronous
// and Exec can return errors from previous Exec calls to the same
// COPY stmt.
//
// You need to call Exec without any arguments to sync the COPY stream
// and to get any errors from pending data, since Stmt.Close() doesn't
// return errors to the user.
func (ci *copyin) Exec(v []driver.Value) (r driver.Result, err error) {
	defer errRecover(&err)

	r = result(0)

	if ci.closed {
		panic("already closed")
	}

	if ci.isErrorSet() {
		err = ci.err
		return nil, err
	}

	if len(v) == 0 {
		err = ci.Close()
		ci.closed = true
		return
	}

	numValues := len(v)
	for i, value := range v {
		ci.buffer = append(ci.buffer, encodeText(value)...)
		if i < numValues-1 {
			ci.buffer = append(ci.buffer, '\t')
		}
	}

	ci.buffer = append(ci.buffer, '\n')

	if len(ci.buffer) > ciBufferSize {
		ci.flush(ci.buffer)
		ci.buffer = make([]byte, 0, len(ci.buffer))
	}

	if ci.isErrorSet() {
		err = ci.err
		return nil, err
	}

	return
}

func (ci *copyin) Close() (err error) {
	defer errRecover(&err)

	if ci.closed {
		return nil
	}

	if len(ci.buffer) > 0 {
		ci.flush(ci.buffer)
	}
	b := newWriteBuf('c')
	ci.cn.send(b)

	<-ci.done

	if ci.isErrorSet() {
		err = ci.err
		return err
	}
	return
}
