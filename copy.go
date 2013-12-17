package pq

import (
	"database/sql/driver"
	"encoding/binary"
	"sync/atomic"
)

// CopyIn creates COPY FROM statement that can be prepared
// with DB.Prepare().
func CopyIn(table string, columns ...string) string {
	stmt := `COPY "` + table + `" (`
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += `"` + col + `"`
	}
	stmt += `) FROM STDIN`
	return stmt
}

// CopyInSchema creates COPY FROM statement that can be prepared
// with DB.Prepare().
func CopyInSchema(schema, table string, columns ...string) string {
	stmt := `COPY "` + schema + `"."` + table + `" (`
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += `"` + col + `"`
	}
	stmt += `) FROM STDIN`
	return stmt
}

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

// flush buffer before the buffer is filled up and needs reallocation
const ciBufferFlushSize = 63 * 1024

func (cn *conn) prepareCopyIn(q string) (_ driver.Stmt, err error) {
	defer errRecover(&err)

	ci := &copyin{
		cn:      cn,
		buffer:  make([]byte, 0, ciBufferSize),
		rowData: make(chan []byte),
		done:    make(chan bool),
	}
	// add CopyData identifier + 4 bytes for message length
	ci.buffer = append(ci.buffer, 'd', 0, 0, 0, 0)

	b := cn.writeBuf('Q')
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
		case 'H':
			errorf("COPY TO is not supported")
		case 'Z':
			// done
			return
		case 'E':
			err = parseError(r)
		default:
			errorf("unknown response for copy query: %q", t)
		}
	}
	panic("not reached")
}

func (ci *copyin) flush(buf []byte) {
	// set message length (without message identifier)
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))

	_, err := ci.cn.c.Write(buf)
	if err != nil {
		panic(err)
	}
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
// You need to call Exec(nil) to sync the COPY stream and to get any
// errors from pending data, since Stmt.Close() doesn't return errors
// to the user.
func (ci *copyin) Exec(v []driver.Value) (r driver.Result, err error) {
	defer errRecover(&err)

	r = driver.RowsAffected(0)

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
		ci.buffer = appendEncodedText(&ci.cn.parameterStatus, ci.buffer, value)
		if i < numValues-1 {
			ci.buffer = append(ci.buffer, '\t')
		}
	}

	ci.buffer = append(ci.buffer, '\n')

	if len(ci.buffer) > ciBufferFlushSize {
		ci.flush(ci.buffer)
		// reset buffer, keep bytes for message identifier and length
		ci.buffer = ci.buffer[:5]
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
	ci.cn.send(ci.cn.writeBuf('c'))

	<-ci.done

	if ci.isErrorSet() {
		err = ci.err
		return err
	}
	return
}
