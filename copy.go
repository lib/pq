package pq

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"sync/atomic"
)

var (
	errCopyInClosed = errors.New("copyin statement has already been closed")
	errBinaryCopyNotSupported = errors.New("only text format supported for COPY")
	errCopyToNotSupported = errors.New("COPY TO is not supported")
	errCopyNotSupportedOutsideTxn = errors.New("COPY is only allowed inside a transaction")
)

// CopyIn creates a COPY FROM statement which can be prepared with
// Tx.Prepare().  The target table should be visible in search_path.
func CopyIn(table string, columns ...string) string {
	stmt := "COPY " + quoteIdentifier(table) + " ("
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += quoteIdentifier(col)
	}
	stmt += ") FROM STDIN"
	return stmt
}

// CopyInSchema creates a COPY FROM statement which can be prepared with
// Tx.Prepare().
func CopyInSchema(schema, table string, columns ...string) string {
	stmt := "COPY " + quoteIdentifier(schema) + "." + quoteIdentifier(table) + " ("
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += quoteIdentifier(col)
	}
	stmt += ") FROM STDIN"
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

	if !cn.isInTransaction() {
		return nil, errCopyNotSupportedOutsideTxn
	}

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
				err = errBinaryCopyNotSupported
				goto abortCopy
			}
			go ci.resploop()
			return ci, nil
		case 'H':
			err = errCopyToNotSupported
			goto abortCopy
		case 'E':
			err = parseError(r)
		case 'Z':
			if err == nil {
				errorf("unexpected ReadyForQuery in response to COPY")
			}
			cn.processReadyForQuery(r)
			return nil, err
		default:
			errorf("unknown response for copy query: %q", t)
		}
	}

abortCopy:
	b = cn.writeBuf('f')
	b.string(err.Error())
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'c', 'C', 'E':
		case 'Z':
			cn.processReadyForQuery(r)
			goto abortDone
		default:
			errorf("unknown response for CopyFail: %q", t)
		}
	}

abortDone:
	return nil, err
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
			ci.cn.processReadyForQuery(r)
			ci.done <- true
			return
		case 'E':
			err := parseError(r)
			ci.setError(err)
		default:
			errorf("unknown response: %q", t)
		}
	}
}

func (ci *copyin) isErrorSet() bool {
	return atomic.LoadInt32(&ci.errorset) != 0
}

func (ci *copyin) setError(err error) {
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

	if ci.closed {
		return nil, errCopyInClosed
	}

	if ci.isErrorSet() {
		return nil, ci.err
	}

	if len(v) == 0 {
		err = ci.Close()
		ci.closed = true
		return nil, err
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

	return driver.RowsAffected(0), nil
}

func (ci *copyin) Close() (err error) {
	defer errRecover(&err)

	if ci.closed {
		return errCopyInClosed
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
	return nil
}
