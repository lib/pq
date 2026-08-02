package pq

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/lib/pq/internal/proto"
)

var (
	errCopyInClosed               = errors.New("pq: copyin statement has already been closed")
	errBinaryCopyNotSupported     = errors.New("pq: only text format supported for COPY")
	errCopyToNotSupported         = errors.New("pq: COPY TO is not supported")
	errCopyNotSupportedOutsideTxn = errors.New("pq: COPY is only allowed inside a transaction")
	errCopyInProgress             = errors.New("pq: COPY is already in progress on this connection")
)

type copyin struct {
	cn       *conn
	buffer   []byte
	rowData  chan []byte
	done     chan bool
	closed   bool
	doneOnce sync.Once
	mu       struct {
		sync.Mutex
		err error
		driver.Result
	}
}

var _ driver.StmtExecContext = (*copyin)(nil)

const (
	ciBufferSize = 64 * 1024
	// flush buffer before the buffer is filled up and needs reallocation
	ciBufferFlushSize = 63 * 1024
)

func (cn *conn) prepareCopyIn(q string) (_ driver.Stmt, resErr error) {
	if !cn.isInTransaction() {
		return nil, errCopyNotSupportedOutsideTxn
	}

	ci := &copyin{
		cn:      cn,
		buffer:  make([]byte, 0, ciBufferSize),
		rowData: make(chan []byte),
		done:    make(chan bool, 1),
	}
	// add CopyData identifier + 4 bytes for message length
	ci.buffer = append(ci.buffer, byte(proto.CopyDataRequest), 0, 0, 0, 0)

	b := cn.writeBuf(proto.Query)
	b.string(q)
	err := cn.send(b)
	if err != nil {
		return nil, err
	}

awaitCopyInResponse:
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, err
		}
		if resErr != nil && t != proto.ErrorResponse && t != proto.ReadyForQuery {
			cn.err.set(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unexpected %s after COPY error: %w", t, resErr)
		}
		switch t {
		case proto.CopyInResponse:
			if r.byte() != 0 {
				resErr = errBinaryCopyNotSupported
				break awaitCopyInResponse
			}
			columns := r.int16()
			for range columns {
				if r.int16() != 0 {
					resErr = errBinaryCopyNotSupported
					break awaitCopyInResponse
				}
			}
			if err := cn.activateCopy(ci); err != nil {
				cn.err.set(driver.ErrBadConn)
				_ = cn.c.Close()
				return nil, err
			}
			go ci.resploop()
			return ci, nil
		case proto.CopyOutResponse:
			return nil, cn.drainRejectedCopyOut()
		case proto.ErrorResponse:
			if resErr != nil {
				ci.setBad(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected second ErrorResponse after COPY error: %w", resErr)
			}
			resErr = parseError(r, q)
		case proto.ReadyForQuery:
			if resErr == nil {
				ci.setBad(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected ReadyForQuery in response to COPY")
			}
			cn.processReadyForQuery(r)
			return nil, resErr
		default:
			ci.setBad(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unknown response for copy query: %q", t)
		}
	}

	// something went wrong, abort COPY before we return
	b = cn.writeBuf(proto.CopyFail)
	b.string(resErr.Error())
	err = cn.send(b)
	if err != nil {
		return nil, err
	}

	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, err
		}

		switch t {
		case proto.CopyDoneResponse, proto.CommandComplete, proto.ErrorResponse:
		case proto.ReadyForQuery:
			// correctly aborted, we're done
			cn.processReadyForQuery(r)
			return nil, resErr
		default:
			ci.setBad(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unknown response for CopyFail: %q", t)
		}
	}
}

func (cn *conn) drainRejectedCopyOut() error {
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return err
		}
		switch t {
		case proto.CopyDataResponse, proto.CopyDoneResponse, proto.CommandComplete, proto.ErrorResponse:
		case proto.ReadyForQuery:
			cn.processReadyForQuery(r)
			return errCopyToNotSupported
		default:
			cn.err.set(driver.ErrBadConn)
			return fmt.Errorf("pq: unknown response while draining COPY TO: %q", t)
		}
	}
}

func (ci *copyin) flush(buf []byte) error {
	if len(buf)-1 > proto.MaxUint32 {
		return errors.New("pq: too many columns")
	}
	if debugProto {
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", proto.RequestCode(buf[0]), len(buf)-5, buf[5:])
	}
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1)) // Set message length (without message identifier).
	return ci.cn.write(buf)
}

func (ci *copyin) resploop() {
	if err := ci.cn.activateCopy(ci); err != nil {
		ci.setBad(driver.ErrBadConn)
		ci.setError(err)
		ci.finishResponse(false)
		return
	}
	synchronized := false
	defer func() { ci.finishResponse(synchronized) }()

	for {
		t, r, err := ci.cn.recv1()
		if err != nil {
			ci.setBad(driver.ErrBadConn)
			ci.setError(err)
			return
		}
		switch t {
		case proto.CommandComplete:
			// complete
			res, _, err := ci.cn.parseComplete(r.string())
			if err != nil {
				ci.setBad(driver.ErrBadConn)
				ci.setError(err)
				return
			}
			ci.setResult(res)
		case proto.ReadyForQuery:
			ci.cn.processReadyForQuery(r)
			synchronized = true
			return
		case proto.ErrorResponse:
			err := parseError(r, "")
			ci.setError(err)
		default:
			ci.setBad(driver.ErrBadConn)
			ci.setError(fmt.Errorf("unknown response during CopyIn: %q", t))
			return
		}
	}
}

func (ci *copyin) finishResponse(synchronized bool) {
	ci.doneOnce.Do(func() {
		if synchronized {
			if err := ci.drainDeferredStmtCloses(); err != nil {
				// Once a deferred Close has been sent, only a complete
				// CloseComplete/ReadyForQuery exchange can make the protocol
				// reusable. Discard the connection on any interruption.
				ci.setBad(driver.ErrBadConn)
				ci.setError(err)
				_ = ci.cn.c.Close()
				ci.cn.clearActiveCopy(ci)
			}
		} else {
			// A protocol failure before ReadyForQuery makes deferred closes
			// unsafe to send. They are discarded with the bad connection.
			ci.cn.clearActiveCopy(ci)
		}
		if ci.done != nil {
			close(ci.done)
		}
	})
}

// drainDeferredStmtCloses keeps COPY's backend-read ownership until every
// queued named statement has been closed. A single absolute deadline bounds
// all batches, including closes queued while an earlier batch is in flight.
func (ci *copyin) drainDeferredStmtCloses() error {
	var deadline time.Time
	deadlineSet := false
	for {
		names, finished, err := ci.cn.nextDeferredStmtCloseBatch(ci, deadlineSet)
		if err != nil {
			return err
		}
		if finished {
			return nil
		}
		if !deadlineSet {
			deadline = ci.cn.closeDeadline()
			if err := ci.cn.c.SetDeadline(deadline); err != nil {
				return err
			}
			deadlineSet = true
		}
		if err := ci.cn.closePreparedStatements(names); err != nil {
			return err
		}
	}
}

func (ci *copyin) setBad(err error) {
	ci.cn.err.set(err)
}

func (ci *copyin) getBad() error {
	return ci.cn.err.get()
}

func (ci *copyin) err() error {
	ci.mu.Lock()
	err := ci.mu.err
	ci.mu.Unlock()
	return err
}

// setError() sets ci.err if one has not been set already.  Caller must not be
// holding ci.Mutex.
func (ci *copyin) setError(err error) {
	ci.mu.Lock()
	if ci.mu.err == nil {
		ci.mu.err = err
	}
	ci.mu.Unlock()
}

func (ci *copyin) setResult(result driver.Result) {
	ci.mu.Lock()
	ci.mu.Result = result
	ci.mu.Unlock()
}

func (ci *copyin) getResult() driver.Result {
	ci.mu.Lock()
	result := ci.mu.Result
	ci.mu.Unlock()
	if result == nil {
		return driver.RowsAffected(0)
	}
	return result
}

func (ci *copyin) waitForResponse(deadline time.Time) error {
	wait := time.Until(deadline)
	if wait <= 0 {
		ci.setBad(driver.ErrBadConn)
		_ = ci.cn.c.Close()
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ci.done:
		return nil
	case <-timer.C:
		ci.setBad(driver.ErrBadConn)
		_ = ci.cn.c.Close()
		return context.DeadlineExceeded
	}
}

func (ci *copyin) sendCopyFail(reason string) error {
	// Do not use conn.writeBuf: the response goroutine owns conn.scratch.
	b := &writeBuf{
		buf: []byte{byte(proto.CopyFail), 0, 0, 0, 0},
		pos: 1,
	}
	b.string(reason)
	return ci.cn.send(b)
}

// abortForTransaction ends an unfinished COPY without allowing the caller to
// read until resploop has consumed ReadyForQuery. The connection deadline and
// explicit timer bound both network I/O and the completion wait.
func (ci *copyin) abortForTransaction() error {
	ci.closed = true
	if err := ci.getBad(); err != nil {
		return err
	}

	deadline := ci.cn.closeDeadline()
	if err := ci.cn.c.SetDeadline(deadline); err != nil {
		return ci.closeError(err)
	}

	ci.mu.Lock()
	var sendErr error
	if ci.mu.err == nil {
		sendErr = ci.sendCopyFail("pq: COPY aborted by transaction end")
	}
	ci.mu.Unlock()
	if sendErr != nil {
		return ci.closeError(sendErr)
	}
	if err := ci.waitForResponse(deadline); err != nil {
		return ci.closeError(err)
	}
	if err := ci.getBad(); err != nil {
		_ = ci.cn.c.Close()
		return err
	}
	if err := ci.cn.c.SetDeadline(time.Time{}); err != nil {
		return ci.closeError(err)
	}
	return nil
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
func (ci *copyin) Exec(v []driver.Value) (driver.Result, error) {
	return ci.exec(v)
}

// ExecContext inserts values into the COPY stream while allowing a blocked
// network flush to be interrupted by ctx.
func (ci *copyin) ExecContext(ctx context.Context, v []driver.NamedValue) (driver.Result, error) {
	if ctx.Done() == nil {
		values := make([]driver.Value, len(v))
		for i := range v {
			values[i] = v[i].Value
		}
		return ci.exec(values)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finish := ci.cn.watchCancel(ctx, false)
	defer finish.finish()

	values := make([]driver.Value, len(v))
	for i := range v {
		values[i] = v[i].Value
	}
	result, err := ci.exec(values)
	finish.finish()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return result, err
}

func (ci *copyin) exec(v []driver.Value) (driver.Result, error) {
	if ci.closed {
		return nil, errCopyInClosed
	}
	if err := ci.getBad(); err != nil {
		return nil, err
	}
	if len(v) == 0 {
		if err := ci.Close(); err != nil {
			return driver.RowsAffected(0), err
		}
		return ci.getResult(), nil
	}

	// Serialize the asynchronous error check with buffering and any CopyData
	// write. Otherwise resploop can publish an ErrorResponse, consume
	// ReadyForQuery, and begin deferred statement cleanup after this check but
	// before a large row is flushed.
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.mu.err != nil {
		return nil, ci.mu.err
	}

	var (
		numValues = len(v)
		err       error
	)
	for i, value := range v {
		ci.buffer, err = appendEncodedText(ci.buffer, value)
		if err != nil {
			return nil, ci.cn.handleError(err)
		}
		if i < numValues-1 {
			ci.buffer = append(ci.buffer, '\t')
		}
	}

	ci.buffer = append(ci.buffer, '\n')

	if len(ci.buffer) > ciBufferFlushSize {
		err := ci.flush(ci.buffer)
		if err != nil {
			return nil, ci.cn.handleError(err)
		}
		// reset buffer, keep bytes for message identifier and length
		ci.buffer = ci.buffer[:5]
	}

	return driver.RowsAffected(0), nil
}

// CopyData inserts a raw string into the COPY stream. The insert is
// asynchronous and CopyData can return errors from previous CopyData calls to
// the same COPY stmt.
//
// You need to call Exec(nil) to sync the COPY stream and to get any
// errors from pending data, since Stmt.Close() doesn't return errors
// to the user.
func (ci *copyin) CopyData(ctx context.Context, line string) (driver.Result, error) {
	if ctx.Done() == nil {
		return ci.copyData(line)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finish := ci.cn.watchCancel(ctx, false)
	result, err := ci.copyData(line)
	finish.finish()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return result, err
}

func (ci *copyin) copyData(line string) (driver.Result, error) {
	if ci.closed {
		return nil, errCopyInClosed
	}
	if err := ci.getBad(); err != nil {
		return nil, err
	}
	// Keep asynchronous error publication ordered with buffer mutation and a
	// possible CopyData write; see exec.
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.mu.err != nil {
		return nil, ci.mu.err
	}
	ci.buffer = append(ci.buffer, []byte(line)...)
	ci.buffer = append(ci.buffer, '\n')

	if len(ci.buffer) > ciBufferFlushSize {
		err := ci.flush(ci.buffer)
		if err != nil {
			return nil, ci.cn.handleError(err)
		}

		// reset buffer, keep bytes for message identifier and length
		ci.buffer = ci.buffer[:5]
	}

	return driver.RowsAffected(0), nil
}

func (ci *copyin) Close() error {
	if ci.closed { // Don't do anything, we're already closed
		return nil
	}
	ci.closed = true

	if err := ci.getBad(); err != nil {
		return err
	}
	deadline := ci.cn.closeDeadline()
	if err := ci.cn.c.SetDeadline(deadline); err != nil {
		return ci.closeError(err)
	}

	// Serialize the decision to finish COPY with publication of an asynchronous
	// ErrorResponse. Once resploop has published an error, the backend is already
	// ending COPY and sending more CopyData or CopyDone would corrupt the next
	// protocol exchange.
	ci.mu.Lock()
	var sendErr error
	if ci.mu.err == nil {
		if len(ci.buffer) > 0 {
			sendErr = ci.flush(ci.buffer)
		}
		if sendErr == nil {
			// Avoid touching the scratch buffer as resploop could be using it.
			sendErr = ci.cn.sendSimpleMessage(proto.CopyDoneRequest)
		}
	}
	ci.mu.Unlock()
	if sendErr != nil {
		return ci.closeError(sendErr)
	}

	if err := ci.waitForResponse(deadline); err != nil {
		return ci.closeError(err)
	}

	if bad := ci.getBad(); bad != nil {
		_ = ci.cn.c.Close()
		if err := ci.err(); err != nil {
			return err
		}
		return bad
	}
	if err := ci.cn.c.SetDeadline(time.Time{}); err != nil {
		return ci.closeError(err)
	}
	if err := ci.err(); err != nil {
		return err
	}
	return nil
}

func (ci *copyin) closeError(err error) error {
	ci.setBad(driver.ErrBadConn)
	_ = ci.cn.c.Close()
	return ci.cn.handleError(err)
}
