package pq

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq/internal/pgpass"
	"github.com/lib/pq/internal/pqsql"
	"github.com/lib/pq/internal/pqutil"
	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
	"github.com/lib/pq/scram"
)

// Common error types
var (
	ErrNotSupported              = errors.New("pq: unsupported command")
	ErrInFailedTransaction       = errors.New("pq: could not complete operation in a failed transaction")
	ErrSSLNotSupported           = errors.New("pq: SSL is not enabled on the server")
	ErrCouldNotDetectUsername    = errors.New("pq: could not detect default username; please provide one explicitly")
	ErrSSLKeyUnknownOwnership    = pqutil.ErrSSLKeyUnknownOwnership
	ErrSSLKeyHasWorldPermissions = pqutil.ErrSSLKeyHasWorldPermissions

	errUnexpectedReady = errors.New("unexpected ReadyForQuery")
	errNoRowsAffected  = errors.New("no RowsAffected available after the empty statement")
	errNoLastInsertID  = errors.New("no LastInsertId available after the empty statement")
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver             = Driver{}
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Execer             = (*conn)(nil) //lint:ignore SA1019 x
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Queryer            = (*conn)(nil) //lint:ignore SA1019 x
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.StmtExecContext    = (*stmt)(nil)
	_ driver.StmtQueryContext   = (*stmt)(nil)
)

func init() {
	sql.Register("postgres", &Driver{})
}

var debugProto = func() bool {
	// Check for exactly "1" (rather than mere existence) so we can add
	// options/flags in the future. I don't know if we ever want that, but it's
	// nice to leave the option open.
	return os.Getenv("PQGO_DEBUG") == "1"
}()

// Driver is the Postgres database driver.
type Driver struct{}

// Open opens a new connection to the database. name is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func (d Driver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

type format int

const (
	formatText   format = 0
	formatBinary format = 1
)

var (
	// One result-column format code with the value 1 (i.e. all binary).
	colFmtDataAllBinary = []byte{0, 1, 0, 1}

	// No result-column format codes (i.e. all text).
	colFmtDataAllText = []byte{0, 0}
)

type transactionStatus byte

const (
	txnStatusIdle                transactionStatus = 'I'
	txnStatusIdleInTransaction   transactionStatus = 'T'
	txnStatusInFailedTransaction transactionStatus = 'E'
)

func (s transactionStatus) String() string {
	switch s {
	case txnStatusIdle:
		return "idle"
	case txnStatusIdleInTransaction:
		return "idle in transaction"
	case txnStatusInFailedTransaction:
		return "in a failed transaction"
	default:
		errorf("unknown transactionStatus %d", s)
	}

	panic("not reached")
}

// Dialer is the dialer interface. It can be used to obtain more control over
// how pq creates network connections.
type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	DialTimeout(network, address string, timeout time.Duration) (net.Conn, error)
}

// DialerContext is the context-aware dialer interface.
type DialerContext interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type defaultDialer struct {
	d net.Dialer
}

func (d defaultDialer) Dial(network, address string) (net.Conn, error) {
	return d.d.Dial(network, address)
}

func (d defaultDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d defaultDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.d.DialContext(ctx, network, address)
}

type conn struct {
	c         net.Conn
	buf       *bufio.Reader
	namei     int
	scratch   [512]byte
	txnStatus transactionStatus
	txnFinish func()

	// Save connection arguments to use during CancelRequest.
	dialer Dialer
	opts   values

	// Cancellation key data for use with CancelRequest messages.
	processID int
	secretKey int

	parameterStatus parameterStatus

	saveMessageType   byte
	saveMessageBuffer []byte

	// If an error is set, this connection is bad and all public-facing
	// functions should return the appropriate error by calling get()
	// (ErrBadConn) or getForNext().
	err syncErr

	// If set, this connection should never use the binary format when
	// receiving query results from prepared statements.  Only provided for
	// debugging.
	disablePreparedBinaryResult bool

	// Whether to always send []byte parameters over as binary.  Enables single
	// round-trip mode for non-prepared Query calls.
	binaryParameters bool

	// If true this connection is in the middle of a COPY
	inCopy bool

	// If not nil, notices will be synchronously sent here
	noticeHandler func(*Error)

	// If not nil, notifications will be synchronously sent here
	notificationHandler func(*Notification)

	// GSSAPI context
	gss GSS
}

type syncErr struct {
	err error
	sync.Mutex
}

// Return ErrBadConn if connection is bad.
func (e *syncErr) get() error {
	e.Lock()
	defer e.Unlock()
	if e.err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// Return the error set on the connection. Currently only used by rows.Next.
func (e *syncErr) getForNext() error {
	e.Lock()
	defer e.Unlock()
	return e.err
}

// Set error, only if it isn't set yet.
func (e *syncErr) set(err error) {
	if err == nil {
		panic("attempt to set nil err")
	}
	e.Lock()
	defer e.Unlock()
	if e.err == nil {
		e.err = err
	}
}

func (cn *conn) writeBuf(b byte) *writeBuf {
	cn.scratch[0] = b
	return &writeBuf{
		buf: cn.scratch[:5],
		pos: 1,
	}
}

// Open opens a new connection to the database. dsn is a connection string. Most
// users should only use it through database/sql package from the standard
// library.
func Open(dsn string) (_ driver.Conn, err error) {
	return DialOpen(defaultDialer{}, dsn)
}

// DialOpen opens a new connection to the database using a dialer.
func DialOpen(d Dialer, dsn string) (_ driver.Conn, err error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	c.Dialer(d)
	return c.open(context.Background())
}

func (c *Connector) open(ctx context.Context) (cn *conn, err error) {
	// Handle any panics during connection initialization. Note that we
	// specifically do *not* want to use errRecover(), as that would turn any
	// connection errors into ErrBadConns, hiding the real error message from
	// the user.
	defer errRecoverNoErrBadConn(&err)

	// Create a new values map (copy). This makes it so maps in different
	// connections do not reference the same underlying data structure, so it
	// is safe for multiple connections to concurrently write to their opts.
	o := make(values)
	for k, v := range c.opts {
		o[k] = v
	}

	cn = &conn{opts: o, dialer: c.dialer}
	if v, ok := o["disable_prepared_binary_result"]; ok {
		cn.disablePreparedBinaryResult, err = pqutil.ParseBool(v)
		if err != nil {
			return nil, err
		}
	}
	if v, ok := o["binary_parameters"]; ok {
		cn.binaryParameters, err = pqutil.ParseBool(v)
		if err != nil {
			return nil, err
		}
	}

	o["password"] = pgpass.PasswordFromPgpass(o)

	cn.c, err = dial(ctx, c.dialer, o)
	if err != nil {
		return nil, err
	}

	err = cn.ssl(o)
	if err != nil {
		if cn.c != nil {
			cn.c.Close()
		}
		return nil, err
	}

	// cn.startup panics on error. Make sure we don't leak cn.c.
	panicking := true
	defer func() {
		if panicking {
			cn.c.Close()
		}
	}()

	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)

	// reset the deadline, in case one was set (see dial)
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		err = cn.c.SetDeadline(time.Time{})
	}
	panicking = false
	return cn, err
}

func dial(ctx context.Context, d Dialer, o values) (net.Conn, error) {
	network, address := network(o)

	// Zero or not specified means wait indefinitely.
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		seconds, err := strconv.ParseInt(timeout, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid value for parameter connect_timeout: %s", err)
		}
		duration := time.Duration(seconds) * time.Second

		// connect_timeout should apply to the entire connection establishment
		// procedure, so we both use a timeout for the TCP connection
		// establishment and set a deadline for doing the initial handshake.
		// The deadline is then reset after startup() is done.
		deadline := time.Now().Add(duration)
		var conn net.Conn
		if dctx, ok := d.(DialerContext); ok {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			conn, err = dctx.DialContext(ctx, network, address)
		} else {
			conn, err = d.DialTimeout(network, address, duration)
		}
		if err != nil {
			return nil, err
		}
		err = conn.SetDeadline(deadline)
		return conn, err
	}
	if dctx, ok := d.(DialerContext); ok {
		return dctx.DialContext(ctx, network, address)
	}
	return d.Dial(network, address)
}

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction ||
		cn.txnStatus == txnStatusInFailedTransaction
}

func (cn *conn) checkIsInTransaction(intxn bool) {
	if cn.isInTransaction() != intxn {
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected transaction status %v", cn.txnStatus)
	}
}

func (cn *conn) Begin() (_ driver.Tx, err error) {
	return cn.begin("")
}

func (cn *conn) begin(mode string) (_ driver.Tx, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	cn.checkIsInTransaction(false)
	_, commandTag, err := cn.simpleExec("BEGIN" + mode)
	if err != nil {
		return nil, err
	}
	if commandTag != "BEGIN" {
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected command tag %s", commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected transaction status %v", cn.txnStatus)
	}
	return cn, nil
}

func (cn *conn) closeTxn() {
	if finish := cn.txnFinish; finish != nil {
		finish()
	}
}

func (cn *conn) Commit() (err error) {
	defer cn.closeTxn()
	if err := cn.err.get(); err != nil {
		return err
	}
	defer cn.errRecover(&err)

	cn.checkIsInTransaction(true)
	// We don't want the client to think that everything is okay if it tries
	// to commit a failed transaction.  However, no matter what we return,
	// database/sql will release this connection back into the free connection
	// pool so we have to abort the current transaction here.  Note that you
	// would get the same behaviour if you issued a COMMIT in a failed
	// transaction, so it's also the least surprising thing to do here.
	if cn.txnStatus == txnStatusInFailedTransaction {
		if err := cn.rollback(); err != nil {
			return err
		}
		return ErrInFailedTransaction
	}

	_, commandTag, err := cn.simpleExec("COMMIT")
	if err != nil {
		if cn.isInTransaction() {
			cn.err.set(driver.ErrBadConn)
		}
		return err
	}
	if commandTag != "COMMIT" {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) Rollback() (err error) {
	defer cn.closeTxn()
	if err := cn.err.get(); err != nil {
		return err
	}
	defer cn.errRecover(&err)
	return cn.rollback()
}

func (cn *conn) rollback() (err error) {
	cn.checkIsInTransaction(true)
	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
		if cn.isInTransaction() {
			cn.err.set(driver.ErrBadConn)
		}
		return err
	}
	if commandTag != "ROLLBACK" {
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(q string) (res driver.Result, commandTag string, err error) {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START conn.simpleExec\n")
		defer fmt.Fprintf(os.Stderr, "         END conn.simpleExec\n")
	}

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			res, commandTag = cn.parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			if res == nil && err == nil {
				err = errUnexpectedReady
			}
			// done
			return
		case 'E':
			err = parseError(r, q)
		case 'I':
			res = emptyRows
		case 'T', 'D':
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown response for simple query: %q", t)
		}
	}
}

func (cn *conn) simpleQuery(q string) (res *rows, err error) {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START conn.simpleQuery\n")
		defer fmt.Fprintf(os.Stderr, "         END conn.simpleQuery\n")
	}
	defer cn.errRecover(&err, q)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C', 'I':
			// We allow queries which don't return any results through Query as
			// well as Exec.  We still have to give database/sql a rows object
			// the user can close, though, to avoid connections from being
			// leaked.  A "rows" with done=true works fine for that purpose.
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected message %q in simple query execution", t)
			}
			if res == nil {
				res = &rows{
					cn: cn,
				}
			}
			// Set the result and tag to the last command complete if there wasn't a
			// query already run. Although queries usually return from here and cede
			// control to Next, a query with zero results does not.
			if t == 'C' {
				res.result, res.tag = cn.parseComplete(r.string())
				if res.colNames != nil {
					return
				}
			}
			res.done = true
		case 'Z':
			cn.processReadyForQuery(r)
			// done
			return
		case 'E':
			res = nil
			err = parseError(r, q)
		case 'D':
			if res == nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected DataRow in simple query execution")
			}
			// the query didn't fail; kick off to Next
			cn.saveMessage(t, r)
			return
		case 'T':
			// res might be non-nil here if we received a previous
			// CommandComplete, but that's fine; just overwrite it
			res = &rows{cn: cn}
			res.rowsHeader = parsePortalRowDescribe(r)

			// To work around a bug in QueryRow in Go 1.2 and earlier, wait
			// until the first DataRow has been received.
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown response for simple query: %q", t)
		}
	}
}

// Decides which column formats to use for a prepared statement.  The input is
// an array of type oids, one element per result column.
func decideColumnFormats(colTyps []fieldDesc, forceText bool) (colFmts []format, colFmtData []byte) {
	if len(colTyps) == 0 {
		return nil, colFmtDataAllText
	}

	colFmts = make([]format, len(colTyps))
	if forceText {
		return colFmts, colFmtDataAllText
	}

	allBinary := true
	allText := true
	for i, t := range colTyps {
		switch t.OID {
		// This is the list of types to use binary mode for when receiving them
		// through a prepared statement.  If a type appears in this list, it
		// must also be implemented in binaryDecode in encode.go.
		case oid.T_bytea:
			fallthrough
		case oid.T_int8:
			fallthrough
		case oid.T_int4:
			fallthrough
		case oid.T_int2:
			fallthrough
		case oid.T_uuid:
			colFmts[i] = formatBinary
			allText = false

		default:
			allBinary = false
		}
	}

	if allBinary {
		return colFmts, colFmtDataAllBinary
	} else if allText {
		return colFmts, colFmtDataAllText
	} else {
		colFmtData = make([]byte, 2+len(colFmts)*2)
		if len(colFmts) > math.MaxUint16 {
			errorf("too many columns (%d > math.MaxUint16)", len(colFmts))
		}
		binary.BigEndian.PutUint16(colFmtData, uint16(len(colFmts)))
		for i, v := range colFmts {
			binary.BigEndian.PutUint16(colFmtData[2+i*2:], uint16(v))
		}
		return colFmts, colFmtData
	}
}

func (cn *conn) prepareTo(q, stmtName string) *stmt {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START conn.prepareTo\n")
		defer fmt.Fprintf(os.Stderr, "         END conn.prepareTo\n")
	}

	st := &stmt{cn: cn, name: stmtName}

	b := cn.writeBuf('P')
	b.string(st.name)
	b.string(q)
	b.int16(0)

	b.next('D')
	b.byte('S')
	b.string(st.name)

	b.next('S')
	cn.send(b)

	cn.readParseResponse()
	st.paramTyps, st.colNames, st.colTyps = cn.readStatementDescribeResponse()
	st.colFmts, st.colFmtData = decideColumnFormats(st.colTyps, cn.disablePreparedBinaryResult)
	cn.readReadyForQuery()
	return st
}

func (cn *conn) Prepare(q string) (_ driver.Stmt, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err, q)

	if pqsql.StartsWithCopy(q) {
		s, err := cn.prepareCopyIn(q)
		if err == nil {
			cn.inCopy = true
		}
		return s, err
	}
	return cn.prepareTo(q, cn.gname()), nil
}

func (cn *conn) Close() (err error) {
	// Skip cn.bad return here because we always want to close a connection.
	defer cn.errRecover(&err)

	// Ensure that cn.c.Close is always run. Since error handling is done with
	// panics and cn.errRecover, the Close must be in a defer.
	defer func() {
		cerr := cn.c.Close()
		if err == nil {
			err = cerr
		}
	}()

	// Don't go through send(); ListenerConn relies on us not scribbling on the
	// scratch buffer of this connection.
	return cn.sendSimpleMessage('X')
}

func toNamedValue(v []driver.Value) []driver.NamedValue {
	v2 := make([]driver.NamedValue, len(v))
	for i := range v {
		v2[i] = driver.NamedValue{Ordinal: i + 1, Value: v[i]}
	}
	return v2
}

// CheckNamedValue implements [driver.NamedValueChecker].
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(driver.Valuer); ok {
		// Ignore Valuer, for backward compatibility with pq.Array().
		return driver.ErrSkip
	}

	// Ignoring []byte / []uint8.
	if _, ok := nv.Value.([]uint8); ok {
		return driver.ErrSkip
	}

	v := reflect.ValueOf(nv.Value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice {
		var err error
		nv.Value, err = Array(v.Interface()).Value()
		return err
	}

	return driver.ErrSkip
}

// Implement the "Queryer" interface
func (cn *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return cn.query(query, toNamedValue(args))
}

func (cn *conn) query(query string, args []driver.NamedValue) (_ *rows, err error) {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START conn.query\n")
		defer fmt.Fprintf(os.Stderr, "         END conn.query\n")
	}
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	if cn.inCopy {
		return nil, errCopyInProgress
	}
	defer cn.errRecover(&err, query)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	if cn.binaryParameters {
		cn.sendBinaryModeQuery(query, args)

		cn.readParseResponse()
		cn.readBindResponse()
		rows := &rows{cn: cn}
		rows.rowsHeader = cn.readPortalDescribeResponse()
		cn.postExecuteWorkaround()
		return rows, nil
	}
	st := cn.prepareTo(query, "")
	st.exec(args)
	return &rows{
		cn:         cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (res driver.Result, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err, query)

	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		return r, err
	}

	if cn.binaryParameters {
		cn.sendBinaryModeQuery(query, toNamedValue(args))

		cn.readParseResponse()
		cn.readBindResponse()
		cn.readPortalDescribeResponse()
		cn.postExecuteWorkaround()
		res, _, err = cn.readExecuteResponse("Execute")
		return res, err
	}
	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st := cn.prepareTo(query, "")
	r, err := st.Exec(args)
	if err != nil {
		panic(err)
	}
	return r, err
}

type safeRetryError struct {
	Err error
}

func (se *safeRetryError) Error() string {
	return se.Err.Error()
}

func (cn *conn) send(m *writeBuf) {
	if debugProto {
		w := m.wrap()
		for len(w) > 0 { // Can contain multiple messages.
			c := proto.RequestCode(w[0])
			l := int(binary.BigEndian.Uint32(w[1:5])) - 4
			fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", c, l, w[5:l+5])
			w = w[l+5:]
		}
	}

	n, err := cn.c.Write(m.wrap())
	if err != nil {
		if n == 0 {
			err = &safeRetryError{Err: err}
		}
		panic(err)
	}
}

func (cn *conn) sendStartupPacket(m *writeBuf) error {
	if debugProto {
		w := m.wrap()
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n",
			"Startup",
			int(binary.BigEndian.Uint32(w[1:5]))-4,
			w[5:])
	}
	_, err := cn.c.Write((m.wrap())[1:])
	return err
}

// Send a message of type typ to the server on the other end of cn.  The
// message should have no payload.  This method does not use the scratch
// buffer.
func (cn *conn) sendSimpleMessage(typ byte) (err error) {
	if debugProto {
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n",
			proto.RequestCode(typ), 0, []byte{})
	}
	_, err = cn.c.Write([]byte{typ, '\x00', '\x00', '\x00', '\x04'})
	return err
}

// saveMessage memorizes a message and its buffer in the conn struct.
// recvMessage will then return these values on the next call to it.  This
// method is useful in cases where you have to see what the next message is
// going to be (e.g. to see whether it's an error or not) but you can't handle
// the message yourself.
func (cn *conn) saveMessage(typ byte, buf *readBuf) {
	if cn.saveMessageType != 0 {
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected saveMessageType %d", cn.saveMessageType)
	}
	cn.saveMessageType = typ
	cn.saveMessageBuffer = *buf
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage(r *readBuf) (byte, error) {
	// workaround for a QueryRow bug, see exec
	if cn.saveMessageType != 0 {
		t := cn.saveMessageType
		*r = cn.saveMessageBuffer
		cn.saveMessageType = 0
		cn.saveMessageBuffer = nil
		return t, nil
	}

	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		return 0, err
	}

	// read the type and length of the message that follows
	t := x[0]
	n := int(binary.BigEndian.Uint32(x[1:])) - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		return 0, err
	}
	*r = y
	if debugProto {
		fmt.Fprintf(os.Stderr, "SERVER ← %-20s %5d  %q\n",
			proto.ResponseCode(t), n, y)
	}
	return t, nil
}

// recv receives a message from the backend, but if an error happened while
// reading the message or the received message was an ErrorResponse, it panics.
// NoticeResponses are ignored.  This function should generally be used only
// during the startup sequence.
func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		var err error
		r = &readBuf{}
		t, err = cn.recvMessage(r)
		if err != nil {
			panic(err)
		}
		switch t {
		case 'E':
			panic(parseError(r, ""))
		case 'N':
			if n := cn.noticeHandler; n != nil {
				n(parseError(r, ""))
			}
		case 'A':
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		default:
			return
		}
	}
}

// recv1Buf is exactly equivalent to recv1, except it uses a buffer supplied by
// the caller to avoid an allocation.
func (cn *conn) recv1Buf(r *readBuf) byte {
	for {
		t, err := cn.recvMessage(r)
		if err != nil {
			panic(err)
		}

		switch t {
		case 'A':
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		case 'N':
			if n := cn.noticeHandler; n != nil {
				n(parseError(r, ""))
			}
		case 'S':
			cn.processParameterStatus(r)
		default:
			return t
		}
	}
}

// recv1 receives a message from the backend, panicking if an error occurs
// while attempting to read it.  All asynchronous messages are ignored, with
// the exception of ErrorResponse.
func (cn *conn) recv1() (t byte, r *readBuf) {
	r = &readBuf{}
	t = cn.recv1Buf(r)
	return t, r
}

func (cn *conn) ssl(o values) error {
	upgrade, err := ssl(o)
	if err != nil {
		return err
	}

	if upgrade == nil {
		// Nothing to do
		return nil
	}

	// only negotiate the ssl handshake if requested (which is the default).
	// sllnegotiation=direct is supported by pg17 and above.
	if sslnegotiation(o) {
		w := cn.writeBuf(0)
		w.int32(80877103)
		if err = cn.sendStartupPacket(w); err != nil {
			return err
		}

		b := cn.scratch[:1]
		_, err = io.ReadFull(cn.c, b)
		if err != nil {
			return err
		}

		if b[0] != 'S' {
			return ErrSSLNotSupported
		}
	}

	cn.c, err = upgrade(cn.c)
	return err
}

// isDriverSetting returns true iff a setting is purely for configuring the
// driver's options and should not be sent to the server in the connection
// startup packet.
func isDriverSetting(key string) bool {
	switch key {
	case "host", "port":
		return true
	case "password":
		return true
	case "sslmode", "sslcert", "sslkey", "sslrootcert", "sslinline", "sslsni":
		return true
	case "fallback_application_name":
		return true
	case "connect_timeout":
		return true
	case "disable_prepared_binary_result":
		return true
	case "binary_parameters":
		return true
	case "krbsrvname":
		return true
	case "krbspn":
		return true
	default:
		return false
	}
}

func (cn *conn) startup(o values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	// Send the backend the name of the database we want to connect to, and the
	// user we want to connect as.  Additionally, we send over any run-time
	// parameters potentially included in the connection string.  If the server
	// doesn't recognize any of them, it will reply with an error.
	for k, v := range o {
		if isDriverSetting(k) {
			// skip options which can't be run-time parameters
			continue
		}
		// The protocol requires us to supply the database name as "database"
		// instead of "dbname".
		if k == "dbname" {
			k = "database"
		}
		w.string(k)
		w.string(v)
	}
	w.string("")
	if err := cn.sendStartupPacket(w); err != nil {
		panic(err)
	}

	for {
		switch t, r := cn.recv(); proto.ResponseCode(t) {
		case proto.BackendKeyData:
			cn.processBackendKeyData(r)
		case proto.ParameterStatus:
			cn.processParameterStatus(r)
		case proto.AuthenticationRequest:
			err := cn.auth(r, o)
			if err != nil {
				panic(err)
			}
		case proto.ReadyForQuery:
			cn.processReadyForQuery(r)
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o values) error {
	switch code := proto.AuthCode(r.int32()); code {
	default:
		return fmt.Errorf("pq: unknown authentication response: %s", code)
	case proto.AuthReqKrb4, proto.AuthReqKrb5, proto.AuthReqCrypt, proto.AuthReqSSPI:
		return fmt.Errorf("pq: unsupported authentication method: %s", code)

	case proto.AuthReqOk:
		return nil

	case proto.AuthReqPassword:
		w := cn.writeBuf(byte(proto.PasswordMessage))
		w.string(o["password"])
		cn.send(w)

		t, r := cn.recv()
		if t != byte(proto.AuthenticationRequest) {
			return fmt.Errorf("pq: unexpected password response: %q", t)
		}
		if r.int32() != int(proto.AuthReqOk) {
			return fmt.Errorf("pq: unexpected authentication response: %q", t)
		}
		return nil

	case proto.AuthReqMD5:
		s := string(r.next(4))
		w := cn.writeBuf(byte(proto.PasswordMessage))
		w.string("md5" + md5s(md5s(o["password"]+o["user"])+s))
		cn.send(w)

		t, r := cn.recv()
		if t != byte(proto.AuthenticationRequest) {
			return fmt.Errorf("pq: unexpected password response: %q", t)
		}
		if r.int32() != int(proto.AuthReqOk) {
			return fmt.Errorf("pq: unexpected authentication response: %q", t)
		}
		return nil

	case proto.AuthReqGSS: // GSSAPI, startup
		if newGss == nil {
			return fmt.Errorf("pq: kerberos error: no GSSAPI provider registered (import github.com/lib/pq/auth/kerberos)")
		}
		cli, err := newGss()
		if err != nil {
			return fmt.Errorf("pq: kerberos error: %w", err)
		}

		var token []byte
		if spn, ok := o["krbspn"]; ok {
			// Use the supplied SPN if provided..
			token, err = cli.GetInitTokenFromSpn(spn)
		} else {
			// Allow the kerberos service name to be overridden
			service := "postgres"
			if val, ok := o["krbsrvname"]; ok {
				service = val
			}
			token, err = cli.GetInitToken(o["host"], service)
		}

		if err != nil {
			return fmt.Errorf("pq: failed to get Kerberos ticket: %w", err)
		}

		w := cn.writeBuf(byte(proto.GSSResponse))
		w.bytes(token)
		cn.send(w)

		// Store for GSSAPI continue message
		cn.gss = cli
		return nil

	case proto.AuthReqGSSCont: // GSSAPI continue
		if cn.gss == nil {
			return errors.New("pq: GSSAPI protocol error")
		}

		done, tokOut, err := cn.gss.Continue([]byte(*r))
		if err == nil && !done {
			w := cn.writeBuf(byte(proto.SASLInitialResponse))
			w.bytes(tokOut)
			cn.send(w)
		}

		// Errors fall through and read the more detailed message from the
		// server.
		return nil

	case proto.AuthReqSASL:
		sc := scram.NewClient(sha256.New, o["user"], o["password"])
		sc.Step(nil)
		if sc.Err() != nil {
			return fmt.Errorf("pq: SCRAM-SHA-256 error: %w", sc.Err())
		}
		scOut := sc.Out()

		w := cn.writeBuf(byte(proto.SASLResponse))
		w.string("SCRAM-SHA-256")
		w.int32(len(scOut))
		w.bytes(scOut)
		cn.send(w)

		t, r := cn.recv()
		if t != byte(proto.AuthenticationRequest) {
			return fmt.Errorf("pq: unexpected password response: %q", t)
		}

		if r.int32() != int(proto.AuthReqSASLCont) {
			return fmt.Errorf("pq: unexpected authentication response: %q", t)
		}

		nextStep := r.next(len(*r))
		sc.Step(nextStep)
		if sc.Err() != nil {
			return fmt.Errorf("pq: SCRAM-SHA-256 error: %w", sc.Err())
		}

		scOut = sc.Out()
		w = cn.writeBuf(byte(proto.SASLResponse))
		w.bytes(scOut)
		cn.send(w)

		t, r = cn.recv()
		if t != byte(proto.AuthenticationRequest) {
			return fmt.Errorf("pq: unexpected password response: %q", t)
		}

		if r.int32() != int(proto.AuthReqSASLFin) {
			return fmt.Errorf("pq: unexpected authentication response: %q", t)
		}

		nextStep = r.next(len(*r))
		sc.Step(nextStep)
		if sc.Err() != nil {
			return fmt.Errorf("pq: SCRAM-SHA-256 error: %w", sc.Err())
		}

		return nil
	}
}

// parseComplete parses the "command tag" from a CommandComplete message, and
// returns the number of rows affected (if applicable) and a string identifying
// only the command that was executed, e.g. "ALTER TABLE".  If the command tag
// could not be parsed, parseComplete panics.
func (cn *conn) parseComplete(commandTag string) (driver.Result, string) {
	commandsWithAffectedRows := []string{
		"SELECT ",
		// INSERT is handled below
		"UPDATE ",
		"DELETE ",
		"FETCH ",
		"MOVE ",
		"COPY ",
	}

	var affectedRows *string
	for _, tag := range commandsWithAffectedRows {
		if strings.HasPrefix(commandTag, tag) {
			t := commandTag[len(tag):]
			affectedRows = &t
			commandTag = tag[:len(tag)-1]
			break
		}
	}
	// INSERT also includes the oid of the inserted row in its command tag. Oids
	// in user tables are deprecated, and the oid is only returned when exactly
	// one row is inserted, so it's unlikely to be of value to any real-world
	// application and we can ignore it.
	if affectedRows == nil && strings.HasPrefix(commandTag, "INSERT ") {
		parts := strings.Split(commandTag, " ")
		if len(parts) != 3 {
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected INSERT command tag %s", commandTag)
		}
		affectedRows = &parts[len(parts)-1]
		commandTag = "INSERT"
	}
	// There should be no affected rows attached to the tag, just return it
	if affectedRows == nil {
		return driver.RowsAffected(0), commandTag
	}
	n, err := strconv.ParseInt(*affectedRows, 10, 64)
	if err != nil {
		cn.err.set(driver.ErrBadConn)
		errorf("could not parse commandTag: %s", err)
	}
	return driver.RowsAffected(n), commandTag
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (cn *conn) sendBinaryParameters(b *writeBuf, args []driver.NamedValue) {
	// Do one pass over the parameters to see if we're going to send any of them
	// over in binary.  If we are, create a paramFormats array at the same time.
	var paramFormats []int
	for i, x := range args {
		_, ok := x.Value.([]byte)
		if ok {
			if paramFormats == nil {
				paramFormats = make([]int, len(args))
			}
			paramFormats[i] = 1
		}
	}
	if paramFormats == nil {
		b.int16(0)
	} else {
		b.int16(len(paramFormats))
		for _, x := range paramFormats {
			b.int16(x)
		}
	}

	b.int16(len(args))
	for _, x := range args {
		if x.Value == nil {
			b.int32(-1)
		} else if xx, ok := x.Value.([]byte); ok && xx == nil {
			b.int32(-1)
		} else {
			datum := binaryEncode(&cn.parameterStatus, x.Value)
			b.int32(len(datum))
			b.bytes(datum)
		}
	}
}

func (cn *conn) sendBinaryModeQuery(query string, args []driver.NamedValue) {
	if len(args) >= 65536 {
		errorf("got %d parameters but PostgreSQL only supports 65535 parameters", len(args))
	}

	b := cn.writeBuf('P')
	b.byte(0) // unnamed statement
	b.string(query)
	b.int16(0)

	b.next('B')
	b.int16(0) // unnamed portal and statement
	cn.sendBinaryParameters(b, args)
	b.bytes(colFmtDataAllText)

	b.next('D')
	b.byte('P')
	b.byte(0) // unnamed portal

	b.next('E')
	b.byte(0)
	b.int32(0)

	b.next('S')
	cn.send(b)
}

func (cn *conn) processParameterStatus(r *readBuf) {
	var err error

	param := r.string()
	switch param {
	case "server_version":
		var major1 int
		var major2 int
		_, err = fmt.Sscanf(r.string(), "%d.%d", &major1, &major2)
		if err == nil {
			cn.parameterStatus.serverVersion = major1*10000 + major2*100
		}

	case "TimeZone":
		cn.parameterStatus.currentLocation, err = time.LoadLocation(r.string())
		if err != nil {
			cn.parameterStatus.currentLocation = nil
		}

	default:
		// ignore
	}
}

func (cn *conn) processReadyForQuery(r *readBuf) {
	cn.txnStatus = transactionStatus(r.byte())
}

func (cn *conn) readReadyForQuery() {
	t, r := cn.recv1()
	switch t {
	case 'Z':
		cn.processReadyForQuery(r)
		return
	case 'E':
		err := parseError(r, "")
		cn.err.set(driver.ErrBadConn)
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected message %q; expected ReadyForQuery", t)
	}
}

func (cn *conn) processBackendKeyData(r *readBuf) {
	cn.processID = r.int32()
	cn.secretKey = r.int32()
}

func (cn *conn) readParseResponse() {
	t, r := cn.recv1()
	switch t {
	case '1':
		return
	case 'E':
		err := parseError(r, "")
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Parse response %q", t)
	}
}

func (cn *conn) readStatementDescribeResponse() (paramTyps []oid.Oid, colNames []string, colTyps []fieldDesc) {
	for {
		t, r := cn.recv1()
		switch t {
		case 't':
			nparams := r.int16()
			paramTyps = make([]oid.Oid, nparams)
			for i := range paramTyps {
				paramTyps[i] = r.oid()
			}
		case 'n':
			return paramTyps, nil, nil
		case 'T':
			colNames, colTyps = parseStatementRowDescribe(r)
			return paramTyps, colNames, colTyps
		case 'E':
			err := parseError(r, "")
			cn.readReadyForQuery()
			panic(err)
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected Describe statement response %q", t)
		}
	}
}

func (cn *conn) readPortalDescribeResponse() rowsHeader {
	t, r := cn.recv1()
	switch t {
	case 'T':
		return parsePortalRowDescribe(r)
	case 'n':
		return rowsHeader{}
	case 'E':
		err := parseError(r, "")
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Describe response %q", t)
	}
	panic("not reached")
}

func (cn *conn) readBindResponse() {
	t, r := cn.recv1()
	switch t {
	case '2':
		return
	case 'E':
		err := parseError(r, "")
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Bind response %q", t)
	}
}

func (cn *conn) postExecuteWorkaround() {
	// Work around a bug in sql.DB.QueryRow: in Go 1.2 and earlier it ignores
	// any errors from rows.Next, which masks errors that happened during the
	// execution of the query.  To avoid the problem in common cases, we wait
	// here for one more message from the database.  If it's not an error the
	// query will likely succeed (or perhaps has already, if it's a
	// CommandComplete), so we push the message into the conn struct; recv1
	// will return it as the next message for rows.Next or rows.Close.
	// However, if it's an error, we wait until ReadyForQuery and then return
	// the error to our caller.
	for {
		t, r := cn.recv1()
		switch t {
		case 'E':
			err := parseError(r, "")
			cn.readReadyForQuery()
			panic(err)
		case 'C', 'D', 'I':
			// the query didn't fail, but we can't process this message
			cn.saveMessage(t, r)
			return
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected message during extended query execution: %q", t)
		}
	}
}

// Only for Exec(), since we ignore the returned data
func (cn *conn) readExecuteResponse(protocolState string) (res driver.Result, commandTag string, err error) {
	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected CommandComplete after error %s", err)
			}
			res, commandTag = cn.parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			if res == nil && err == nil {
				err = errUnexpectedReady
			}
			return res, commandTag, err
		case 'E':
			err = parseError(r, "")
		case 'T', 'D', 'I':
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected %q after error %s", t, err)
			}
			if t == 'I' {
				res = emptyRows
			}
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown %s response: %q", protocolState, t)
		}
	}
}

func parseStatementRowDescribe(r *readBuf) (colNames []string, colTyps []fieldDesc) {
	n := r.int16()
	colNames = make([]string, n)
	colTyps = make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		// format code not known when describing a statement; always 0
		r.next(2)
	}
	return
}

func parsePortalRowDescribe(r *readBuf) rowsHeader {
	n := r.int16()
	colNames := make([]string, n)
	colFmts := make([]format, n)
	colTyps := make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		colFmts[i] = format(r.int16())
	}
	return rowsHeader{
		colNames: colNames,
		colFmts:  colFmts,
		colTyps:  colTyps,
	}
}

func (cn *conn) ResetSession(ctx context.Context) error {
	// Ensure bad connections are reported: From database/sql/driver:
	// If a connection is never returned to the connection pool but immediately reused, then
	// ResetSession is called prior to reuse but IsValid is not called.
	return cn.err.get()
}

func (cn *conn) IsValid() bool {
	return cn.err.get() == nil
}
