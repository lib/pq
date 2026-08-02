package pq

import (
	"bufio"
	"bytes"
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

const defaultNetworkCloseTimeout = 5 * time.Second

// Allow a responsive backend to report QueryCanceled and leave prepared
// statement connections reusable. If no response arrives promptly, close the
// primary connection so context cancellation cannot hang indefinitely.
const cancelResponseGracePeriod = 100 * time.Millisecond

const cancelRequestTimeout = 10 * time.Second

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver                         = Driver{}
	_ driver.DriverContext                  = (*Driver)(nil)
	_ driver.Connector                      = (*driverConnector)(nil)
	_ driver.Connector                      = (*Connector)(nil)
	_ driver.Conn                           = (*conn)(nil)
	_ driver.ConnBeginTx                    = (*conn)(nil)
	_ driver.ConnPrepareContext             = (*conn)(nil)
	_ driver.ExecerContext                  = (*conn)(nil)
	_ driver.NamedValueChecker              = (*conn)(nil)
	_ driver.Pinger                         = (*conn)(nil)
	_ driver.QueryerContext                 = (*conn)(nil)
	_ driver.SessionResetter                = (*conn)(nil)
	_ driver.Validator                      = (*conn)(nil)
	_ driver.Stmt                           = (*stmt)(nil)
	_ driver.StmtExecContext                = (*stmt)(nil)
	_ driver.StmtQueryContext               = (*stmt)(nil)
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
	//_ driver.RowsColumnTypeNullable         = (*rows)(nil) // TODO
	_ driver.RowsColumnTypePrecisionScale = (*rows)(nil)
	_ driver.RowsColumnTypeScanType       = (*rows)(nil)
	_ driver.RowsNextResultSet            = (*rows)(nil)
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

// OpenConnector returns a connector whose Connect method receives the context
// supplied by database/sql. This lets cancellation interrupt startup as well
// as queries on established connections.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &driverConnector{driver: d, name: name}, nil
}

// driverConnector preserves Driver.Open's historical behavior of deferring
// data source parsing until each physical connection attempt rather than doing
// it during sql.Open. Rebuilding the Connector also preserves the historical
// behavior of re-reading environment and service-file configuration.
type driverConnector struct {
	driver *Driver
	name   string
}

func (c *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connector, err := NewConnector(c.name)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (c *driverConnector) Driver() driver.Driver {
	return c.driver
}

// Parameters sent by PostgreSQL on startup.
type parameterStatus struct {
	serverVersion                            int
	currentLocation                          *time.Location
	inHotStandby, defaultTransactionReadOnly sql.NullBool
	isRedshift                               bool
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
		panic(fmt.Sprintf("pq: unknown transactionStatus %d", s))
	}
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
	c            net.Conn
	buf          *bufio.Reader
	closeTimeout time.Duration
	startupPhase bool
	namei        int
	scratch      [512]byte
	txnStatus    transactionStatus
	txnFinish    func()

	// Save connection arguments to use during CancelRequest.
	dialer          Dialer
	cfg             Config
	parameterStatus parameterStatus

	saveMessageType   proto.ResponseCode
	saveMessageBuffer []byte

	// If an error is set this connection is bad and all public-facing
	// functions should return the appropriate error by calling get()
	// (ErrBadConn) or getForNext().
	err syncErr

	secretKey           []byte              // Cancellation key for CancelRequest messages.
	pid                 int                 // Cancellation PID.
	noticeHandler       func(*Error)        // If not nil, notices will be synchronously sent here
	notificationHandler func(*Notification) // If not nil, notifications will be synchronously sent here
	gss                 GSS                 // GSSAPI context
	gssComplete         bool                // GSSAPI peer authentication completed
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

func (cn *conn) writeBuf(b proto.RequestCode) *writeBuf {
	cn.scratch[0] = byte(b)
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
	cn, err := c.open(context.Background())
	if err != nil {
		return nil, err
	}
	return cn, nil
}

func (c *Connector) open(ctx context.Context) (*conn, error) {
	tsa := c.cfg.TargetSessionAttrs
restartAll:
	var (
		errs []error
		app  = func(err error, cfg Config) bool {
			if err != nil {
				if debugProto {
					fmt.Fprintln(os.Stderr, "CONNECT  (error)", err)
				}
				errs = append(errs, fmt.Errorf("connecting to %s:%d: %w", cfg.Host, cfg.Port, err))
			}
			return err != nil
		}
	)
	for _, cfg := range c.cfg.hosts() {
		mode := cfg.SSLMode
		if mode == "" {
			mode = SSLModePrefer
		}
	restartHost:
		if debugProto {
			fmt.Fprintln(os.Stderr, "CONNECT ", cfg.debugString())
		}

		cfg.SSLMode = mode
		cn := &conn{cfg: cfg, dialer: c.dialer}
		pgpassHost := cn.cfg.Host
		if cn.cfg.Hostaddr.IsValid() && !cn.cfg.hasExplicitHost() {
			pgpassHost = cn.cfg.Hostaddr.String()
		}
		cn.cfg.Password = pgpass.PasswordFromPgpass(cn.cfg.Passfile, cn.cfg.User, cn.cfg.Password,
			pgpassHost, strconv.Itoa(int(cn.cfg.Port)), cn.cfg.Database)

		var err error
		cn.c, err = dial(ctx, c.dialer, cn.cfg)
		if ctxErr := ctx.Err(); ctxErr != nil {
			if cn.c != nil {
				_ = cn.c.Close()
			}
			return nil, ctxErr
		}
		if app(err, cfg) {
			continue
		}
		handshakeConn := cn.c
		stopContext := context.AfterFunc(ctx, func() {
			_ = handshakeConn.SetDeadline(time.Now())
			_ = handshakeConn.Close()
		})
		closeHandshake := func() {
			stopContext()
			_ = cn.c.Close()
		}
		contextError := func(err error) error {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}

		err = cn.ssl(cn.cfg, mode)
		if err != nil && mode == SSLModePrefer {
			closeHandshake()
			if err = ctx.Err(); err != nil {
				app(err, cfg)
				continue
			}
			mode = SSLModeDisable
			goto restartHost
		}
		err = contextError(err)
		if app(err, cfg) {
			closeHandshake()
			continue
		}

		cn.buf = bufio.NewReader(cn.c)
		err = cn.startup(cn.cfg)
		if err != nil && mode == SSLModeAllow {
			closeHandshake()
			if err = ctx.Err(); err != nil {
				app(err, cfg)
				continue
			}
			mode = SSLModeRequire
			goto restartHost
		}
		err = contextError(err)
		if app(err, cfg) {
			closeHandshake()
			continue
		}

		err = cn.checkTSA(tsa)
		err = contextError(err)
		if app(err, cfg) {
			closeHandshake()
			continue
		}
		err = contextError(cn.err.get())
		if app(err, cfg) {
			closeHandshake()
			continue
		}

		stopContext()
		if err = contextError(nil); app(err, cfg) {
			_ = cn.c.Close()
			continue
		}
		// Reset deadlines set by connect_timeout, or by cancellation of ctx.
		err = cn.c.SetDeadline(time.Time{})
		if app(err, cfg) {
			_ = cn.c.Close()
			continue
		}

		return cn, nil
	}

	// target_session_attrs=prefer-standby is treated as standby in checkTSA; we
	// ran out of hosts so none are on standby. Clear the setting and try again.
	if tsa == TargetSessionAttrsPreferStandby {
		tsa = TargetSessionAttrsAny
		goto restartAll
	}

	if len(c.cfg.Multi) == 0 {
		// Remove the "connecting to [..]" when we have just one host, so the
		// error is identical to what we had before.
		return nil, errors.Unwrap(errs[0])
	}
	return nil, fmt.Errorf("pq: could not connect to any of the hosts:\n%w", errors.Join(errs...))
}

func (cn *conn) getBool(query string) (value bool, err error) {
	res, err := cn.simpleQuery(query)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := res.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	v := make([]driver.Value, 1)
	err = res.Next(v)
	if err != nil {
		return false, err
	}

	switch vv := v[0].(type) {
	default:
		return false, fmt.Errorf("parseBool: unknown type %T: %[1]v", v[0])
	case bool:
		return vv, nil
	case string:
		return vv == "on", nil
	}
}

func (cn *conn) checkTSA(tsa TargetSessionAttrs) error {
	var (
		geths = func() (hs bool, err error) {
			hs = cn.parameterStatus.inHotStandby.Bool
			if !cn.parameterStatus.inHotStandby.Valid {
				hs, err = cn.getBool("select pg_catalog.pg_is_in_recovery()")
			}
			return hs, err
		}
		getro = func() (ro bool, err error) {
			ro = cn.parameterStatus.defaultTransactionReadOnly.Bool
			if !cn.parameterStatus.defaultTransactionReadOnly.Valid {
				ro, err = cn.getBool("show transaction_read_only")
			}
			return ro, err
		}
	)

	switch tsa {
	default:
		panic("unreachable")
	case "", TargetSessionAttrsAny:
		return nil
	case TargetSessionAttrsReadWrite, TargetSessionAttrsReadOnly:
		readonly, err := getro()
		if err != nil {
			return err
		}
		switch {
		case tsa == TargetSessionAttrsReadOnly && !readonly:
			return errors.New("session is not read-only")
		case tsa == TargetSessionAttrsReadWrite:
			if readonly {
				return errors.New("session is read-only")
			}
			hs, err := geths()
			if err != nil {
				return err
			}
			if hs {
				return errors.New("server is in hot standby mode")
			}
			return nil
		default:
			return nil
		}
	case TargetSessionAttrsPrimary, TargetSessionAttrsStandby, TargetSessionAttrsPreferStandby:
		hs, err := geths()
		if err != nil {
			return err
		}
		switch {
		case (tsa == TargetSessionAttrsStandby || tsa == TargetSessionAttrsPreferStandby) && !hs:
			return errors.New("server is not in hot standby mode")
		case tsa == TargetSessionAttrsPrimary && hs:
			return errors.New("server is in hot standby mode")
		default:
			return nil
		}
	}
}

func dial(ctx context.Context, d Dialer, cfg Config) (net.Conn, error) {
	network, address := cfg.network()

	// Zero or not specified means wait indefinitely.
	if cfg.ConnectTimeout > 0 {
		// connect_timeout should apply to the entire connection establishment
		// procedure, so we both use a timeout for the TCP connection
		// establishment and set a deadline for doing the initial handshake. The
		// deadline is then reset after startup() is done.
		var (
			deadline = time.Now().Add(cfg.ConnectTimeout)
			conn     net.Conn
			err      error
		)
		dialCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
		if dctx, ok := d.(DialerContext); ok {
			conn, err = dctx.DialContext(dialCtx, network, address)
		} else {
			conn, err = dialLegacy(dialCtx, func() (net.Conn, error) {
				return d.DialTimeout(network, address, cfg.ConnectTimeout)
			})
		}
		conn, err = checkedDialResult(conn, err)
		if err != nil {
			return nil, err
		}
		err = conn.SetDeadline(deadline)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		return conn, nil
	}
	if dctx, ok := d.(DialerContext); ok {
		conn, err := dctx.DialContext(ctx, network, address)
		return checkedDialResult(conn, err)
	}
	return dialLegacy(ctx, func() (net.Conn, error) {
		return d.Dial(network, address)
	})
}

type legacyDialResult struct {
	conn net.Conn
	err  error
}

func dialLegacy(ctx context.Context, call func() (net.Conn, error)) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		conn, err := call()
		return checkedDialResult(conn, err)
	}

	result := make(chan legacyDialResult)
	go func() {
		conn, err := call()
		r := legacyDialResult{conn: conn, err: err}
		select {
		case result <- r:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()

	select {
	case r := <-result:
		if err := ctx.Err(); err != nil {
			if r.conn != nil {
				_ = r.conn.Close()
			}
			return nil, err
		}
		return checkedDialResult(r.conn, r.err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func checkedDialResult(conn net.Conn, err error) (net.Conn, error) {
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("pq: dialer returned a nil connection without an error")
	}
	return conn, nil
}

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction || cn.txnStatus == txnStatusInFailedTransaction
}

func (cn *conn) checkIsInTransaction(intxn bool) error {
	if cn.isInTransaction() != intxn {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected transaction status %v", cn.txnStatus)
	}
	return nil
}

// Implement [driver.ConnBeginTx].
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

	if err := cn.err.get(); err != nil {
		return nil, err
	}
	if err := cn.checkIsInTransaction(false); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	finish := cn.watchCancel(ctx, false)
	_, commandTag, err := cn.simpleExec("BEGIN" + mode)
	if err != nil {
		finish()
		return nil, cn.handleError(err)
	}
	if commandTag != "BEGIN" {
		finish()
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected command tag %s", commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		finish()
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected transaction status %v", cn.txnStatus)
	}

	cn.txnFinish = finish
	return cn, nil
}

func (cn *conn) Commit() error {
	defer func() {
		if cn.txnFinish != nil {
			cn.txnFinish()
		}
	}()
	if err := cn.err.get(); err != nil {
		return err
	}
	if err := cn.checkIsInTransaction(true); err != nil {
		return err
	}

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
		return cn.handleError(err)
	}
	if commandTag != "COMMIT" {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	return cn.checkIsInTransaction(false)
}

func (cn *conn) Rollback() error {
	defer func() {
		if cn.txnFinish != nil {
			cn.txnFinish()
		}
	}()
	if err := cn.err.get(); err != nil {
		return err
	}

	err := cn.rollback()
	return cn.handleError(err)
}

func (cn *conn) rollback() (err error) {
	if err := cn.checkIsInTransaction(true); err != nil {
		return err
	}

	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
		if cn.isInTransaction() {
			cn.err.set(driver.ErrBadConn)
		}
		return err
	}
	if commandTag != "ROLLBACK" {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	return cn.checkIsInTransaction(false)
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(q string) (res driver.Result, commandTag string, resErr error) {
	if debugProto {
		fmt.Fprintln(os.Stderr, "         START conn.simpleExec")
		defer fmt.Fprintln(os.Stderr, "         END conn.simpleExec")
	}

	b := cn.writeBuf(proto.Query)
	b.string(q)
	err := cn.send(b)
	if err != nil {
		return nil, "", err
	}

	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, "", err
		}
		switch t {
		case proto.CommandComplete:
			res, commandTag, err = cn.parseComplete(r.string())
			if err != nil {
				return nil, "", err
			}
		case proto.ReadyForQuery:
			cn.processReadyForQuery(r)
			if res == nil && resErr == nil {
				resErr = errUnexpectedReady
			}
			return res, commandTag, resErr
		case proto.ErrorResponse:
			resErr = parseError(r, q)
		case proto.EmptyQueryResponse:
			res = emptyRows
		case proto.RowDescription, proto.DataRow:
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			return nil, "", fmt.Errorf("pq: unknown response for simple query: %q", t)
		}
	}
}

func (cn *conn) simpleQuery(q string) (*rows, error) {
	if debugProto {
		fmt.Fprintln(os.Stderr, "         START conn.simpleQuery")
		defer fmt.Fprintln(os.Stderr, "         END conn.simpleQuery")
	}

	b := cn.writeBuf(proto.Query)
	b.string(q)
	err := cn.send(b)
	if err != nil {
		return nil, cn.handleError(err, q)
	}

	var (
		res    *rows
		resErr error
	)
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, cn.handleError(err, q)
		}
		switch t {
		case proto.CommandComplete, proto.EmptyQueryResponse:
			// We allow queries which don't return any results through Query as
			// well as Exec. We still have to give database/sql a rows object
			// the user can close, though, to avoid connections from being
			// leaked. A "rows" with done=true works fine for that purpose.
			if resErr != nil {
				cn.err.set(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected message %q in simple query execution", t)
			}
			if res == nil {
				res = &rows{cn: cn}
			}
			// Set the result and tag to the last command complete if there wasn't a
			// query already run. Although queries usually return from here and cede
			// control to Next, a query with zero results does not.
			if t == proto.CommandComplete {
				res.result, res.tag, err = cn.parseComplete(r.string())
				if err != nil {
					return nil, cn.handleError(err, q)
				}
				if res.colNames != nil {
					return res, cn.handleError(resErr, q)
				}
			}
			res.done = true
		case proto.ReadyForQuery:
			cn.processReadyForQuery(r)
			if resErr != nil {
				return nil, cn.handleError(resErr, q)
			}
			if res == nil {
				return &rows{done: true}, nil
			}
			if !res.done {
				cn.err.set(driver.ErrBadConn)
				return nil, errUnexpectedReady
			}
			return res, nil
		case proto.ErrorResponse:
			res = nil
			resErr = parseError(r, q)
		case proto.DataRow:
			if res == nil {
				cn.err.set(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected DataRow in simple query execution")
			}
			return res, cn.saveMessage(t, r) // The query didn't fail; kick off to Next
		case proto.RowDescription:
			if resErr != nil {
				cn.err.set(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected RowDescription after error: %w", resErr)
			}
			// res might be non-nil here if we received a previous
			// CommandComplete, but that's fine and just overwrite it.
			res = &rows{cn: cn, rowsHeader: parsePortalRowDescribe(r)}

			// To work around a bug in QueryRow in Go 1.2 and earlier, wait
			// until the first DataRow has been received.
		default:
			cn.err.set(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unknown response for simple query: %q", t)
		}
	}
}

// Decides which column formats to use for a prepared statement.  The input is
// an array of type oids, one element per result column.
func decideColumnFormats(colTyps []fieldDesc, forceText bool) (colFmts []format, colFmtData []byte, _ error) {
	if len(colTyps) == 0 {
		return nil, colFmtDataAllText, nil
	}

	colFmts = make([]format, len(colTyps))
	if forceText {
		return colFmts, colFmtDataAllText, nil
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
		return colFmts, colFmtDataAllBinary, nil
	} else if allText {
		return colFmts, colFmtDataAllText, nil
	} else {
		colFmtData = make([]byte, 2+len(colFmts)*2)
		if len(colFmts) > math.MaxUint16 {
			return nil, nil, fmt.Errorf("pq: too many columns (%d > math.MaxUint16)", len(colFmts))
		}
		binary.BigEndian.PutUint16(colFmtData, uint16(len(colFmts)))
		for i, v := range colFmts {
			binary.BigEndian.PutUint16(colFmtData[2+i*2:], uint16(v))
		}
		return colFmts, colFmtData, nil
	}
}

func (cn *conn) prepareTo(q, stmtName string) (*stmt, error) {
	if debugProto {
		fmt.Fprintln(os.Stderr, "         START conn.prepareTo")
		defer fmt.Fprintln(os.Stderr, "         END conn.prepareTo")
	}

	st := &stmt{cn: cn, name: stmtName}

	b := cn.writeBuf(proto.Parse)
	b.string(st.name)
	b.string(q)
	b.int16(0)

	b.next(proto.Describe)
	b.byte(proto.Sync)
	b.string(st.name)

	b.next(proto.Sync)
	err := cn.send(b)
	if err != nil {
		return nil, err
	}

	err = cn.readParseResponse()
	if err != nil {
		return nil, err
	}
	st.paramTyps, st.colNames, st.colTyps, err = cn.readStatementDescribeResponse()
	if err != nil {
		return nil, err
	}
	st.colFmts, st.colFmtData, err = decideColumnFormats(st.colTyps, cn.cfg.DisablePreparedBinaryResult)
	if err != nil {
		return nil, err
	}

	err = cn.readReadyForQuery()
	if err != nil {
		return nil, err
	}
	return st, nil
}

// Implement [driver.ConnPrepareContext].
func (cn *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer cn.watchCancel(ctx, false)()
	if err := cn.err.get(); err != nil {
		return nil, err
	}

	if pqsql.StartsWithCopy(q) {
		s, err := cn.prepareCopyIn(q)
		return s, cn.handleError(err, q)
	}
	s, err := cn.prepareTo(q, cn.gname())
	if err != nil {
		return nil, cn.handleError(err, q)
	}
	return s, nil
}

func (cn *conn) Close() error {
	if cn.c == nil {
		return nil
	}
	if err := cn.c.SetWriteDeadline(cn.closeDeadline()); err != nil {
		_ = cn.c.Close()
		return cn.handleError(err)
	}
	// Don't go through send(); ListenerConn relies on us not scribbling on the
	// scratch buffer of this connection.
	err := cn.sendSimpleMessage(proto.Terminate)
	closeErr := cn.c.Close()
	if err != nil {
		return cn.handleError(err)
	}
	return closeErr
}

func (cn *conn) closeDeadline() time.Time {
	timeout := cn.closeTimeout
	if timeout <= 0 {
		timeout = defaultNetworkCloseTimeout
	}
	return time.Now().Add(timeout)
}

// CheckNamedValue implements [driver.NamedValueChecker].
func (cn *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if cn.cfg.BinaryParameters {
		if bin, ok := nv.Value.(interface{ BinaryValue() ([]byte, error) }); ok {
			var err error
			nv.Value, err = bin.BinaryValue()
			return err
		}
	}

	// Ignore Valuer, for backward compatibility with pq.Array().
	if _, ok := nv.Value.(driver.Valuer); ok {
		return driver.ErrSkip
	}

	v := reflect.ValueOf(nv.Value)
	if !v.IsValid() {
		return driver.ErrSkip
	}
	t := v.Type()
	for t.Kind() == reflect.Pointer {
		t, v = t.Elem(), v.Elem()
	}

	// Ignore []byte and related types: *[]byte, json.RawMessage, etc.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return driver.ErrSkip
	}

	switch v.Kind() {
	default:
		return driver.ErrSkip
	case reflect.Slice:
		var err error
		nv.Value, err = Array(v.Interface()).Value()
		return err
	case reflect.Uint64:
		value := v.Uint()
		if value >= math.MaxInt64 {
			nv.Value = strconv.FormatUint(value, 10)
		} else {
			nv.Value = int64(value)
		}
		return nil
	}
}

// Implement [driver.QueryerContext].
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finish := cn.watchCancel(ctx, false)
	r, err := cn.query(query, args)
	if err != nil {
		if finish != nil {
			finish()
		}
		return nil, err
	}
	r.finish = finish
	return r, nil
}

func (cn *conn) query(query string, args []driver.NamedValue) (*rows, error) {
	if debugProto {
		fmt.Fprintln(os.Stderr, "         START conn.query")
		defer fmt.Fprintln(os.Stderr, "         END conn.query")
	}
	if err := cn.err.get(); err != nil {
		return nil, err
	}

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	if cn.cfg.BinaryParameters {
		err := cn.sendBinaryModeQuery(query, args)
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.readParseResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.readBindResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}

		rows := &rows{cn: cn}
		rows.rowsHeader, err = cn.readPortalDescribeResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.postExecuteWorkaround()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		return rows, nil
	}

	st, err := cn.prepareTo(query, "")
	if err != nil {
		return nil, cn.handleError(err, query)
	}
	err = st.exec(args)
	if err != nil {
		return nil, cn.handleError(err, query)
	}
	return &rows{
		cn:         cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

// Implement [driver.ExecerContext].
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer cn.watchCancel(ctx, false)()
	if err := cn.err.get(); err != nil {
		return nil, err
	}

	// simpleExec is *much* faster than going through prepare/exec.
	if len(args) == 0 {
		r, _, err := cn.simpleExec(query) // Ignore commandTag, our caller doesn't care.
		return r, cn.handleError(err, query)
	}

	if cn.cfg.BinaryParameters {
		err := cn.sendBinaryModeQuery(query, args)
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.readParseResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.readBindResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}

		_, err = cn.readPortalDescribeResponse()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		err = cn.postExecuteWorkaround()
		if err != nil {
			return nil, cn.handleError(err, query)
		}
		res, _, err := cn.readExecuteResponse("Execute")
		return res, cn.handleError(err, query)
	}

	// Use the unnamed statement to defer planning until bind time, or else
	// value-based selectivity estimates cannot be used.
	st, err := cn.prepareTo(query, "")
	if err != nil {
		return nil, cn.handleError(err, query)
	}
	err = st.exec(args)
	if err != nil {
		return nil, cn.handleError(err, query)
	}
	r, _, err := cn.readExecuteResponse("simple query")
	return r, cn.handleError(err, query)
}

func (cn *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := cn.err.get(); err != nil {
		return err
	}
	defer cn.watchCancel(ctx, false)()
	rows, err := cn.simpleQuery(";")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		cn.err.set(driver.ErrBadConn)
		return driver.ErrBadConn
	}
	if err = rows.Close(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		cn.err.set(driver.ErrBadConn)
		return driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return cn.err.get()
}

type safeRetryError struct{ Err error }

func (se *safeRetryError) Error() string { return se.Err.Error() }
func (se *safeRetryError) Unwrap() error { return se.Err }

// write sends one complete frontend message. Once any prefix has been written,
// retrying the operation could splice a second message into the protocol
// stream, so every incomplete positive-count write poisons the connection.
// A zero-count failure is safe for database/sql to retry on a new connection.
func (cn *conn) write(p []byte) error {
	n, err := cn.c.Write(p)
	if n == len(p) && err == nil {
		return nil
	}
	if n < 0 || n > len(p) {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: invalid write count %d for %d-byte message", n, len(p))
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	if n == 0 {
		return &safeRetryError{Err: err}
	}
	cn.err.set(driver.ErrBadConn)
	return err
}

func (cn *conn) send(m *writeBuf) error {
	if debugProto {
		w := m.wrap()
		for len(w) > 0 { // Can contain multiple messages.
			c := proto.RequestCode(w[0])
			l := int(binary.BigEndian.Uint32(w[1:5])) - 4
			payload := w[5 : l+5]
			if c == proto.PasswordMessage {
				payload = []byte("[REDACTED]")
			}
			fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", c, l, payload)
			w = w[l+5:]
		}
	}

	return cn.write(m.wrap())
}

func (cn *conn) sendStartupPacket(m *writeBuf) error {
	if debugProto {
		w := m.wrap()
		name := "Startup"
		payload := w[5:]
		if len(payload) >= 4 && int32(binary.BigEndian.Uint32(payload[:4])) == proto.CancelRequestCode {
			name = "CancelRequest"
			payload = []byte("[REDACTED]")
		} else {
			payload = debugStartupPayload(payload)
		}
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", name, int(binary.BigEndian.Uint32(w[1:5]))-4, payload)
	}
	return cn.write((m.wrap())[1:])
}

func debugStartupPayload(payload []byte) []byte {
	if len(payload) < 4 {
		return []byte("[REDACTED MALFORMED STARTUP]")
	}

	redacted := append([]byte(nil), payload[:4]...)
	fields := payload[4:]
	for {
		keyEnd := bytes.IndexByte(fields, 0)
		if keyEnd < 0 {
			return []byte("[REDACTED MALFORMED STARTUP]")
		}
		key := fields[:keyEnd]
		fields = fields[keyEnd+1:]
		redacted = append(redacted, key...)
		redacted = append(redacted, 0)
		if len(key) == 0 {
			if len(fields) != 0 {
				return []byte("[REDACTED MALFORMED STARTUP]")
			}
			return redacted
		}

		valueEnd := bytes.IndexByte(fields, 0)
		if valueEnd < 0 {
			return []byte("[REDACTED MALFORMED STARTUP]")
		}
		value := fields[:valueEnd]
		fields = fields[valueEnd+1:]
		if bytes.EqualFold(key, []byte("password")) {
			value = []byte("[REDACTED]")
		}
		redacted = append(redacted, value...)
		redacted = append(redacted, 0)
	}
}

// Send a message of type typ to the server on the other end of cn. The message
// should have no payload. This method does not use the scratch buffer.
func (cn *conn) sendSimpleMessage(typ proto.RequestCode) error {
	if debugProto {
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", typ, 0, []byte{})
	}
	return cn.write([]byte{byte(typ), '\x00', '\x00', '\x00', '\x04'})
}

// saveMessage memorizes a message and its buffer in the conn struct.
// recvMessage will then return these values on the next call to it.  This
// method is useful in cases where you have to see what the next message is
// going to be (e.g. to see whether it's an error or not) but you can't handle
// the message yourself.
func (cn *conn) saveMessage(typ proto.ResponseCode, buf *readBuf) error {
	if cn.saveMessageType != 0 {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("unexpected saveMessageType %d", cn.saveMessageType)
	}
	cn.saveMessageType = typ
	cn.saveMessageBuffer = *buf
	return nil
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage(r *readBuf) (proto.ResponseCode, error) {
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
		return 0, cn.badBackendMessage(err)
	}

	// Read the type and length of the message that follows. The protocol length
	// is unsigned on the wire, so validate it before converting to int or
	// subtracting the four-byte length word.
	t := proto.ResponseCode(x[0])
	wireLength := binary.BigEndian.Uint32(x[1:])

	// When PostgreSQL cannot start a backend (e.g., an external process limit),
	// it sends plain text like "Ecould not fork new process [..]", which
	// doesn't use the standard encoding for the Error message.
	//
	// libpq checks "if ErrorResponse && (msgLength < 8 || msgLength > MAX_ERRLEN)",
	// but check < 4 since n represents bytes remaining to be read after length.
	//
	// Use txnStatus to check if we're in the startup phase.
	if cn.txnStatus == 0 && t == proto.ErrorResponse &&
		(wireLength < 8 || wireLength-4 > proto.MaxMsgLen) {
		msg := readPreProtocolError(cn.buf, proto.MaxMsgLen-len(x[1:]))
		return 0, cn.badBackendMessage(fmt.Errorf("pq: server error: %s%s", string(x[1:]), msg))
	}
	if wireLength < 4 {
		return 0, cn.badBackendMessage(fmt.Errorf("pq: lost synchronization with server: got message type %q, invalid length %d", t, wireLength))
	}
	payloadLength := wireLength - 4
	if cn.startupPhase && payloadLength > proto.MaxMsgLen {
		return 0, cn.badBackendMessage(fmt.Errorf("pq: lost synchronization with server during startup: got message type %q, length %d", t, payloadLength))
	}
	if t == proto.ParameterDescription && payloadLength > proto.MaxParameterDescriptionLen {
		return 0, cn.badBackendMessage(fmt.Errorf("pq: lost synchronization with server: got ParameterDescription length %d, maximum is %d", payloadLength, proto.MaxParameterDescriptionLen))
	}
	if wireLength > proto.MaxLongMsgLen {
		return 0, cn.badBackendMessage(fmt.Errorf("pq: lost synchronization with server: got message type %q, length %d exceeds maximum %d", t, wireLength, proto.MaxLongMsgLen))
	}
	if !proto.ValidLongMessageType(t) && payloadLength > proto.MaxMsgLen {
		return 0, cn.badBackendMessage(fmt.Errorf("pq: lost synchronization with server: got message type %q, length %d", t, payloadLength))
	}
	n := int(payloadLength)

	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		return 0, cn.badBackendMessage(err)
	}
	if err := validateBackendMessage(t, y); err != nil {
		return 0, cn.badBackendMessage(err)
	}
	*r = y
	if debugProto {
		payload := y
		if t == proto.AuthenticationRequest || t == proto.BackendKeyData {
			payload = []byte("[REDACTED]")
		}
		fmt.Fprintf(os.Stderr, "SERVER ← %-20s %5d  %q\n", t, n, payload)
	}
	return t, nil
}

func (cn *conn) badBackendMessage(err error) error {
	cn.err.set(driver.ErrBadConn)
	return err
}

func readPreProtocolError(r *bufio.Reader, limit int) string {
	msg := make([]byte, 0, min(limit, 256))
	for len(msg) < limit {
		b, err := r.ReadByte()
		if err != nil || b == 0 {
			break
		}
		msg = append(msg, b)
	}
	return string(msg)
}

func validateBackendMessage(t proto.ResponseCode, payload []byte) error {
	minimum := 0
	switch t {
	case proto.ReadyForQuery:
		minimum = 1
	case proto.AuthenticationRequest, proto.FunctionCallResponse:
		minimum = 4
	case proto.BackendKeyData, proto.NegotiateProtocolVersion:
		minimum = 8
	case proto.DataRow, proto.RowDescription, proto.ParameterDescription:
		minimum = 2
	case proto.CopyInResponse, proto.CopyOutResponse, proto.CopyBothResponse:
		minimum = 3
	case proto.NotificationResponse:
		minimum = 6 // PID and two empty, NUL-terminated strings.
	case proto.ParameterStatus:
		minimum = 2 // Two empty, NUL-terminated strings.
	case proto.CommandComplete, proto.ErrorResponse, proto.NoticeResponse:
		minimum = 1 // At least the terminating NUL byte.
	}
	if len(payload) < minimum {
		return fmt.Errorf("pq: invalid %s payload: got %d bytes, need at least %d", t, len(payload), minimum)
	}

	switch t {
	case proto.ReadyForQuery:
		if len(payload) != 1 {
			return fmt.Errorf("pq: invalid %s payload length: got %d, want 1", t, len(payload))
		}
		switch transactionStatus(payload[0]) {
		case txnStatusIdle, txnStatusIdleInTransaction, txnStatusInFailedTransaction:
		default:
			return fmt.Errorf("pq: invalid %s transaction status %q", t, payload[0])
		}
	case proto.AuthenticationRequest:
		code := proto.AuthCode(int32(binary.BigEndian.Uint32(payload)))
		switch code {
		case proto.AuthReqOk, proto.AuthReqKrb4, proto.AuthReqKrb5,
			proto.AuthReqPassword, proto.AuthReqGSS, proto.AuthReqSSPI:
			if len(payload) != 4 {
				return fmt.Errorf("pq: invalid %s payload length for %s: got %d, want 4", t, code, len(payload))
			}
		case proto.AuthReqCrypt:
			if len(payload) != 6 {
				return fmt.Errorf("pq: invalid %s payload length for %s: got %d, want 6", t, code, len(payload))
			}
		case proto.AuthReqMD5:
			if len(payload) != 8 {
				return fmt.Errorf("pq: invalid %s payload length for %s: got %d, want 8", t, code, len(payload))
			}
		case proto.AuthReqSASL:
			if !validSASLMechanismList(payload) {
				return fmt.Errorf("pq: invalid %s SASL mechanism list", t)
			}
		}
	case proto.CommandComplete:
		if end, ok := backendCString(payload, 0); !ok || end != len(payload) {
			return fmt.Errorf("pq: invalid %s payload: command tag is not NUL-terminated", t)
		}
	case proto.ParameterStatus:
		end, ok := backendCString(payload, 0)
		if !ok {
			return fmt.Errorf("pq: invalid %s payload: parameter name is not NUL-terminated", t)
		}
		if end, ok = backendCString(payload, end); !ok || end != len(payload) {
			return fmt.Errorf("pq: invalid %s payload: parameter value is not NUL-terminated", t)
		}
	case proto.NotificationResponse:
		end, ok := backendCString(payload, 4)
		if !ok {
			return fmt.Errorf("pq: invalid %s payload: channel is not NUL-terminated", t)
		}
		if end, ok = backendCString(payload, end); !ok || end != len(payload) {
			return fmt.Errorf("pq: invalid %s payload: notification payload is not NUL-terminated", t)
		}
	case proto.ErrorResponse, proto.NoticeResponse:
		if !validBackendErrorFields(payload) {
			return fmt.Errorf("pq: invalid %s payload: fields are not NUL-terminated", t)
		}
	case proto.DataRow:
		if !validDataRow(payload) {
			return fmt.Errorf("pq: invalid %s payload: invalid column data", t)
		}
	case proto.RowDescription:
		if !validRowDescription(payload) {
			return fmt.Errorf("pq: invalid %s payload: invalid field description", t)
		}
	case proto.ParameterDescription:
		count := int(binary.BigEndian.Uint16(payload))
		if len(payload) != 2+count*4 {
			return fmt.Errorf("pq: invalid %s payload length %d for %d parameters", t, len(payload), count)
		}
	case proto.CopyInResponse, proto.CopyOutResponse, proto.CopyBothResponse:
		count := int(binary.BigEndian.Uint16(payload[1:]))
		if len(payload) != 3+count*2 {
			return fmt.Errorf("pq: invalid %s payload length %d for %d columns", t, len(payload), count)
		}
		if payload[0] > byte(formatBinary) {
			return fmt.Errorf("pq: invalid %s overall format code %d", t, payload[0])
		}
		for pos := 3; pos < len(payload); pos += 2 {
			if formatCode := binary.BigEndian.Uint16(payload[pos:]); formatCode > uint16(formatBinary) {
				return fmt.Errorf("pq: invalid %s column format code %d", t, formatCode)
			}
		}
	case proto.FunctionCallResponse:
		length := int32(binary.BigEndian.Uint32(payload))
		if length < -1 || (length == -1 && len(payload) != 4) ||
			(length >= 0 && int64(length) != int64(len(payload)-4)) {
			return fmt.Errorf("pq: invalid %s result length %d for %d payload bytes", t, length, len(payload)-4)
		}
	case proto.NegotiateProtocolVersion:
		if !validNegotiateProtocolVersion(payload) {
			return fmt.Errorf("pq: invalid %s payload", t)
		}
	}
	return nil
}

func backendCString(payload []byte, start int) (int, bool) {
	for i := start; i < len(payload); i++ {
		if payload[i] == 0 {
			return i + 1, true
		}
	}
	return 0, false
}

func validBackendErrorFields(payload []byte) bool {
	for pos := 0; pos < len(payload); {
		if payload[pos] == 0 {
			return pos == len(payload)-1
		}
		var ok bool
		pos, ok = backendCString(payload, pos+1)
		if !ok {
			return false
		}
	}
	return false
}

func validDataRow(payload []byte) bool {
	count := int(binary.BigEndian.Uint16(payload))
	pos := 2
	for range count {
		if len(payload)-pos < 4 {
			return false
		}
		length := int32(binary.BigEndian.Uint32(payload[pos:]))
		pos += 4
		switch {
		case length == -1:
		case length < -1 || int64(length) > int64(len(payload)-pos):
			return false
		default:
			pos += int(length)
		}
	}
	return pos == len(payload)
}

func validRowDescription(payload []byte) bool {
	count := int(binary.BigEndian.Uint16(payload))
	pos := 2
	for range count {
		var ok bool
		pos, ok = backendCString(payload, pos)
		if !ok || len(payload)-pos < 18 {
			return false
		}
		formatCode := binary.BigEndian.Uint16(payload[pos+16 : pos+18])
		if formatCode > uint16(formatBinary) {
			return false
		}
		pos += 18
	}
	return pos == len(payload)
}

func validNegotiateProtocolVersion(payload []byte) bool {
	count := int32(binary.BigEndian.Uint32(payload[4:]))
	if count < 0 {
		return false
	}
	pos := 8
	for range count {
		var ok bool
		pos, ok = backendCString(payload, pos)
		if !ok {
			return false
		}
	}
	return pos == len(payload)
}

func validSASLMechanismList(payload []byte) bool {
	for pos := 4; pos < len(payload); {
		end, ok := backendCString(payload, pos)
		if !ok {
			return false
		}
		if end == pos+1 {
			return end == len(payload)
		}
		pos = end
	}
	return false
}

// recvError receives a message from the backend, returning an error if an error
// happened while reading the message or the received message is an
// ErrorResponse. NoticeResponses are ignored. This function should generally be
// used only during the startup sequence.
func (cn *conn) recvError() (proto.ResponseCode, *readBuf, error) {
	for {
		r := new(readBuf)
		t, err := cn.recvMessage(r)
		if err != nil {
			return 0, nil, err
		}
		switch t {
		case proto.ErrorResponse:
			return 0, nil, parseError(r, "")
		case proto.NoticeResponse:
			if n := cn.noticeHandler; n != nil {
				n(parseError(r, ""))
			}
		case proto.NotificationResponse:
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		default:
			return t, r, nil
		}
	}
}

// recv1Buf is exactly equivalent to recv1, except it uses a buffer supplied by
// the caller to avoid an allocation.
func (cn *conn) recv1Buf(r *readBuf) (proto.ResponseCode, error) {
	for {
		t, err := cn.recvMessage(r)
		if err != nil {
			return 0, err
		}

		switch t {
		case proto.NotificationResponse:
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		case proto.NoticeResponse:
			if n := cn.noticeHandler; n != nil {
				n(parseError(r, ""))
			}
		case proto.ParameterStatus:
			cn.processParameterStatus(r)
		default:
			return t, nil
		}
	}
}

// recv1 receives a message from the backend, returning an error if an error
// happened while reading the message or the received message an ErrorResponse.
// All asynchronous messages are ignored, with the exception of ErrorResponse.
func (cn *conn) recv1() (proto.ResponseCode, *readBuf, error) {
	r := new(readBuf)
	t, err := cn.recv1Buf(r)
	if err != nil {
		return 0, nil, err
	}
	return t, r, nil
}

// We need to let PostgreSQL know the query is cancelled: just dropping the
// connection won't stop the query.
//
// So create a goroutine which selects on ctx.Done() and a finish channel.
// Returns a function to send to this, which should be called after the query is
// finished.
func (cn *conn) watchCancel(ctx context.Context, fromStmt bool) func() {
	if ctx.Done() == nil { // "may return nil if this context can never be canceled"
		return func() {}
	}

	finished := make(chan struct{})
	done := make(chan struct{})
	var finishOnce sync.Once
	canceled := false
	go func() {
		defer close(done)
		select {
		case <-finished: // Query finished successfully.
		case <-ctx.Done():
			canceled = true
			cancelCtx, stopCancel := context.WithTimeout(context.Background(), cancelRequestTimeout)
			defer stopCancel()
			cancelResult := make(chan error, 1)
			go func() { cancelResult <- cn.sendCancelRequestContext(cancelCtx) }()

			timer := time.NewTimer(cancelResponseGracePeriod)
			defer timer.Stop()
			queryFinished, cancelComplete := false, false
			finishedWait := (<-chan struct{})(finished)
			for {
				select {
				case <-finishedWait:
					queryFinished = true
					finishedWait = nil
					if cancelComplete {
						return
					}

				case cancelErr := <-cancelResult:
					if queryFinished {
						return
					}
					select {
					case <-finishedWait:
						return
					default:
					}
					if cancelErr == nil {
						// The request was delivered; wait for the primary response or
						// for the grace period to expire.
						cancelComplete = true
						cancelResult = nil
						continue
					}
					cn.invalidateCanceledOperation(ctx, fromStmt)
					return

				case <-timer.C:
					// Stop an in-flight side channel and make a late result harmless.
					// The primary is no longer reusable, so there is no next query for
					// a late CancelRequest to race.
					stopCancel()
					cn.invalidateCanceledOperation(ctx, fromStmt)
					return
				}
			}
		}
	}()

	return func() {
		finishOnce.Do(func() { close(finished) })
		<-done
		if canceled && !fromStmt {
			cn.err.set(ctx.Err())
			if cn.c != nil {
				_ = cn.c.Close()
			}
		}
	}
}

func (cn *conn) invalidateCanceledOperation(ctx context.Context, fromStmt bool) {
	if fromStmt {
		cn.err.set(driver.ErrBadConn)
	} else {
		cn.err.set(ctx.Err())
	}
	if cn.c != nil {
		_ = cn.c.Close()
	}
}

func (cn *conn) sendCancelRequest() error {
	ctx, cancel := context.WithTimeout(context.Background(), cancelRequestTimeout)
	defer cancel()
	return cn.sendCancelRequestContext(ctx)
}

func (cn *conn) sendCancelRequestContext(ctx context.Context) error {
	// Use a copy since a new connection is created here. This is necessary
	// because cancel is called from a goroutine in watchCancel.
	cfg := cn.cfg.Clone()

	// Legacy Dialer implementations only receive the timeout through
	// DialTimeout, not through ctx. Preserve a shorter configured timeout, but
	// never permit a CancelRequest dial to outlive its own context.
	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > cancelRequestTimeout {
		cfg.ConnectTimeout = cancelRequestTimeout
	}

	c, err := dial(ctx, cn.dialer, cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	stopContext := context.AfterFunc(ctx, func() {
		_ = c.SetDeadline(time.Now())
		_ = c.Close()
	})
	defer stopContext()

	cn2 := conn{c: c}
	if err := cn2.ssl(cfg, cfg.SSLMode); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	w := cn2.writeBuf(0)
	w.int32(proto.CancelRequestCode)
	w.int32(cn.pid)
	w.bytes(cn.secretKey)
	if err := cn2.sendStartupPacket(w); err != nil {
		return err
	}

	// Read until EOF to ensure that the server received the cancel.
	_, err = io.Copy(io.Discard, c)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// Don't refer to Config.SSLMode here, as the mode in arguments may be different
// in case of sslmode=allow or prefer.
func (cn *conn) ssl(cfg Config, mode SSLMode) error {
	upgrade, err := ssl(cfg, mode)
	if err != nil {
		return err
	}
	if upgrade == nil {
		return nil // Nothing to do
	}

	// Only negotiate the ssl handshake if requested (which is the default).
	// sslnegotiation=direct is supported by pg17 and above.
	if cfg.SSLNegotiation != SSLNegotiationDirect {
		w := cn.writeBuf(0)
		w.int32(proto.NegotiateSSLCode)
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

func (cn *conn) startup(cfg Config) error {
	cn.startupPhase = true
	defer func() { cn.startupPhase = false }()

	w := cn.writeBuf(0)
	// Send maximum protocol version in startup; if the server doesn't support
	// this version it responds with NegotiateProtocolVersion and the maximum
	// version it supports (and will use).
	w.int32(cfg.MaxProtocolVersion.proto())

	if cfg.User != "" {
		w.string("user")
		w.string(cfg.User)
	}
	if cfg.Database != "" {
		w.string("database")
		w.string(cfg.Database)
	}
	// w.string("replication") // Sent by libpq, but we don't support that.
	if cfg.Options != "" {
		w.string("options")
		w.string(cfg.Options)
	}
	if cfg.ApplicationName != "" {
		w.string("application_name")
		w.string(cfg.ApplicationName)
	}
	if cfg.ClientEncoding != "" {
		w.string("client_encoding")
		w.string(cfg.ClientEncoding)
	}
	if cfg.Datestyle != "" {
		w.string("datestyle")
		w.string(cfg.Datestyle)
	}
	for k, v := range cfg.Runtime {
		w.string(k)
		w.string(v)
	}

	w.string("")
	if err := cn.sendStartupPacket(w); err != nil {
		return err
	}

	var didauth, authOK bool
	for {
		t, r, err := cn.recvError()
		if err != nil {
			return err
		}
		switch t {
		case proto.BackendKeyData:
			cn.pid = r.int32()
			if len(*r) > 256 {
				return fmt.Errorf("pq: cancellation key longer than 256 bytes: %d bytes", len(*r))
			}
			cn.secretKey = make([]byte, len(*r))
			copy(cn.secretKey, *r)
		case proto.ParameterStatus:
			cn.processParameterStatus(r)
		case proto.AuthenticationRequest:
			code := proto.AuthCode(r.int32())
			if code == proto.AuthReqOk {
				authOK = true
			} else {
				didauth = true
				authOK = false
			}
			err := cn.auth(code, r, cfg)
			if err != nil {
				return err
			}
		case proto.NegotiateProtocolVersion:
			newestMinor := r.int32()
			serverVersion := proto.ProtocolVersion30&0xFFFF0000 | newestMinor
			if serverVersion < cfg.MinProtocolVersion.proto() {
				return fmt.Errorf("pq: protocol version mismatch: min_protocol_version=%s; server supports up to 3.%d", cfg.MinProtocolVersion, newestMinor)
			}
		case proto.ReadyForQuery:
			if cn.gss != nil && !cn.gssComplete {
				return errors.New("pq: GSSAPI mutual authentication did not complete")
			}
			if didauth && !authOK {
				return errors.New("pq: server completed startup without AuthenticationOk")
			}
			if !didauth && !cn.cfg.RequireAuth.allows(RequireAuthNone) {
				return fmt.Errorf("pq: authentication method requirement %q failed: server did not perform any authentication", cn.cfg.RequireAuth)
			}
			cn.processReadyForQuery(r)
			return nil
		default:
			return fmt.Errorf("pq: unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(code proto.AuthCode, r *readBuf, cfg Config) error {
	switch code {
	default:
		return fmt.Errorf("pq: unknown authentication response: %s", code)
	case proto.AuthReqKrb4, proto.AuthReqKrb5, proto.AuthReqCrypt, proto.AuthReqSSPI:
		return fmt.Errorf("pq: unsupported authentication method: %s", code)
	case proto.AuthReqOk:
		if cn.gss != nil && !cn.gssComplete {
			return errors.New("pq: GSSAPI mutual authentication did not complete")
		}
		return nil

	case proto.AuthReqPassword:
		if !cn.cfg.RequireAuth.allows(RequireAuthPassword) {
			return fmt.Errorf("pq: authentication method requirement %q failed: server requested %q", cn.cfg.RequireAuth, RequireAuthPassword)
		}
		w := cn.writeBuf(proto.PasswordMessage)
		w.string(cfg.Password)
		// Don't need to check AuthOk response here; auth() is called in a loop,
		// which catches the errors and AuthReqOk responses.
		return cn.send(w)

	case proto.AuthReqMD5:
		if !cn.cfg.RequireAuth.allows(RequireAuthMD5) {
			return fmt.Errorf("pq: authentication method requirement %q failed: server requested %q", cn.cfg.RequireAuth, RequireAuthMD5)
		}
		s := string(r.next(4))
		w := cn.writeBuf(proto.PasswordMessage)
		w.string("md5" + md5s(md5s(cfg.Password+cfg.User)+s))
		// Same here.
		return cn.send(w)

	case proto.AuthReqGSS: // GSSAPI, startup
		if !cn.cfg.RequireAuth.allows(RequireAuthGSS) {
			return fmt.Errorf("pq: authentication method requirement %q failed: server requested %q", cn.cfg.RequireAuth, RequireAuthGSS)
		}
		if cfg.KrbSpn == "" && cfg.Hostaddr.IsValid() && !cfg.hasExplicitHost() {
			return errors.New("pq: GSSAPI authentication requires an explicit host name when krbspn is not set")
		}
		if newGss == nil {
			return fmt.Errorf("pq: kerberos error: no GSSAPI provider registered (import github.com/lib/pq/auth/kerberos)")
		}
		cli, err := newGss()
		if err != nil {
			return fmt.Errorf("pq: kerberos error: %w", err)
		}

		var token []byte
		if cfg.KrbSpn != "" {
			// Use the supplied SPN if provided.
			token, err = cli.GetInitTokenFromSpn(cfg.KrbSpn)
		} else {
			// Allow the kerberos service name to be overridden.
			service := "postgres"
			if cfg.KrbSrvname != "" {
				service = cfg.KrbSrvname
			}
			token, err = cli.GetInitToken(cfg.Host, service)
		}
		if err != nil {
			return fmt.Errorf("pq: failed to get Kerberos ticket: %w", err)
		}

		w := cn.writeBuf(proto.GSSResponse)
		w.bytes(token)
		err = cn.send(w)
		if err != nil {
			return err
		}

		// Store for GSSAPI continue message
		cn.gss = cli
		cn.gssComplete = false
		return nil

	case proto.AuthReqGSSCont: // GSSAPI continue
		if cn.gss == nil {
			return errors.New("pq: GSSAPI protocol error")
		}

		done, tokOut, err := cn.gss.Continue([]byte(*r))
		if err != nil {
			return fmt.Errorf("pq: GSSAPI continuation failed: %w", err)
		}
		if len(tokOut) > 0 {
			w := cn.writeBuf(proto.GSSResponse)
			w.bytes(tokOut)
			if err := cn.send(w); err != nil {
				return err
			}
		}
		cn.gssComplete = done
		return nil

	case proto.AuthReqSASL:
		if !cn.cfg.RequireAuth.allows(RequireAuthScramSHA256) {
			return fmt.Errorf("pq: authentication method requirement %q failed: server requested %q", cn.cfg.RequireAuth, RequireAuthScramSHA256)
		}
		var offered bool
		for {
			mechanism := r.string()
			if mechanism == "" {
				break
			}
			if mechanism == "SCRAM-SHA-256" {
				offered = true
			}
		}
		if !offered {
			return errors.New("pq: server did not offer the supported SASL mechanism SCRAM-SHA-256")
		}
		sc := scram.NewClient(sha256.New, cfg.User, cfg.Password)
		sc.Step(nil)
		if sc.Err() != nil {
			return fmt.Errorf("pq: SCRAM-SHA-256 error: %w", sc.Err())
		}
		scOut := sc.Out()

		w := cn.writeBuf(proto.SASLResponse)
		w.string("SCRAM-SHA-256")
		w.int32(len(scOut))
		w.bytes(scOut)
		err := cn.send(w)
		if err != nil {
			return err
		}

		t, r, err := cn.recvError()
		if err != nil {
			return err
		}
		if t != proto.AuthenticationRequest {
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
		w = cn.writeBuf(proto.SASLResponse)
		w.bytes(scOut)
		err = cn.send(w)
		if err != nil {
			return err
		}

		t, r, err = cn.recvError()
		if err != nil {
			return err
		}
		if t != proto.AuthenticationRequest {
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
// only the command that was executed, e.g. "ALTER TABLE". Returns an error if
// the command can cannot be parsed.
func (cn *conn) parseComplete(commandTag string) (driver.Result, string, error) {
	commandsWithAffectedRows := []string{
		"SELECT ",
		// INSERT is handled below
		"UPDATE ",
		"DELETE ",
		"FETCH ",
		"MOVE ",
		"COPY ",
		"MERGE ",
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
			return nil, "", fmt.Errorf("pq: unexpected INSERT command tag %s", commandTag)
		}
		affectedRows = &parts[len(parts)-1]
		commandTag = "INSERT"
	}
	// There should be no affected rows attached to the tag, just return it
	if affectedRows == nil {
		return driver.RowsAffected(0), commandTag, nil
	}
	n, err := strconv.ParseInt(*affectedRows, 10, 64)
	if err != nil {
		cn.err.set(driver.ErrBadConn)
		return nil, "", fmt.Errorf("pq: could not parse commandTag: %w", err)
	}
	return driver.RowsAffected(n), commandTag, nil
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (cn *conn) sendBinaryParameters(b *writeBuf, args []driver.NamedValue) error {
	// Do one pass over the parameters to see if we're going to send any of them
	// over in binary. If we are, create a paramFormats array at the same time.
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
			datum, err := binaryEncode(x.Value)
			if err != nil {
				return err
			}
			b.int32(len(datum))
			b.bytes(datum)
		}
	}
	return nil
}

func (cn *conn) sendBinaryModeQuery(query string, args []driver.NamedValue) error {
	if len(args) >= 65536 {
		return fmt.Errorf("pq: got %d parameters but PostgreSQL only supports 65535 parameters", len(args))
	}

	b := cn.writeBuf(proto.Parse)
	b.byte(0) // unnamed statement
	b.string(query)
	b.int16(0)

	b.next(proto.Bind)
	b.int16(0) // unnamed portal and statement
	err := cn.sendBinaryParameters(b, args)
	if err != nil {
		return err
	}
	b.bytes(colFmtDataAllText)

	b.next(proto.Describe)
	b.byte(proto.Parse)
	b.byte(0) // unnamed portal

	b.next(proto.Execute)
	b.byte(0)
	b.int32(0)

	b.next(proto.Sync)
	return cn.send(b)
}

func (cn *conn) processParameterStatus(r *readBuf) {
	switch r.string() {
	default:
		// ignore
	case "padb_version":
		cn.parameterStatus.isRedshift = true
	case "server_version":
		var major1, major2 int
		_, err := fmt.Sscanf(r.string(), "%d.%d", &major1, &major2)
		if err == nil {
			cn.parameterStatus.serverVersion = major1*10000 + major2*100
		}
	case "TimeZone":
		switch tz := r.string(); tz {
		case "UTC", "Etc/UTC", "Etc/Universal", "Etc/Zulu", "Etc/UCT":
			cn.parameterStatus.currentLocation = time.UTC
		default:
			var err error
			cn.parameterStatus.currentLocation, err = time.LoadLocation(tz)
			if err != nil {
				cn.parameterStatus.currentLocation = nil
			}
		}
	// Use sql.NullBool so we can distinguish between false and not sent. If
	// it's not sent we use a query to get the value – I don't know when these
	// parameters are not sent, but this is what libpq does.
	case "in_hot_standby":
		b, err := pqutil.ParseBool(r.string())
		if err == nil {
			cn.parameterStatus.inHotStandby = sql.NullBool{Valid: true, Bool: b}
		}
	case "default_transaction_read_only":
		b, err := pqutil.ParseBool(r.string())
		if err == nil {
			cn.parameterStatus.defaultTransactionReadOnly = sql.NullBool{Valid: true, Bool: b}
		}
	}
}

func (cn *conn) processReadyForQuery(r *readBuf) {
	cn.txnStatus = transactionStatus(r.byte())
}

func (cn *conn) readReadyForQuery() error {
	t, r, err := cn.recv1()
	if err != nil {
		return err
	}
	switch t {
	case proto.ReadyForQuery:
		cn.processReadyForQuery(r)
		return nil
	case proto.ErrorResponse:
		err := parseError(r, "")
		cn.err.set(driver.ErrBadConn)
		return err
	default:
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected message %q; expected ReadyForQuery", t)
	}
}

func (cn *conn) readParseResponse() error {
	t, r, err := cn.recv1()
	if err != nil {
		return err
	}
	switch t {
	case proto.ParseComplete:
		return nil
	case proto.ErrorResponse:
		err := parseError(r, "")
		_ = cn.readReadyForQuery()
		return err
	default:
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected Parse response %q", t)
	}
}

func (cn *conn) readStatementDescribeResponse() (paramTyps []oid.Oid, colNames []string, colTyps []fieldDesc, _ error) {
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, nil, nil, err
		}
		switch t {
		case proto.ParameterDescription:
			nparams := r.int16()
			paramTyps = make([]oid.Oid, nparams)
			for i := range paramTyps {
				paramTyps[i] = r.oid()
			}
		case proto.NoData:
			return paramTyps, nil, nil, nil
		case proto.RowDescription:
			colNames, colTyps = parseStatementRowDescribe(r)
			return paramTyps, colNames, colTyps, nil
		case proto.ErrorResponse:
			err := parseError(r, "")
			_ = cn.readReadyForQuery()
			return nil, nil, nil, err
		default:
			cn.err.set(driver.ErrBadConn)
			return nil, nil, nil, fmt.Errorf("pq: unexpected Describe statement response %q", t)
		}
	}
}

func (cn *conn) readPortalDescribeResponse() (rowsHeader, error) {
	t, r, err := cn.recv1()
	if err != nil {
		return rowsHeader{}, err
	}
	switch t {
	case proto.RowDescription:
		return parsePortalRowDescribe(r), nil
	case proto.NoData:
		return rowsHeader{}, nil
	case proto.ErrorResponse:
		err := parseError(r, "")
		_ = cn.readReadyForQuery()
		return rowsHeader{}, err
	default:
		cn.err.set(driver.ErrBadConn)
		return rowsHeader{}, fmt.Errorf("pq: unexpected Describe response %q", t)
	}
}

func (cn *conn) readBindResponse() error {
	t, r, err := cn.recv1()
	if err != nil {
		return err
	}
	switch t {
	case proto.BindComplete:
		return nil
	case proto.ErrorResponse:
		err := parseError(r, "")
		_ = cn.readReadyForQuery()
		return err
	default:
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected Bind response %q", t)
	}
}

func (cn *conn) postExecuteWorkaround() error {
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
		t, r, err := cn.recv1()
		if err != nil {
			return err
		}
		switch t {
		case proto.ErrorResponse:
			err := parseError(r, "")
			_ = cn.readReadyForQuery()
			return err
		case proto.CommandComplete, proto.DataRow, proto.EmptyQueryResponse:
			// the query didn't fail, but we can't process this message
			return cn.saveMessage(t, r)
		default:
			cn.err.set(driver.ErrBadConn)
			return fmt.Errorf("pq: unexpected message during extended query execution: %q", t)
		}
	}
}

// Only for Exec(), since we ignore the returned data
func (cn *conn) readExecuteResponse(protocolState string) (res driver.Result, commandTag string, resErr error) {
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, "", err
		}
		switch t {
		case proto.CommandComplete:
			if resErr != nil {
				cn.err.set(driver.ErrBadConn)
				return nil, "", fmt.Errorf("pq: unexpected CommandComplete after error %s", resErr)
			}
			res, commandTag, err = cn.parseComplete(r.string())
			if err != nil {
				return nil, "", err
			}
		case proto.ReadyForQuery:
			cn.processReadyForQuery(r)
			if res == nil && resErr == nil {
				resErr = errUnexpectedReady
			}
			return res, commandTag, resErr
		case proto.ErrorResponse:
			resErr = parseError(r, "")
		case proto.RowDescription, proto.DataRow, proto.EmptyQueryResponse:
			if resErr != nil {
				cn.err.set(driver.ErrBadConn)
				return nil, "", fmt.Errorf("pq: unexpected %q after error %s", t, resErr)
			}
			if t == proto.EmptyQueryResponse {
				res = emptyRows
			}
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			return nil, "", fmt.Errorf("pq: unknown %s response: %q", protocolState, t)
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
