package pq

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lib/pq/oid"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Common error types
var (
	ErrSSLNotSupported     = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported        = errors.New("pq: Unsupported command")
	ErrInFailedTransaction = errors.New("pq: Could not complete operation in a failed transaction")
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

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

type conn struct {
	c         net.Conn
	buf       *bufio.Reader
	namei     int
	scratch   [512]byte
	txnStatus transactionStatus

	parameterStatus parameterStatus

	saveMessageType   byte
	saveMessageBuffer *readBuf
}

func (c *conn) writeBuf(b byte) *writeBuf {
	c.scratch[0] = b
	w := writeBuf(c.scratch[:5])
	return &w
}

func Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)

	o := make(values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")
	// N.B.: Extra float digits should be set to 3, but that breaks
	// Postgres 8.4 and older, where the max is 2.
	o.Set("extra_float_digits", "2")
	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	if strings.HasPrefix(name, "postgres://") {
		name, err = ParseURL(name)
		if err != nil {
			return nil, err
		}
	}

	if err := parseOpts(name, o); err != nil {
		return nil, err
	}

	// Use the "fallback" application name if necessary
	if fallback := o.Get("fallback_application_name"); fallback != "" {
		if !o.Isset("application_name") {
			o.Set("application_name", fallback)
		}
	}
	o.Unset("fallback_application_name")

	// We can't work with any client_encoding other than UTF-8 currently.
	// However, we have historically allowed the user to set it to UTF-8
	// explicitly, and there's no reason to break such programs, so allow that.
	// Note that the "options" setting could also set client_encoding, but
	// parsing its value is not worth it.  Instead, we always explicitly send
	// client_encoding as a separate run-time parameter, which should override
	// anything set in options.
	if enc := o.Get("client_encoding"); enc != "" && !isUTF8(enc) {
		return nil, errors.New("client_encoding must be absent or 'UTF8'")
	}
	o.Set("client_encoding", "UTF8")
	// DateStyle needs a similar treatment.
	if datestyle := o.Get("datestyle"); datestyle != "" {
		if datestyle != "ISO, MDY" {
			panic(fmt.Sprintf("setting datestyle must be absent or %v; got %v",
				"ISO, MDY", datestyle))
		}
	} else {
		o.Set("datestyle", "ISO, MDY")
	}

	// If a user is not provided by any other means, the last
	// resort is to use the current operating system provided user
	// name.
	if o.Get("user") == "" {
		u, err := userCurrent()
		if err != nil {
			return nil, err
		} else {
			o.Set("user", u)
		}
	}

	c, err := dial(o)
	if err != nil {
		return nil, err
	}

	cn := &conn{c: c}
	cn.ssl(o)
	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)
	// reset the deadline, in case one was set (see dial)
	err = cn.c.SetDeadline(time.Time{})
	return cn, err
}

func dial(o values) (net.Conn, error) {
	ntw, addr := network(o)

	timeout := o.Get("connect_timeout")
	// Ensure the option will not be sent.
	o.Unset("connect_timeout")

	// Zero or not specified means wait indefinitely.
	if timeout != "" && timeout != "0" {
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
		conn, err := net.DialTimeout(ntw, addr, duration)
		if err != nil {
			return nil, err
		}
		err = conn.SetDeadline(deadline)
		return conn, err
	}
	return net.Dial(ntw, addr)
}

func network(o values) (string, string) {
	host := o.Get("host")

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o.Get("port"))
		return "unix", sockPath
	}

	return "tcp", host + ":" + o.Get("port")
}

type values map[string]string

func (vs values) Set(k, v string) {
	vs[k] = v
}

func (vs values) Get(k string) (v string) {
	return vs[k]
}

func (vs values) Isset(k string) bool {
	_, ok := vs[k]
	return ok
}

func (vs values) Unset(k string) {
	delete(vs, k)
}

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// parseOpts parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parseOpts(name string, o values) error {
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o.Set(string(keyRunes), "")
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o.Set(string(keyRunes), string(valRunes))
	}

	return nil
}

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction ||
		cn.txnStatus == txnStatusInFailedTransaction
}

func (cn *conn) checkIsInTransaction(intxn bool) {
	if cn.isInTransaction() != intxn {
		errorf("unexpected transaction status %v", cn.txnStatus)
	}
}

func (cn *conn) Begin() (_ driver.Tx, err error) {
	defer errRecover(&err)

	cn.checkIsInTransaction(false)
	_, commandTag, err := cn.simpleExec("BEGIN")
	if err != nil {
		return nil, err
	}
	if commandTag != "BEGIN" {
		return nil, fmt.Errorf("unexpected command tag %s", commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		return nil, fmt.Errorf("unexpected transaction status %v", cn.txnStatus)
	}
	return cn, nil
}

func (cn *conn) Commit() (err error) {
	defer errRecover(&err)

	cn.checkIsInTransaction(true)
	// We don't want the client to think that everything is okay if it tries
	// to commit a failed transaction.  However, no matter what we return,
	// database/sql will release this connection back into the free connection
	// pool so we have to abort the current transaction here.  Note that you
	// would get the same behaviour if you issued a COMMIT in a failed
	// transaction, so it's also the least surprising thing to do here.
	if cn.txnStatus == txnStatusInFailedTransaction {
		if err := cn.Rollback(); err != nil {
			return err
		}
		return ErrInFailedTransaction
	}

	_, commandTag, err := cn.simpleExec("COMMIT")
	if err != nil {
		return err
	}
	if commandTag != "COMMIT" {
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) Rollback() (err error) {
	defer errRecover(&err)

	cn.checkIsInTransaction(true)
	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
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
	defer errRecover(&err)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			res, commandTag = parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			// done
			return
		case 'E':
			err = parseError(r)
		case 'T', 'D':
			// ignore any results
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) simpleQuery(q string) (res driver.Rows, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: "", query: q}

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			// We allow queries which don't return any results through Query as
			// well as Exec.  We still have to give database/sql a rows object
			// the user can close, though, to avoid connections from being
			// leaked.  A "rows" with done=true works fine for that purpose.
			if err != nil {
				errorf("unexpected CommandComplete in simple query execution")
			}
			res = &rows{st: st, done: true}
		case 'Z':
			cn.processReadyForQuery(r)
			// done
			return
		case 'E':
			res = nil
			err = parseError(r)
		case 'T':
			res = &rows{st: st}
			st.cols, st.rowTyps = parseMeta(r)
			// After we get the meta, we want to kick out to Next()
			return
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) prepareTo(q, stmtName string) (_ driver.Stmt, err error) {
	return cn.prepareToSimpleStmt(q, stmtName)
}

func (cn *conn) prepareToSimpleStmt(q, stmtName string) (_ *stmt, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: stmtName, query: q}

	b := cn.writeBuf('P')
	b.string(st.name)
	b.string(q)
	b.int16(0)
	cn.send(b)

	b = cn.writeBuf('D')
	b.byte('S')
	b.string(st.name)
	cn.send(b)

	cn.send(cn.writeBuf('S'))

	for {
		t, r := cn.recv1()
		switch t {
		case '1':
		case 't':
			nparams := int(r.int16())
			st.paramTyps = make([]oid.Oid, nparams)

			for i := range st.paramTyps {
				st.paramTyps[i] = r.oid()
			}
		case 'T':
			st.cols, st.rowTyps = parseMeta(r)
		case 'n':
			// no data
		case 'Z':
			cn.processReadyForQuery(r)
			return st, err
		case 'E':
			err = parseError(r)
		default:
			errorf("unexpected describe rows response: %q", t)
		}
	}

	panic("not reached")
}

func (cn *conn) Prepare(q string) (driver.Stmt, error) {
	if len(q) >= 4 && strings.EqualFold(q[:4], "COPY") {
		return cn.prepareCopyIn(q)
	}
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	defer errRecover(&err)

	// Don't go through send(); ListenerConn relies on us not scribbling on the
	// scratch buffer of this connection.
	err = cn.sendSimpleMessage('X')
	if err != nil {
		return err
	}

	return cn.c.Close()
}

// Implement the "Queryer" interface
func (cn *conn) Query(query string, args []driver.Value) (_ driver.Rows, err error) {
	defer errRecover(&err)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	st, err := cn.prepareToSimpleStmt(query, "")
	if err != nil {
		panic(err)
	}

	st.exec(args)
	return &rows{st: st}, nil
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	defer errRecover(&err)

	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		return r, err
	}

	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st, err := cn.prepareTo(query, "")
	if err != nil {
		panic(err)
	}

	r, err := st.Exec(args)
	if err != nil {
		panic(err)
	}

	return r, err
}

// Assumes len(*m) is > 5
func (cn *conn) send(m *writeBuf) {
	b := (*m)[1:]
	binary.BigEndian.PutUint32(b, uint32(len(b)))

	if (*m)[0] == 0 {
		*m = b
	}

	_, err := cn.c.Write(*m)
	if err != nil {
		panic(err)
	}
}

// Send a message of type typ to the server on the other end of cn.  The
// message should have no payload.  This method does not use the scratch
// buffer.
func (cn *conn) sendSimpleMessage(typ byte) (err error) {
	_, err = cn.c.Write([]byte{typ, '\x00', '\x00', '\x00', '\x04'})
	return err
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage() (byte, *readBuf, error) {
	// workaround for a QueryRow bug, see exec
	if cn.saveMessageType != 0 {
		t, r := cn.saveMessageType, cn.saveMessageBuffer
		cn.saveMessageType = 0
		cn.saveMessageBuffer = nil
		return t, r, nil
	}

	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		return 0, nil, err
	}
	t := x[0]

	b := readBuf(x[1:])
	n := b.int32() - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		return 0, nil, err
	}

	return t, (*readBuf)(&y), nil
}

// recv receives a message from the backend, but if an error happened while
// reading the message or the received message was an ErrorResponse, it panics.
// NoticeResponses are ignored.  This function should generally be used only
// during the startup sequence.
func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		var err error
		t, r, err = cn.recvMessage()
		if err != nil {
			panic(err)
		}

		switch t {
		case 'E':
			panic(parseError(r))
		case 'N':
			// ignore
		default:
			return
		}
	}

	panic("not reached")
}

// recv1 receives a message from the backend, panicking if an error occurs
// while attempting to read it.  All asynchronous messages are ignored, with
// the exception of ErrorResponse.
func (cn *conn) recv1() (t byte, r *readBuf) {
	for {
		var err error
		t, r, err = cn.recvMessage()
		if err != nil {
			panic(err)
		}

		switch t {
		case 'A', 'N':
			// ignore
		case 'S':
			cn.processParameterStatus(r)
		default:
			return
		}
	}

	panic("not reached")
}

func (cn *conn) ssl(o values) {
	tlsConf := tls.Config{}
	switch mode := o.Get("sslmode"); mode {
	case "require", "":
		tlsConf.InsecureSkipVerify = true
	case "verify-full":
		// fall out
	case "disable":
		return
	default:
		errorf(`unsupported sslmode %q; only "require" (default), "verify-full", and "disable" supported`, mode)
	}

	w := cn.writeBuf(0)
	w.int32(80877103)
	cn.send(w)

	b := cn.scratch[:1]
	_, err := io.ReadFull(cn.c, b)
	if err != nil {
		panic(err)
	}

	if b[0] != 'S' {
		panic(ErrSSLNotSupported)
	}

	cn.c = tls.Client(cn.c, &tlsConf)
}

func (cn *conn) startup(o values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	// Send the backend the name of the database we want to connect to, and the
	// user we want to connect as.  Additionally, we send over any run-time
	// parameters potentially included in the connection string.  If the server
	// doesn't recognize any of them, it will reply with an error.
	for k, v := range o {
		// skip options which can't be run-time parameters
		if k == "password" || k == "host" ||
			k == "port" || k == "sslmode" {
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
	cn.send(w)

	for {
		t, r := cn.recv()
		switch t {
		case 'K':
		case 'S':
			cn.processParameterStatus(r)
		case 'R':
			cn.auth(r, o)
		case 'Z':
			cn.processReadyForQuery(r)
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeBuf('p')
		w.string(o.Get("password"))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeBuf('p')
		w.string("md5" + md5s(md5s(o.Get("password")+o.Get("user"))+s))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	default:
		errorf("unknown authentication response: %d", code)
	}
}

type stmt struct {
	cn        *conn
	name      string
	query     string
	cols      []string
	rowTyps   []oid.Oid
	paramTyps []oid.Oid
	closed    bool
	lasterr   error
}

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}

	defer errRecover(&err)

	w := st.cn.writeBuf('C')
	w.byte('S')
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	t, _ := st.cn.recv1()
	if t != '3' {
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, r := st.cn.recv1()
	if t != 'Z' {
		errorf("expected ready for query, but got: %q", t)
	}
	st.cn.processReadyForQuery(r)

	return nil
}

func (st *stmt) Query(v []driver.Value) (r driver.Rows, err error) {
	defer errRecover(&err)
	st.exec(v)
	return &rows{st: st}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer errRecover(&err)

	if len(v) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := st.cn.simpleExec(st.query)
		return r, err
	}
	st.exec(v)

	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C':
			res, _ = parseComplete(r.string())
		case 'Z':
			st.cn.processReadyForQuery(r)
			// done
			return
		case 'T', 'D':
			// ignore any results
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
	if len(v) != len(st.paramTyps) {
		errorf("got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	w := st.cn.writeBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for i, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(&st.cn.parameterStatus, x, st.paramTyps[i])
			w.int32(len(b))
			w.bytes(b)
		}
	}
	w.int16(0)
	st.cn.send(w)

	w = st.cn.writeBuf('E')
	w.string("")
	w.int32(0)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	var err error
	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case '2':
			if err != nil {
				panic(err)
			}
			goto workaround
		case 'Z':
			st.cn.processReadyForQuery(r)
			if err != nil {
				panic(err)
			}
			return
		default:
			errorf("unexpected bind response: %q", t)
		}
	}

	// Work around a bug in sql.DB.QueryRow: in Go 1.2 and earlier it ignores
	// any errors from rows.Next, which masks errors that happened during the
	// execution of the query.  To avoid the problem in common cases, we wait
	// here for one more message from the database.  If it's not an error the
	// query will likely succeed (or perhaps has already, if it's a
	// CommandComplete), so we push the message into the conn struct; recv1
	// will return it as the next message for rows.Next or rows.Close.
	// However, if it's an error, we wait until ReadyForQuery and then return
	// the error to our caller.
workaround:
	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C', 'D':
			// the query didn't fail, but we can't process this message
			st.cn.saveMessageType = t
			st.cn.saveMessageBuffer = r
			return
		case 'Z':
			if err == nil {
				errorf("unexpected ReadyForQuery during extended query execution")
			}
			panic(err)
		default:
			errorf("unexpected message during query execution: %q", t)
		}
	}
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

// parseComplete parses the "command tag" from a CommandComplete message, and
// returns the number of rows affected (if applicable) and a string
// identifying only the command that was executed, e.g. "ALTER TABLE".  If the
// command tag could not be parsed, parseComplete panics.
func parseComplete(commandTag string) (driver.Result, string) {
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
	// INSERT also includes the oid of the inserted row in its command tag.
	// Oids in user tables are deprecated, and the oid is only returned when
	// exactly one row is inserted, so it's unlikely to be of value to any
	// real-world application and we can ignore it.
	if affectedRows == nil && strings.HasPrefix(commandTag, "INSERT ") {
		parts := strings.Split(commandTag, " ")
		if len(parts) != 3 {
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
		errorf("could not parse commandTag: %s", err)
	}
	return driver.RowsAffected(n), commandTag
}

type rows struct {
	st   *stmt
	done bool
}

func (rs *rows) Close() error {
	for {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			return nil
		default:
			return err
		}
	}
	panic("not reached")
}

func (rs *rows) Columns() []string {
	return rs.st.cols
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	if rs.done {
		return io.EOF
	}

	if rs.st.lasterr != nil {
		return rs.st.lasterr
	}

	defer errRecover(&err)

	conn := rs.st.cn
	for {
		t, r := conn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C':
			continue
		case 'Z':
			conn.processReadyForQuery(r)
			rs.done = true
			if err != nil {
				return err
			}
			return io.EOF
		case 'D':
			n := r.int16()
			if n < len(dest) {
				dest = dest[:n]
			}
			for i := range dest {
				l := r.int32()
				if l == -1 {
					dest[i] = nil
					continue
				}
				dest[i] = decode(&conn.parameterStatus, r.next(l), rs.st.rowTyps[i])
			}
			return
		default:
			errorf("unexpected message after execute: %q", t)
		}
	}

	panic("not reached")
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", pq.QuoteIdentifier(tblname)), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (c *conn) processParameterStatus(r *readBuf) {
	var err error

	param := r.string()
	switch param {
	case "server_version":
		var major1 int
		var major2 int
		var minor int
		_, err = fmt.Sscanf(r.string(), "%d.%d.%d", &major1, &major2, &minor)
		if err == nil {
			c.parameterStatus.serverVersion = major1*10000 + major2*100 + minor
		}

	case "TimeZone":
		c.parameterStatus.currentLocation, err = time.LoadLocation(r.string())
		if err != nil {
			c.parameterStatus.currentLocation = nil
		}

	default:
		// ignore
	}
}

func (c *conn) processReadyForQuery(r *readBuf) {
	c.txnStatus = transactionStatus(r.byte())
}

func parseMeta(r *readBuf) (cols []string, rowTyps []oid.Oid) {
	n := r.int16()
	cols = make([]string, n)
	rowTyps = make([]oid.Oid, n)
	for i := range cols {
		cols[i] = r.string()
		r.next(6)
		rowTyps[i] = r.oid()
		r.next(8)
	}
	return
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}
		unsupported := func() {
			panic(fmt.Sprintf("setting %v not supported", parts[0]))
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual. Unsupported but well-defined
		// keys cause a panic; these should be unset prior to
		// execution. Options which pq expects to be set to a
		// certain value are allowed, but must be set to that
		// value if present (they can, of course, be absent).
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			unsupported()
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		case "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE", "PGREALM":
			unsupported()
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL", "PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT", "PGSSLCRL":
			unsupported()
		case "PGREQUIREPEER":
			unsupported()
		case "PGKRBSRVNAME", "PGGSSLIB":
			unsupported()
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
		case "PGDATESTYLE":
			accrue("datestyle")
		case "PGTZ":
			accrue("timezone")
		case "PGGEQO":
			accrue("geqo")
		case "PGSYSCONFDIR", "PGLOCALEDIR":
			unsupported()
		}
	}

	return out
}

// isUTF8 returns whether name is a fuzzy variation of the string "UTF-8".
func isUTF8(name string) bool {
	// Recognize all sorts of silly things as "UTF-8", like Postgres does
	s := strings.Map(alnumLowerASCII, name)
	return s == "utf8" || s == "unicode"
}

func alnumLowerASCII(ch rune) rune {
	if 'A' <= ch && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	if 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' {
		return ch
	}
	return -1 // discard
}
