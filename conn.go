package pq

import (
	"crypto/md5"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
    "time"
)

var (
	ErrSSLNotSupported = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported    = errors.New("pq: this is postgres, a real database, this isn't a valid command")
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}

const WAIT_NOTIFICATION_CMD = "ACCEPT"
const MAX_NOTIFICATIONS = 10

type notification struct {
    channel string
    payload string
}

type conn struct {
	c     net.Conn
	namei int
    notifications chan *notification
}

func Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)

	o := make(Values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")

	// Default the username, but ignore errors, because a user
	// passed in via environment variable or connection string
	// would be okay.  This can result in connections failing
	// *sometimes* if the client relies on being able to determine
	// the current username and there are intermittent problems.
	u, err := user.Current()
	if err == nil {
		o.Set("user", u.Username)
	}

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	parseOpts(name, o)

	c, err := net.Dial(network(o))
	if err != nil {
		return nil, err
	}

    cn := &conn{c: c, notifications: make(chan *notification, MAX_NOTIFICATIONS)}
	cn.ssl(o)
	cn.startup(o)
	return cn, nil
}

func network(o Values) (string, string) {
	host := o.Get("host")

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o.Get("port"))
		return "unix", sockPath
	}

	return "tcp", host + ":" + o.Get("port")
}

type Values map[string]string

func (vs Values) Set(k, v string) {
	vs[k] = v
}

func (vs Values) Get(k string) (v string) {
	v, _ = vs[k]
	return
}

func parseOpts(name string, o Values) {
	if len(name) == 0 {
		return
	}

	ps := strings.Split(name, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			errorf("invalid option: %q", p)
		}
		o.Set(kv[0], kv[1])
	}
}

func (cn *conn) Begin() (driver.Tx, error) {
	st, err := cn.Prepare("BEGIN")
	if err != nil {
		return nil, err
	}

	_, err = st.Exec(nil)
	return cn, err
}

func (cn *conn) Commit() error {
	st, err := cn.Prepare("COMMIT")
	if err != nil {
		return err
	}

	_, err = st.Exec(nil)
	return err
}

func (cn *conn) Rollback() error {
	st, err := cn.Prepare("ROLLBACK")
	if err != nil {
		return err
	}

	_, err = st.Exec(nil)
	return err
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) prepareTo(q, stmtName string) (_ driver.Stmt, err error) {
	defer errRecover(&err)

    // If notification wait command, creates fake statement
    cmd_fields := strings.Fields(q)
    if len(cmd_fields) == 2 && (strings.ToUpper(cmd_fields[0]) == WAIT_NOTIFICATION_CMD) {
        timeout, err := strconv.ParseInt(cmd_fields[1], 10, 32)
        if err != nil {
            return nil, errors.New("Invalid wait notification command timeout")
        }
        st := &stmt{cn: cn, name: stmtName, notwait: true, nottimeout:int(timeout)}
		st.cols = make([]string, 2)
		st.ooid = make([]int, 2)
        st.cols[0] = "channel"
        st.cols[1] = "payload"
        st.ooid[0] = t_text
        st.ooid[1] = t_text
        return st, nil
    }

	st := &stmt{cn: cn, name: stmtName}

	b := newWriteBuf('P')
	b.string(st.name)
	b.string(q)
	b.int16(0)
	cn.send(b)

	b = newWriteBuf('D')
	b.byte('S')
	b.string(st.name)
	cn.send(b)

	cn.send(newWriteBuf('H'))

	t, r := cn.recv()
	if t != '1' {
		errorf("unexpected parse response: %q", t)
	}

	t, r = cn.recv()
	if t != 't' {
		errorf("unexpected describe params response: %q", t)
	}
	st.nparams = int(r.int16())

	t, r = cn.recv()
	switch t {
	case 'T':
		n := r.int16()
		st.cols = make([]string, n)
		st.ooid = make([]int, n)
		for i := range st.cols {
			st.cols[i] = r.string()
			r.next(6)
			st.ooid[i] = r.int32()
			r.next(8)
		}
	case 'n':
		// no data
	default:
		errorf("unexpected describe rows response: %q", t)
	}

	return st, nil
}

func (cn *conn) Prepare(q string) (_ driver.Stmt, err error) {
	st, err := cn.prepareTo(q, cn.gname())
	return st, err
}

func (cn *conn) Close() (err error) {
	defer errRecover(&err)
	cn.send(newWriteBuf('X'))

	return cn.c.Close()
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(
	query string, args []driver.Value) (_ driver.Result, err error) {
	defer errRecover(&err)

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

func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		t, r = cn.recv1()
		switch t {
		case 'E':
			panic(parseError(r))
		case 'N':
			// TODO(bmizerany): log notices?
		default:
			return
		}
	}

	panic("not reached")
}

func (cn *conn) recv1() (byte, *readBuf) {
	x := make([]byte, 5)
	_, err := io.ReadFull(cn.c, x)
	if err != nil {
		panic(err)
	}

	b := readBuf(x[1:])
	y := make([]byte, b.int32()-4)
	_, err = io.ReadFull(cn.c, y)
	if err != nil {
		panic(err)
	}

	return x[0], (*readBuf)(&y)
}

func (cn *conn) ssl(o Values) {
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

	w := newWriteBuf(0)
	w.int32(80877103)
	cn.send(w)

	b := make([]byte, 1)
	_, err := io.ReadFull(cn.c, b)
	if err != nil {
		panic(err)
	}

	if b[0] != 'S' {
		panic(ErrSSLNotSupported)
	}

	cn.c = tls.Client(cn.c, &tlsConf)
}

func (cn *conn) startup(o Values) {
	w := newWriteBuf(0)
	w.int32(196608)
	w.string("user")
	w.string(o.Get("user"))
	w.string("database")
	w.string(o.Get("dbname"))
	w.string("")
	cn.send(w)

	for {
		t, r := cn.recv()
		switch t {
		case 'K', 'S':
		case 'R':
			cn.auth(r, o)
		case 'Z':
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o Values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 5:
		s := string(r.next(4))
		w := newWriteBuf('p')
		w.string("md5" + md5s(md5s(o.Get("password")+o.Get("user"))+s))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication resoonse: %q", t)
		}
	default:
		errorf("unknown authentication response: %d", code)
	}
}

type stmt struct {
	cn          *conn
	name        string
	cols        []string
	nparams     int
	ooid        []int
	closed      bool
    notwait     bool
    nottimeout  int
    notcount    int
}

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}

    // Fake notification wait statement
    if st.notwait {
	    st.closed = true
        return nil
    }

	defer errRecover(&err)

	w := newWriteBuf('C')
	w.byte('S')
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(newWriteBuf('S'))

	t, _ := st.cn.recv()
	if t != '3' {
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, _ = st.cn.recv()
	if t != 'Z' {
		errorf("expected ready for query, but got: %q", t)
	}

	return nil
}

func (st *stmt) Query(v []driver.Value) (_ driver.Rows, err error) {
	defer errRecover(&err)
	st.exec(v)
	return &rows{st: st}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer errRecover(&err)
	st.exec(v)

	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C':
			res = parseComplete(r.string())
		case 'Z':
			// done
			return
		case 'D':
			errorf("unexpected data row returned in Exec; check your query")
		case 'S', 'N':
			// Ignore
        case 'A':
            parseNotificationResponse(st.cn, r)
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
    // Fake notification wait statement
    if st.notwait {
        return
    }

	w := newWriteBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for _, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(x)
			w.int32(len(b))
			w.bytes(b)
		}
	}
	w.int16(0)
	st.cn.send(w)

	w = newWriteBuf('E')
	w.string("")
	w.int32(0)
	st.cn.send(w)

	st.cn.send(newWriteBuf('S'))

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
			return
		case 'Z':
			if err != nil {
				panic(err)
			}
			return
		default:
			errorf("unexpected bind response: %q", t)
		}
	}
}

func (st *stmt) NumInput() int {
	return st.nparams
}

type result int64

func (i result) RowsAffected() (int64, error) {
	return int64(i), nil
}

func (i result) LastInsertId() (int64, error) {
	return 0, ErrNotSupported
}

func parseComplete(s string) driver.Result {
	parts := strings.Split(s, " ")
	n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return result(n)
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

    // Process fake notification wait statement
    if rs.st.notwait {
        // Return next received notification
        not := pollNotificationResponse(rs.st.cn)
        if not != nil {
            dest[0] = not.channel
            dest[1] = not.payload
            rs.st.notcount++
            return nil
        }
        // If at least one notification was received don't wait for timeout
        if rs.st.notcount > 0 {
            rs.done = true
			return io.EOF
        }
        // Blocks waiting for new notification or timeout
        not = readNotificationResponse(rs.st.cn, time.Duration(rs.st.nottimeout) * time.Millisecond)
        if not == nil {
            rs.done = true
			return io.EOF
        }
        dest[0] = not.channel
        dest[1] = not.payload
        rs.st.notcount++
        rs.done = true
        return nil
    }

	defer errRecover(&err)

	for {
		t, r := rs.st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C', 'S':
			continue
		case 'Z':
			rs.done = true
			if err != nil {
				return err
			}
			return io.EOF
		case 'D':
			n := r.int16()
			for i := 0; i < len(dest) && i < n; i++ {
				l := r.int32()
				if l == -1 {
					dest[i] = nil
					continue
				}
				dest[i] = decode(r.next(l), rs.st.ooid[i])
			}
			return
		default:
			errorf("unexpected message after execute: %q", t)
		}
	}

	panic("not reached")
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
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

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual, with omissions briefly
		// noted.
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			accrue("hostaddr")
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,
		// PGREALM
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL":
			accrue("requiressl")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLCRL":
			accrue("sslcrl")
		case "PGREQUIREPEER":
			accrue("requirepeer")
		case "PGKRBSRVNAME":
			accrue("krbsrvname")
		case "PGGSSLIB":
			accrue("gsslib")
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,
			// PGLOCALEDIR
		}
	}

	return out
}

// Parse received NotificationResponse and inserts into channel
func parseNotificationResponse(cn *conn, r *readBuf) {
    // Builds notification response structure
    nr := new(notification)
    r.next(4)   // skip pid
    nr.channel = r.string()
    nr.payload = r.string()
    // Insert notification response in channel without blocking
    select {
    case cn.notifications <- nr:
        return
    default:
        // Notification channel is full
        return
    }
}

// Get next notification response from channel or return nil (does not block)
func pollNotificationResponse(cn *conn) (not *notification) {
    select {
    case not := <-cn.notifications:
        return not
    default:
        return nil
    }
    panic("not reached")
}

// Return next notification in channel or waits notification till specified timeout
func readNotificationResponse(cn *conn, timeout time.Duration) (not *notification) {
    not = pollNotificationResponse(cn)
    if not != nil {
        return not
    }
    cn.c.SetReadDeadline(time.Now().Add(timeout))
    defer timeoutRecover(not)
    t, r := cn.recv1()
    cn.c.SetReadDeadline(time.Time{})
    if t != 'A' {
        return nil
    }
    parseNotificationResponse(cn, r)
    return pollNotificationResponse(cn)
}

// Recover from socket read timeout error or panic for other errors
func timeoutRecover(not *notification) {
	e := recover()
	switch v := e.(type) {
	case nil:
		// Do nothing
	case *net.OpError:
        if v.Timeout() {
            not = nil
            return
        }
        panic(e)
	default:
        panic(e)
	}
}

