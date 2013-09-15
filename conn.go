// Package pq is a pure Go Postgres driver for the database/sql package.
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
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

var (
	ErrSSLNotSupported = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported    = errors.New("pq: invalid command")
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}

type conn struct {
	c       net.Conn
	buf     *bufio.Reader
	namei   int
	scratch [512]byte
}

func (c *conn) writeBuf(b byte) *writeBuf {
	c.scratch[0] = b
	w := writeBuf(c.scratch[:5])
	return &w
}

func Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)
	defer errRecoverWithPGReason(&err)

	o := make(Values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	if err := parseOpts(name, o); err != nil {
		return nil, err
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

	c, err := net.Dial(network(o))
	if err != nil {
		return nil, err
	}

	cn := &conn{c: c}
	cn.ssl(o)
	cn.buf = bufio.NewReader(cn.c)
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
	return vs[k]
}

type scanner struct {
	s []rune
	i int
}

func NewScanner(s string) *scanner {
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

func parseOpts(name string, o Values) error {
	s := NewScanner(name)

top:
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
				break top
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			if r, ok = s.SkipSpaces(); !ok {
				break
			}
		}

		// The current character should be =
		if r != '=' {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			break top
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r != '\\' {
					valRunes = append(valRunes, r)
				}

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
				case '\\':
					continue
				case '\'':
					break quote
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o.Set(string(keyRunes), string(valRunes))
	}

	return nil
}

func (cn *conn) Begin() (driver.Tx, error) {
	_, err := cn.Exec("BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return cn, err
}

func (cn *conn) Commit() error {
	_, err := cn.Exec("COMMIT", nil)
	return err
}

func (cn *conn) Rollback() error {
	_, err := cn.Exec("ROLLBACK", nil)
	return err
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(q string) (res driver.Result, err error) {
	defer errRecover(&err)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			res = parseComplete(r.string())
		case 'Z':
			// done
			return
		case 'E':
			err = parseError(r)
		case 'T', 'N', 'S', 'D':
			// ignore
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
		case 'C', 'N':
			// Ignore--we may need to consume Complete
			// here if the query finishes with an error,
			// and NoticeResponse if we get a notice (we
			// should communicate these rather than
			// ignoring them, but there's not a great way
			// to expose them right now)
		case 'Z':
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
		case '1', '2', 'N':
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
			return st, err
		case 'E':
			err = parseError(r)
		case 'C':
			// command complete
			return st, err
		default:
			errorf("unexpected describe rows response: %q", t)
		}
	}

	panic("not reached")
}

func (cn *conn) Prepare(q string) (driver.Stmt, error) {
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	defer errRecover(&err)
	cn.send(cn.writeBuf('X'))

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
		return cn.simpleExec(query)
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

func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		t, r = cn.recv1()
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

func (cn *conn) recv1() (byte, *readBuf) {
	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		panic(err)
	}
	c := x[0]

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
		panic(err)
	}

	return c, (*readBuf)(&y)
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

func (cn *conn) startup(o Values) {
	w := cn.writeBuf(0)
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

func (st *stmt) Query(v []driver.Value) (r driver.Rows, err error) {
	return st.cn.Query(st.query, v)
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer errRecover(&err)

	if len(v) == 0 {
		return st.cn.simpleExec(st.query)
	}
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
		case 'T', 'N', 'S', 'D':
			// Ignore
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
	w := st.cn.writeBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for i, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(x, st.paramTyps[i])
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
			return
		case 'Z':
			if err != nil {
				panic(err)
			}
			return
		case 'N':
			// ignore
		default:
			errorf("unexpected bind response: %q", t)
		}
	}
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

func parseComplete(s string) driver.Result {
	parts := strings.Split(s, " ")
	n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return driver.RowsAffected(n)
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

	for {
		t, r := rs.st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C', 'S', 'N':
			continue
		case 'Z':
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
				dest[i] = decode(r.next(l), rs.st.rowTyps[i])
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
		mustBe := func(expected string) {
			if parts[1] != expected {
				panic(fmt.Sprintf("setting %v must be absent or %v; got %v",
					parts[0], expected, parts[1]))
			}
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
			accrue("hostaddr")
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
			mustBeUtf8(parts[1])
		case "PGDATESTYLE":
			mustBe("ISO, MDY")
		case "PGTZ", "PGGEQO", "PGSYSCONFDIR", "PGLOCALEDIR":
			unsupported()
		}
	}

	return out
}

var notUTF8re = regexp.MustCompile("[^a-zA-Z0-9]")

func mustBeUtf8(value string) {
	// Recognize all sorts of silly things as utf-8, like Postgres does
	encoding := notUTF8re.ReplaceAllLiteralString(strings.ToLower(value),
		"")
	if encoding != "utf8" && encoding != "unicode" {
		panic("setting PGCLIENT_ENCODING must be absent or 'UTF8'")
	}
}
