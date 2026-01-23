package pqtest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type Fake struct {
	l net.Listener
	t testing.TB
}

// NewFake creates a new "fake" PostgreSQL server. You need to accept
// connections with [Fake.Accept].
//
// This can also be tested against libpq with something like:
//
//	f := pqtest.NewFake(t)
//	f.Accept(..)
//
//	fmt.Println("\n" + f.DSN())
//	time.Sleep(9 * time.Minute)
func NewFake(t testing.TB) Fake {
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	return Fake{l: l, t: t}
}

// DSN is the DSN to connect to for this server.
func (f Fake) DSN() string {
	h, p, err := net.SplitHostPort(f.l.Addr().String())
	if err != nil {
		f.t.Fatal(err)
	}
	return "host=" + h + " port=" + p
}

// Host returns the hostname for this server.
func (f Fake) Host() string {
	h, _, err := net.SplitHostPort(f.l.Addr().String())
	if err != nil {
		f.t.Fatal(err)
	}
	return h
}

// Port returns the port for this server.
func (f Fake) Port() string {
	_, p, err := net.SplitHostPort(f.l.Addr().String())
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

// Accept callback for new connections.
func (f Fake) Accept(fun func(net.Conn)) {
	go func() {
		for {
			cn, err := f.l.Accept()
			if err != nil {
				f.t.Errorf("accepting connection: %s", err)
				return
			}
			go fun(cn)
		}
	}()
}

// Startup reads the startup message from the server with [f.ReadStartup] and
// sends [proto.AuthenticationRequest] and [proto.ReadyForQuery].
func (f Fake) Startup(cn net.Conn, params map[string]string) {
	if _, ok := f.ReadStartup(cn); !ok {
		return
	}
	// Technically we don't *need* to send the AuthRequest, but the psql CLI
	// expects it.
	f.WriteMsg(cn, proto.AuthenticationRequest, "\x00\x00\x00\x00")
	if len(params) > 0 {
		f.WriteStartup(cn, params)
	}
	f.WriteMsg(cn, proto.ReadyForQuery, "I")
}

// ReadStartup reads the startup message.
func (f Fake) ReadStartup(cn net.Conn) (map[string]string, bool) {
	_, msg, ok := f.read(cn, true)
	var (
		params = make(map[string]string)
		m      = strings.Split(string(msg[4:len(msg)-2]), "\x00")
	)
	for i := 0; i < len(m); i += 2 {
		params[m[i]] = m[i+1]
	}
	return params, ok
}

// WriteStartup writes startup parameters.
func (f Fake) WriteStartup(cn net.Conn, params map[string]string) {
	for k, v := range params {
		f.WriteMsg(cn, proto.ParameterStatus, fmt.Sprintf("%s\x00%s\x00", k, v))
	}
}

// ReadMsg reads a message from the client (frontend).
func (f Fake) ReadMsg(cn net.Conn) (proto.RequestCode, []byte, bool) {
	return f.read(cn, false)
}

func (f Fake) read(cn net.Conn, startup bool) (proto.RequestCode, []byte, bool) {
	// Startup message has no code and only a length (herp derp).
	sz := 5
	if startup {
		sz = 4
	}
	typ := make([]byte, sz)
	_, err := cn.Read(typ)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, false
		}
		f.t.Errorf("reading: %s", err)
		return 0, nil, false
	}

	var (
		code   = proto.RequestCode(typ[0])
		length = typ[1:]
	)
	if startup {
		code = 0
		length = typ
	}

	data := make([]byte, int(binary.BigEndian.Uint32(length))-4)
	_, err = cn.Read(data)
	if err != nil {
		f.t.Errorf("reading: %s", err)
		return 0, nil, false
	}
	return code, data, true
}

// WriteMsg writes a message to the client (frontend).
func (f Fake) WriteMsg(cn net.Conn, code proto.ResponseCode, msg string) {
	l := []byte{byte(code), 0, 0, 0, 0}
	binary.BigEndian.PutUint32(l[1:], uint32(len(msg)+4))
	_, err := cn.Write(append(l, msg...))
	if err != nil {
		f.t.Error(err)
	}
}

// SimpleQuery responds to a simpleQuery workflow; values are as a col, value
// pair:
//
//	f.SimpleQuery(cn, "SELECT",
//		"colname", "val",
//		"int", 2)
//
// Currently only supports string, int, and bool for values
func (f Fake) SimpleQuery(cn net.Conn, tag string, values ...any) {
	if len(values)%2 != 0 {
		f.t.Fatal("values not % 2")
	}
	var (
		cols = make([]string, 0, len(values)/2)
		vals = make([]any, 0, len(values)/2)
	)
	for i := 0; i < len(values); i += 2 {
		s, ok := values[i].(string)
		if !ok {
			f.t.Fatalf("column name is not a string: %T %[1]v", values[i])
		}
		cols, vals = append(cols, s), append(vals, values[i+1])
	}

	b := make([]byte, 0, 64)
	b = binary.BigEndian.AppendUint16(b, uint16(len(cols)))
	for i, c := range cols {
		var (
			l = math.MaxUint16
			o oid.Oid
		)
		// TODO: would be nice if there's a helper method in the oid package to
		// get the oid from Go type. Need to look at this package in general.
		switch v := vals[i].(type) {
		case bool:
			l, o = 1, oid.T_bool
		case int:
			o = oid.T_int4
		case string:
			l, o = len(v), oid.T_text
		default:
			f.t.Fatalf("value type not supported: %T %[1]v", c)
			return
		}

		b = append(b, c...)                             // colname
		b = append(b, 0)                                // end string
		b = append(b, 0, 0, 0, 0, 0, 0)                 // table and column oid, not used
		b = binary.BigEndian.AppendUint32(b, uint32(o)) // data oid
		b = binary.BigEndian.AppendUint16(b, uint16(l)) // len
		b = append(b, 0xff, 0xff, 0xff, 0xff)           // atttypmod
		b = append(b, 0, 0)                             // format
	}
	f.WriteMsg(cn, proto.RowDescription, string(b))

	b = b[:0]
	b = binary.BigEndian.AppendUint16(b, uint16(len(cols)))
	for _, v := range vals {
		var s string
		switch vv := v.(type) {
		case bool:
			s = "f"
			if vv {
				s = "t"
			}
		case int:
			s = strconv.Itoa(vv)
		case string:
			s = vv
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(s))) // value len
		b = append(b, s...)                                  // and the value
	}
	f.WriteMsg(cn, proto.DataRow, string(b))

	f.WriteMsg(cn, proto.CommandComplete, tag+"\x00")
}
