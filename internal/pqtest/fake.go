package pqtest

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/lib/pq/internal/proto"
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
func (f Fake) Startup(cn net.Conn) {
	if !f.ReadStartup(cn) {
		return
	}
	// Technically we don't *need* to send the AuthRequest, but the psql CLI
	// expects it.
	f.WriteMsg(cn, proto.AuthenticationRequest, 0, 0, 0, 0)
	f.WriteMsg(cn, proto.ReadyForQuery, 'I')
}

// ReadStartup reads the startup message.
func (f Fake) ReadStartup(cn net.Conn) bool {
	_, _, ok := f.read(cn, true)
	return ok
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
func (f Fake) WriteMsg(cn net.Conn, code proto.ResponseCode, msg ...byte) {
	l := []byte{byte(code), 0, 0, 0, 0}
	binary.BigEndian.PutUint32(l[1:], uint32(len(msg)+4))
	_, err := cn.Write(append(l, msg...))
	if err != nil {
		f.t.Error(err)
	}
}
