// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.
package pq

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Special bePid value issued on reconnection.
const Reconnected int = -1

// The minimum/initial back-off for reconnection.
const MinReconnectDelay time.Duration = 3 * time.Second

// The maximum back-off for reconnection.
const MaxReconnectDelay time.Duration = 15 * time.Minute

var errClosed = errors.New("listener closed")

type Notification struct {
	BePid   int
	RelName string
	Extra   string
}

func recvNotification(r *readBuf) Notification {
	bePid := r.int32()
	relname := r.string()
	extra := r.string()

	return Notification{bePid, relname, extra}
}

type message struct {
	typ byte
	buf *readBuf
}

type Listener struct {
	name      string
	cn        *conn
	closed    bool
	lock      *sync.Mutex
	channels  map[string]map[chan<- *Notification]bool
	replyChan chan message
}

func NewListener(name string) (*Listener, error) {
	cn, err := Open(name)

	if err != nil {
		return nil, err
	}

	l := &Listener{
		name,
		cn.(*conn),
		false,
		new(sync.Mutex),
		make(map[string]map[chan<- *Notification]bool),
		make(chan message)}

	go l.listen()

	return l, nil
}

func (l *Listener) recv2() (byte, *readBuf, error) {
	x := make([]byte, 5)
	_, err := io.ReadFull(l.cn.buf, x)
	if err != nil {
		if l.closed {
			// Listener.Close() called.
			return 0, nil, errClosed
		}

		return 0, nil, err
	}

	b := readBuf(x[1:])
	y := make([]byte, b.int32()-4)
	_, err = io.ReadFull(l.cn.buf, y)
	if err != nil {
		if l.closed {
			// Listener.Close() called.
			return 0, nil, errClosed
		}

		return x[0], nil, err
	}

	return x[0], (*readBuf)(&y), err
}

func (l *Listener) listen() {
	for {
		t, r, err := l.recv2()

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				l.cn = reconnect(l.name)
				err = l.relisten()

				if err != nil {
					// Just reconnect if LISTEN fails.
					continue
				}

				// Notify everyone that we have reconnected.
				for relname, chans := range l.channels {
					for ch := range chans {
						ch <- &Notification{Reconnected, relname, ""}
					}
				}

				continue
			} else if err == errClosed {
				return
			} else {
				panic(err)
			}
		}

		switch t {
		case 'A':
			n := recvNotification(r)
			l.dispatch(&n)
		default:
			l.replyChan <- message{t, r}
		}
	}
}

func (l *Listener) relisten() error {
	for relname := range l.channels {
		_, err := l.cn.simpleQuery("LISTEN " + quoteRelname(relname))
		if err != nil {
			return err
		}
	}

	return nil
}

func reconnect(name string) *conn {
	delay := MinReconnectDelay

	for {
		cn, err := Open(name)

		if err == nil {
			return cn.(*conn)
		}

		time.Sleep(delay)
		delay *= 2

		if delay > MaxReconnectDelay {
			delay = MaxReconnectDelay
		}
	}

	panic("not reached")
}

func (l *Listener) dispatch(n *Notification) {
	data, ok := l.channels[n.RelName]

	if ok {
		for ch := range data {
			ch <- n
		}
	}
}

func (l *Listener) Close() error {
	l.closed = true

	return l.cn.Close()
}

func (l *Listener) Listen(relname string, c chan<- *Notification) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	data, ok := l.channels[relname]

	if !ok {
		data = make(map[chan<- *Notification]bool, 1)
		l.channels[relname] = data
	}

	data[c] = true

	if len(data) == 1 {
		return l.simpleQuery2("LISTEN " + quoteRelname(relname))
	}

	return nil
}

func quoteRelname(relname string) string {
	return fmt.Sprintf(`"%s"`, strings.Replace(relname, `"`, `""`, -1))
}

func (l *Listener) simpleQuery2(q string) (err error) {
	defer errRecover(&err)

	b := newWriteBuf('Q')
	b.string(q)
	l.cn.send(b)

	for {
		m := <-l.replyChan
		t, r := m.typ, m.buf
		switch t {
		case 'C':
			// ignore
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

func (l *Listener) Unlisten(relname string, c chan<- *Notification) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	data, ok := l.channels[relname]

	if !ok {
		return nil
	}

	delete(data, c)

	if len(data) == 0 {
		err := l.simpleQuery2("UNLISTEN " + quoteRelname(relname))

		if err != nil {
			return err
		}

		delete(l.channels, relname)
	}

	return nil
}
