package pq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq/internal/proto"
)

// Notification represents a single notification from the database.
type Notification struct {
	BePid   int    // Process ID (PID) of the notifying postgres backend.
	Channel string // Name of the channel the notification was sent on.
	Extra   string // Payload, or the empty string if unspecified.
}

func recvNotification(r *readBuf) *Notification {
	bePid := r.int32()
	channel := r.string()
	extra := r.string()
	return &Notification{bePid, channel, extra}
}

// SetNotificationHandler sets the given notification handler on the given
// connection. A runtime panic occurs if c is not a pq connection. A nil handler
// may be used to unset it.
//
// Note: Notification handlers are executed synchronously by pq meaning commands
// won't continue to be processed until the handler returns.
func SetNotificationHandler(c driver.Conn, handler func(*Notification)) {
	c.(*conn).notificationHandler = handler
}

// NotificationHandlerConnector wraps a regular connector and sets a
// notification handler on it.
type NotificationHandlerConnector struct {
	driver.Connector
	notificationHandler func(*Notification)
}

// Connect calls the underlying connector's connect method and then sets the
// notification handler.
func (n *NotificationHandlerConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := n.Connector.Connect(ctx)
	if err == nil {
		SetNotificationHandler(c, n.notificationHandler)
	}
	return c, err
}

// ConnectorNotificationHandler returns the currently set notification handler,
// if any. If the given connector is not a result of
// [ConnectorWithNotificationHandler], nil is returned.
func ConnectorNotificationHandler(c driver.Connector) func(*Notification) {
	if c, ok := c.(*NotificationHandlerConnector); ok {
		return c.notificationHandler
	}
	return nil
}

// ConnectorWithNotificationHandler creates or sets the given handler for the
// given connector. If the given connector is a result of calling this function
// previously, it is simply set on the given connector and returned. Otherwise,
// this returns a new connector wrapping the given one and setting the
// notification handler. A nil notification handler may be used to unset it.
//
// The returned connector is intended to be used with database/sql.OpenDB.
//
// Note: Notification handlers are executed synchronously by pq meaning commands
// won't continue to be processed until the handler returns.
func ConnectorWithNotificationHandler(c driver.Connector, handler func(*Notification)) *NotificationHandlerConnector {
	if c, ok := c.(*NotificationHandlerConnector); ok {
		c.notificationHandler = handler
		return c
	}
	return &NotificationHandlerConnector{Connector: c, notificationHandler: handler}
}

const (
	connStateIdle int32 = iota
	connStateExpectResponse
	connStateExpectReadyForQuery
)

type message struct {
	typ proto.ResponseCode
	err error
}

type listenerEvent struct {
	typ ListenerEventType
	err error
}

var errListenerConnClosed = errors.New("pq: ListenerConn has been closed")

const (
	listenerChannelCapacity = 32
)

// ListenerConn is a low-level interface for waiting for notifications. You
// should use [Listener] instead.
type ListenerConn struct {
	connectionLock   sync.Mutex    // guards cn and err
	senderLock       chan struct{} // the sending goroutine holds the token in this channel
	senderLockOnce   sync.Once
	cn               *conn
	err              error
	connState        int32
	notificationChan chan<- *Notification
	replyChan        chan message
	done             chan struct{}
	doneOnce         sync.Once
}

// NewListenerConn creates a new ListenerConn. Use NewListener instead.
func NewListenerConn(name string, notificationChan chan<- *Notification) (*ListenerConn, error) {
	return newDialListenerConn(defaultDialer{}, name, notificationChan)
}

func newDialListenerConn(d Dialer, name string, c chan<- *Notification) (*ListenerConn, error) {
	cn, err := DialOpen(d, name)
	if err != nil {
		return nil, err
	}
	return startListenerConn(cn.(*conn), c), nil
}

func newDialListenerConnContext(ctx context.Context, d Dialer, name string, c chan<- *Notification) (*ListenerConn, error) {
	connector, err := NewConnector(name)
	if err != nil {
		return nil, err
	}
	connector.Dialer(d)
	cn, err := connector.open(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = cn.Close()
		return nil, err
	}
	return startListenerConn(cn, c), nil
}

func startListenerConn(cn *conn, c chan<- *Notification) *ListenerConn {
	l := &ListenerConn{
		cn:               cn,
		notificationChan: c,
		connState:        connStateIdle,
		replyChan:        make(chan message, 2),
		done:             make(chan struct{}),
	}

	go l.listenerConnMain()
	return l
}

// We can only allow one goroutine at a time to be running a query on the
// connection for various reasons, so the goroutine sending on the connection
// must be holding senderLock.
//
// Returns an error if an unrecoverable error has occurred and the ListenerConn
// should be abandoned.
func (l *ListenerConn) acquireSenderLock(ctx context.Context) error {
	l.senderLockOnce.Do(func() {
		l.senderLock = make(chan struct{}, 1)
		l.senderLock <- struct{}{}
	})

	// We must acquire senderLock first to avoid deadlocks; see
	// ExecSimpleQueryContext. A channel token lets a canceled caller stop waiting
	// behind another query without abandoning a goroutine on a sync.Mutex.
	select {
	case <-l.senderLock:
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		l.connectionLock.Lock()
		err := l.err
		l.connectionLock.Unlock()
		return err
	}
	if err := ctx.Err(); err != nil {
		l.releaseSenderLock()
		return err
	}

	l.connectionLock.Lock()
	err := l.err
	l.connectionLock.Unlock()
	if err != nil {
		l.releaseSenderLock()
		return err
	}
	return nil
}

func (l *ListenerConn) releaseSenderLock() {
	l.senderLock <- struct{}{}
}

// setState advances the protocol state to newState. Returns false if moving
// to that state from the current state is not allowed.
func (l *ListenerConn) setState(newState int32) bool {
	var expectedState int32

	switch newState {
	case connStateIdle:
		expectedState = connStateExpectReadyForQuery
	case connStateExpectResponse:
		expectedState = connStateIdle
	case connStateExpectReadyForQuery:
		expectedState = connStateExpectResponse
	default:
		panic(fmt.Sprintf("unexpected listenerConnState %d", newState))
	}

	return atomic.CompareAndSwapInt32(&l.connState, expectedState, newState)
}

// Main logic is here: receive messages from the postgres backend, forward
// notifications and query replies and keep the internal state in sync with the
// protocol state. Returns when the connection has been lost, is about to go
// away or should be discarded because we couldn't agree on the state with the
// server backend.
func (l *ListenerConn) listenerConnLoop() (err error) {
	r := &readBuf{}
	for {
		t, err := l.cn.recvMessage(r)
		if err != nil {
			return err
		}

		switch t {
		case proto.NotificationResponse:
			// recvNotification copies all the data so we don't need to worry
			// about the scratch buffer being overwritten.
			select {
			case l.notificationChan <- recvNotification(r):
			case <-l.done:
				return errListenerConnClosed
			}

		case proto.RowDescription, proto.DataRow:
			// only used by tests; ignore

		case proto.ErrorResponse:
			// We might receive an ErrorResponse even when not in a query; it
			// is expected that the server will close the connection after
			// that, but we should make sure that the error we display is the
			// one from the stray ErrorResponse, not io.ErrUnexpectedEOF.
			if !l.setState(connStateExpectReadyForQuery) {
				return parseError(r, "")
			}
			l.replyChan <- message{t, parseError(r, "")}

		case proto.CommandComplete, proto.EmptyQueryResponse:
			if !l.setState(connStateExpectReadyForQuery) {
				// protocol out of sync
				return fmt.Errorf("unexpected CommandComplete")
			}
			// ExecSimpleQuery doesn't need to know about this message

		case proto.ReadyForQuery:
			if !l.setState(connStateIdle) {
				// protocol out of sync
				return fmt.Errorf("unexpected ReadyForQuery")
			}
			l.replyChan <- message{t, nil}

		case proto.ParameterStatus:
			// ignore
		case proto.NoticeResponse:
			if n := l.cn.noticeHandler; n != nil {
				n(parseError(r, ""))
			}
		default:
			return fmt.Errorf("unexpected message %q from server in listenerConnLoop", t)
		}
	}
}

// This is the main routine for the goroutine receiving on the database
// connection. Most of the main logic is in listenerConnLoop.
func (l *ListenerConn) listenerConnMain() {
	err := l.listenerConnLoop()

	// listenerConnLoop terminated; we're done, but we still have to clean up.
	// Make sure nobody tries to start any new queries by making sure the err
	// pointer is set. It is important that we do not overwrite its value; a
	// connection could be closed by either this goroutine or one sending on the
	// connection – whoever closes the connection is assumed to have the more
	// meaningful error message (as the other one will probably get
	// net.errClosed), so that goroutine sets the error we expose while the
	// other error is discarded. If the connection is lost while two goroutines
	// are operating on the socket, it probably doesn't matter which error we
	// expose so we don't try to do anything more complex.
	l.connectionLock.Lock()
	if l.err == nil {
		l.err = err
	}
	l.signalDone()
	l.connectionLock.Unlock()
	// The receive loop has already failed, so a graceful Terminate exchange is
	// neither useful nor safe. Raw-close without holding connectionLock so a
	// concurrent Close or context cancellation cannot wait behind network I/O.
	_ = l.cn.c.Close()

	// There might be a query in-flight; make sure nobody's waiting for a
	// response to it, since there's not going to be one.
	close(l.replyChan)

	// let the listener know we're done
	close(l.notificationChan)

	// this ListenerConn is done
}

// Listen sends a LISTEN query to the server. See ExecSimpleQuery.
func (l *ListenerConn) Listen(channel string) (bool, error) {
	return l.ListenContext(context.Background(), channel)
}

// ListenContext sends a LISTEN query to the server. Canceling ctx invalidates
// the dedicated connection because its protocol state can no longer be known.
func (l *ListenerConn) ListenContext(ctx context.Context, channel string) (bool, error) {
	return l.ExecSimpleQueryContext(ctx, "LISTEN "+QuoteIdentifier(channel))
}

// Unlisten sends an UNLISTEN query to the server. See ExecSimpleQuery.
func (l *ListenerConn) Unlisten(channel string) (bool, error) {
	return l.UnlistenContext(context.Background(), channel)
}

// UnlistenContext sends an UNLISTEN query to the server. Canceling ctx
// invalidates the dedicated connection because its protocol state can no
// longer be known.
func (l *ListenerConn) UnlistenContext(ctx context.Context, channel string) (bool, error) {
	return l.ExecSimpleQueryContext(ctx, "UNLISTEN "+QuoteIdentifier(channel))
}

// UnlistenAll sends an `UNLISTEN *` query to the server. See ExecSimpleQuery.
func (l *ListenerConn) UnlistenAll() (bool, error) {
	return l.UnlistenAllContext(context.Background())
}

// UnlistenAllContext sends an `UNLISTEN *` query to the server. Canceling ctx
// invalidates the dedicated connection because its protocol state can no
// longer be known.
func (l *ListenerConn) UnlistenAllContext(ctx context.Context) (bool, error) {
	return l.ExecSimpleQueryContext(ctx, "UNLISTEN *")
}

// Ping the remote server to make sure it's alive. Non-nil error means the
// connection has failed and should be abandoned.
func (l *ListenerConn) Ping() error {
	return l.PingContext(context.Background())
}

// PingContext pings the remote server. Canceling ctx invalidates the dedicated
// connection so a blackholed read or write cannot leave the call blocked.
func (l *ListenerConn) PingContext(ctx context.Context) error {
	sent, err := l.ExecSimpleQueryContext(ctx, "")
	if !sent {
		return err
	}
	return err
}

// Attempt to send a query on the connection. Returns an error if sending the
// query failed, and the caller should initiate closure of this connection. The
// caller must be holding senderLock (see acquireSenderLock and
// releaseSenderLock).
func (l *ListenerConn) sendSimpleQuery(q string) (err error) {
	// Must set connection state before sending the query
	if !l.setState(connStateExpectResponse) {
		return errors.New("pq: two queries running at the same time")
	}

	// Can't use l.cn.writeBuf here because it uses the scratch buffer which
	// might get overwritten by listenerConnLoop.
	b := &writeBuf{
		buf: []byte("Q\x00\x00\x00\x00"),
		pos: 1,
	}
	b.string(q)
	return l.cn.send(b)
}

// ExecSimpleQuery executes a "simple query" (i.e. one with no bindable
// parameters) on the connection. The possible return values are:
//  1. "executed" is true; the query was executed to completion on the database
//     server. If the query failed, err will be set to the error returned by the
//     database, otherwise err will be nil.
//  2. If "executed" is false, the query could not be executed on the remote
//     server. err will be non-nil.
//
// After a call to ExecSimpleQuery has returned an executed=false value, the
// connection has either been closed or will be closed shortly thereafter, and
// all subsequently executed queries will return an error.
func (l *ListenerConn) ExecSimpleQuery(q string) (executed bool, err error) {
	return l.ExecSimpleQueryContext(context.Background(), q)
}

// ExecSimpleQueryContext is like ExecSimpleQuery, but permits a caller to bound
// both writing the query and waiting for its response. Once a query has begun,
// canceling ctx closes this ListenerConn: PostgreSQL's simple-query protocol
// provides no way to resume safely without consuming the complete response.
func (l *ListenerConn) ExecSimpleQueryContext(ctx context.Context, q string) (executed bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err = l.acquireSenderLock(ctx); err != nil {
		return false, err
	}
	defer l.releaseSenderLock()

	stopCancel := func() {}
	if ctx.Done() != nil {
		var operationActive atomic.Bool
		operationActive.Store(true)
		cancelDone := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			if operationActive.CompareAndSwap(true, false) {
				l.invalidate(ctx.Err())
			}
			close(cancelDone)
		})
		stopCancel = func() {
			if operationActive.CompareAndSwap(true, false) {
				if !stop() {
					<-cancelDone
				}
				return
			}
			<-cancelDone
		}
	}
	defer stopCancel()

	err = l.sendSimpleQuery(q)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			l.invalidate(ctxErr)
			return false, ctxErr
		}
		// We can't know what state the protocol is in, so we need to abandon
		// this connection.
		l.invalidate(err)
		return false, err
	}

	// now we just wait for a reply..
	for {
		var (
			m  message
			ok bool
		)
		select {
		case m, ok = <-l.replyChan:
		case <-ctx.Done():
			l.invalidate(ctx.Err())
			return false, ctx.Err()
		}
		if !ok {
			// We lost the connection to server, don't bother waiting for a
			// a response. err should have been set already.
			l.connectionLock.Lock()
			err := l.err
			l.connectionLock.Unlock()
			if ctxErr := ctx.Err(); ctxErr != nil {
				l.invalidate(ctxErr)
				return false, ctxErr
			}
			return false, err
		}
		switch m.typ {
		case proto.ReadyForQuery:
			// sanity check
			if m.err != nil {
				panic("m.err != nil")
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				l.invalidate(ctxErr)
				return false, ctxErr
			}
			// done; err might or might not be set
			return true, err

		case proto.ErrorResponse:
			// sanity check
			if m.err == nil {
				panic("m.err == nil")
			}
			// server responded with an error; ReadyForQuery to follow
			err = m.err

		default:
			return false, fmt.Errorf("unknown response for simple query: %q", m.typ)
		}
	}
}

// invalidate records err and closes the network connection. It is safe to call
// concurrently and is used when an in-flight operation can no longer consume
// the remainder of its protocol response.
func (l *ListenerConn) invalidate(err error) {
	l.connectionLock.Lock()
	if l.err == nil {
		l.err = err
	}
	l.signalDone()
	l.connectionLock.Unlock()
	_ = l.cn.c.Close()
}

// Close closes the connection.
func (l *ListenerConn) Close() error {
	l.connectionLock.Lock()
	if l.err != nil {
		l.connectionLock.Unlock()
		return errListenerConnClosed
	}
	l.err = errListenerConnClosed
	l.signalDone()
	l.connectionLock.Unlock()
	// We can't send anything on the connection without holding senderLock.
	// Simply close the net.Conn to wake up everyone operating on it.
	return l.cn.c.Close()
}

func (l *ListenerConn) signalDone() {
	l.doneOnce.Do(func() { close(l.done) })
}

// Err returns the reason the connection was closed. It is not safe to call
// this function until l.Notify has been closed.
func (l *ListenerConn) Err() error {
	return l.err
}

// ErrChannelAlreadyOpen is returned from Listen when a channel is already
// open.
var ErrChannelAlreadyOpen = errors.New("pq: channel is already open")

// ErrChannelNotOpen is returned from Unlisten when a channel is not open.
var ErrChannelNotOpen = errors.New("pq: channel is not open")

// ListenerEventType is an enumeration of listener event types.
type ListenerEventType int

const (
	// ListenerEventConnected is emitted only when the database connection has
	// been initially initialized. The err argument of the callback will always
	// be nil.
	ListenerEventConnected ListenerEventType = iota

	// ListenerEventDisconnected is emitted after a database connection has been
	// lost, either because of an error or because Close has been called. The
	// err argument will be set to the reason the database connection was lost.
	ListenerEventDisconnected

	// ListenerEventReconnected is emitted after a database connection has been
	// re-established after connection loss. The err argument of the callback
	// will always be nil. After this event has been emitted, a nil
	// pq.Notification is sent on the Listener.Notify channel.
	ListenerEventReconnected

	// ListenerEventConnectionAttemptFailed is emitted after a connection to the
	// database was attempted, but failed. The err argument will be set to an
	// error describing why the connection attempt did not succeed.
	ListenerEventConnectionAttemptFailed
)

// EventCallbackType is the event callback type. See also ListenerEventType
// constants' documentation.
type EventCallbackType func(event ListenerEventType, err error)

func (l ListenerEventType) String() string {
	return map[ListenerEventType]string{
		ListenerEventConnected:               "connected",
		ListenerEventDisconnected:            "disconnected",
		ListenerEventReconnected:             "reconnected",
		ListenerEventConnectionAttemptFailed: "connectionAttemptFailed",
	}[l]
}

// Listener provides an interface for listening to notifications from a
// PostgreSQL database. For general usage information, see section
// "Notifications".
//
// Listener can safely be used from concurrently running goroutines.
type Listener struct {
	// Channel for receiving notifications from the database. In some cases a
	// nil value will be sent to report possible notification loss. See section
	// "Notifications" above.
	Notify chan *Notification

	dsn                  string
	minReconnectInterval time.Duration
	maxReconnectInterval time.Duration
	dialer               Dialer
	eventCallback        EventCallbackType
	eventLock            sync.Mutex
	eventQueue           []listenerEvent
	eventWake            chan struct{}

	// connectLock serializes connection attempts. operationLock serializes
	// changes to channels with queries and connection resynchronization. Neither
	// lock is needed by Close. operationLock is a channel token so a context can
	// cancel while an operation is queued behind another operation.
	connectLock       sync.Mutex
	operationLock     chan struct{}
	operationLockOnce sync.Once

	lock                 sync.Mutex
	isClosed             bool
	done                 chan struct{}
	reconnectCond        *sync.Cond
	connectAttempt       uint64
	connectCancel        context.CancelFunc
	pendingCN            *ListenerConn
	cn                   *ListenerConn
	connNotificationChan <-chan *Notification
	channels             map[string]struct{}
	notificationQueue    chan *Notification
	notificationLost     atomic.Bool
}

// NewListener creates a new database connection dedicated to LISTEN / NOTIFY.
//
// name should be set to a connection string to be used to establish the
// database connection (see section "Connection String Parameters" above).
//
// minReconnect controls the duration to wait before trying to re-establish the
// database connection after connection loss. After each consecutive failure
// this interval is doubled, until maxReconnect is reached. Successfully
// completing the connection establishment procedure resets the interval back to
// minReconnect.
//
// If maxReconnect is less than minReconnect, it is raised to minReconnect.
//
// The last parameter cb can be set to a function which will be called by the
// Listener when the state of the underlying database connection changes.
// Callbacks are delivered in order by a dedicated goroutine, so a callback may
// call Listener methods. You should still avoid potentially time-consuming
// work because it delays later callbacks.
func NewListener(dsn string, minReconnect, maxReconnect time.Duration, cb EventCallbackType) *Listener {
	return NewDialListener(defaultDialer{}, dsn, minReconnect, maxReconnect, cb)
}

// NewDialListener is like NewListener but it takes a Dialer.
func NewDialListener(d Dialer, dsn string, minReconnect, maxReconnect time.Duration, cb EventCallbackType) *Listener {
	minReconnect, maxReconnect = normalizeReconnectIntervals(minReconnect, maxReconnect)
	l := &Listener{
		dsn:                  dsn,
		minReconnectInterval: minReconnect,
		maxReconnectInterval: maxReconnect,
		dialer:               d,
		eventCallback:        cb,
		eventWake:            make(chan struct{}, 1),
		channels:             make(map[string]struct{}),
		Notify:               make(chan *Notification, listenerChannelCapacity),
		done:                 make(chan struct{}),
	}
	l.reconnectCond = sync.NewCond(&l.lock)
	go l.listenerMain()
	return l
}

func normalizeReconnectIntervals(minReconnect, maxReconnect time.Duration) (time.Duration, time.Duration) {
	if maxReconnect < minReconnect {
		maxReconnect = minReconnect
	}
	return minReconnect, maxReconnect
}

// NotificationChannel returns the notification channel for this listener. This
// is the same channel as Notify, and will not be recreated during the life time
// of the Listener.
func (l *Listener) NotificationChannel() <-chan *Notification {
	return l.Notify
}

func (l *Listener) acquireOperationLock(ctx context.Context) error {
	l.operationLockOnce.Do(func() {
		l.operationLock = make(chan struct{}, 1)
		l.operationLock <- struct{}{}
	})

	select {
	case <-l.operationLock:
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return net.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		l.releaseOperationLock()
		return err
	}
	l.lock.Lock()
	closed := l.isClosed
	l.lock.Unlock()
	if closed {
		l.releaseOperationLock()
		return net.ErrClosed
	}
	return nil
}

func (l *Listener) releaseOperationLock() {
	l.operationLock <- struct{}{}
}

// Listen starts listening for notifications on a channel. Calls to this
// function will block until an acknowledgement has been received from the
// server. Note that Listener automatically re-establishes the connection after
// connection loss, so this function may block indefinitely if the connection
// can not be re-established.
//
// Listen will only fail in three conditions:
//  1. The channel is already open. The returned error will be
//     [ErrChannelAlreadyOpen].
//  2. The query was executed on the remote server, but PostgreSQL returned an
//     error message in response to the query. The returned error will be a
//     [pq.Error] containing the information the server supplied.
//  3. Close is called on the Listener before the request could be completed.
//
// The channel name is case-sensitive.
func (l *Listener) Listen(channel string) error {
	return l.ListenContext(context.Background(), channel)
}

// ListenContext is like Listen, but ctx bounds waiting to send LISTEN, receive
// its acknowledgement, or obtain a replacement connection. If ctx is canceled
// while a query is in flight, the dedicated connection is discarded and the
// Listener reconnects before it is used again.
//
// As with other context-aware database operations, cancellation can race with
// completion. A nil return means the acknowledgement was received. A context
// error means the caller cannot know whether the request took effect before
// the connection was discarded.
func (l *Listener) ListenContext(ctx context.Context, channel string) error {
	for {
		if err := l.acquireOperationLock(ctx); err != nil {
			return err
		}

		l.lock.Lock()
		if l.isClosed {
			l.lock.Unlock()
			l.releaseOperationLock()
			return net.ErrClosed
		}

		// The server allows LISTEN on a channel which is already open, but
		// reporting this catches mistakes in application logic.
		if _, exists := l.channels[channel]; exists {
			l.lock.Unlock()
			l.releaseOperationLock()
			return ErrChannelAlreadyOpen
		}
		cn := l.cn
		l.lock.Unlock()

		if cn == nil {
			l.releaseOperationLock()
			if err := l.waitForConnectionContext(ctx, nil); err != nil {
				return err
			}
			continue
		}

		// Record desired state only after PostgreSQL acknowledges LISTEN. If
		// the connection fails first, wait for its replacement and retry; this
		// prevents a canceled caller from creating a future subscription.
		responded, err := cn.ListenContext(ctx, channel)
		l.lock.Lock()
		if l.isClosed {
			l.lock.Unlock()
			l.releaseOperationLock()
			return net.ErrClosed
		}
		if responded {
			if err != nil {
				l.lock.Unlock()
				l.releaseOperationLock()
				return err
			}
			l.channels[channel] = struct{}{}
			l.lock.Unlock()
			l.releaseOperationLock()
			return nil
		}
		l.lock.Unlock()
		l.releaseOperationLock()

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err := l.waitForConnectionContext(ctx, cn); err != nil {
			return err
		}
	}
}

// waitForConnection waits until old has been replaced by a live connection.
// Callers must not hold operationLock while waiting: reconnect needs it in
// order to synchronize the desired channel set with the new connection.
func (l *Listener) waitForConnection(old *ListenerConn) error {
	return l.waitForConnectionContext(context.Background(), old)
}

func (l *Listener) waitForConnectionContext(ctx context.Context, old *ListenerConn) error {
	stopWake := func() bool { return true }
	if ctx.Done() != nil {
		stopWake = context.AfterFunc(ctx, func() {
			l.lock.Lock()
			l.reconnectCond.Broadcast()
			l.lock.Unlock()
		})
	}
	defer stopWake()

	l.lock.Lock()
	defer l.lock.Unlock()
	for !l.isClosed && ctx.Err() == nil && (l.cn == nil || l.cn == old) {
		l.reconnectCond.Wait()
	}
	if l.isClosed {
		return net.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Unlisten removes a channel from the Listener's channel list. Returns
// ErrChannelNotOpen if the Listener is not listening on the specified channel.
// Returns immediately with no error if there is no connection. Note that you
// might still get notifications for this channel even after Unlisten has
// returned.
//
// The channel name is case-sensitive.
func (l *Listener) Unlisten(channel string) error {
	return l.UnlistenContext(context.Background(), channel)
}

// UnlistenContext is like Unlisten, but ctx bounds waiting to send UNLISTEN
// and receive its acknowledgement. Canceling an in-flight request discards the
// dedicated connection so it can be resynchronized safely.
func (l *Listener) UnlistenContext(ctx context.Context, channel string) error {
	if err := l.acquireOperationLock(ctx); err != nil {
		return err
	}
	defer l.releaseOperationLock()

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}

	// Similarly to LISTEN, this is not an error in Postgres, but it seems
	// useful to distinguish from the normal conditions.
	_, exists := l.channels[channel]
	if !exists {
		l.lock.Unlock()
		return ErrChannelNotOpen
	}

	cn := l.cn
	if cn == nil {
		// Don't bother waiting for resync if there's no connection.
		delete(l.channels, channel)
		l.lock.Unlock()
		return nil
	}
	l.lock.Unlock()

	// Similarly to Listen (see comment there), the caller should only be
	// bothered with an error if it came from the backend as a response to
	// our query.
	resp, err := cn.UnlistenContext(ctx, channel)

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}
	if !resp {
		if ctxErr := ctx.Err(); ctxErr != nil {
			l.lock.Unlock()
			return ctxErr
		}
	}
	if resp && err != nil {
		l.lock.Unlock()
		return err
	}
	delete(l.channels, channel)
	l.lock.Unlock()
	return nil
}

// UnlistenAll removes all channels from the Listener's channel list. Returns
// immediately with no error if there is no connection. Note that you might
// still get notifications for any of the deleted channels even after
// UnlistenAll has returned.
func (l *Listener) UnlistenAll() error {
	return l.UnlistenAllContext(context.Background())
}

// UnlistenAllContext is like UnlistenAll, but ctx bounds waiting to send
// UNLISTEN * and receive its acknowledgement. Canceling an in-flight request
// discards the dedicated connection so it can be resynchronized safely.
func (l *Listener) UnlistenAllContext(ctx context.Context) error {
	if err := l.acquireOperationLock(ctx); err != nil {
		return err
	}
	defer l.releaseOperationLock()

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}

	cn := l.cn
	if cn == nil {
		// Don't bother waiting for resync if there's no connection.
		l.channels = make(map[string]struct{})
		l.lock.Unlock()
		return nil
	}
	l.lock.Unlock()

	// Similarly to Listen (see comment in that function), the caller
	// should only be bothered with an error if it came from the backend as
	// a response to our query.
	gotResponse, err := cn.UnlistenAllContext(ctx)

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}
	if !gotResponse {
		if ctxErr := ctx.Err(); ctxErr != nil {
			l.lock.Unlock()
			return ctxErr
		}
	}
	if gotResponse && err != nil {
		l.lock.Unlock()
		return err
	}
	l.channels = make(map[string]struct{})
	l.lock.Unlock()
	return nil
}

// Ping the remote server to make sure it's alive. Non-nil return value means
// that there is no active connection.
func (l *Listener) Ping() error {
	return l.PingContext(context.Background())
}

// PingContext pings the remote server and bounds both the network write and the
// response wait with ctx. Canceling an in-flight ping discards the dedicated
// connection; Listener will reconnect automatically.
func (l *Listener) PingContext(ctx context.Context) error {
	if err := l.acquireOperationLock(ctx); err != nil {
		return err
	}
	defer l.releaseOperationLock()

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}
	cn := l.cn
	l.lock.Unlock()
	if cn == nil {
		return errors.New("no connection")
	}

	err := cn.PingContext(ctx)
	l.lock.Lock()
	closed := l.isClosed
	l.lock.Unlock()
	if closed {
		return net.ErrClosed
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// Clean up after losing the server connection. Returns cn.Err(), which should
// have the reason the connection was lost.
func (l *Listener) disconnectCleanup(cn *ListenerConn, notificationChan <-chan *Notification) error {
	// sanity check; can't look at Err() until the channel has been closed
	select {
	case _, ok := <-notificationChan:
		if ok {
			panic("connNotificationChan not closed")
		}
	default:
		panic("connNotificationChan not closed")
	}

	err := cn.Err()
	_ = cn.Close()

	l.lock.Lock()
	if l.cn == cn {
		l.cn = nil
		l.connNotificationChan = nil
		l.reconnectCond.Broadcast()
	}
	l.lock.Unlock()
	return err
}

// Synchronize the list of channels we want to be listening on with the server
// after the connection has been established.
func (l *Listener) resync(ctx context.Context, cn *ListenerConn, notificationChan <-chan *Notification, channels []string) error {
	doneChan := make(chan error, 1)
	go func(notificationChan <-chan *Notification) {
		for _, channel := range channels {
			// If we got a response, return that error to our caller as it's
			// going to be more descriptive than cn.Err().
			gotResponse, err := cn.ListenContext(ctx, channel)
			if gotResponse && err != nil {
				doneChan <- err
				return
			}

			// If we couldn't reach the server, wait for notificationChan to
			// close and then return the error message from the connection, as
			// per ListenerConn's interface.
			if err != nil {
				for range notificationChan {
				}
				doneChan <- cn.Err()
				return
			}
		}
		doneChan <- nil
	}(notificationChan)

	// Ignore notifications while synchronization is going on to avoid
	// deadlocks. We have to send a nil notification over Notify anyway as we
	// can't possibly know which notifications (if any) were lost while the
	// connection was down, so there's no reason to try and process these
	// messages at all.
	for {
		select {
		case _, ok := <-notificationChan:
			if !ok {
				notificationChan = nil
			}

		case err := <-doneChan:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// caller should NOT be holding l.lock
func (l *Listener) closed() bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.isClosed
}

func (l *Listener) connect() error {
	l.connectLock.Lock()
	defer l.connectLock.Unlock()

	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}
	l.connectAttempt++
	attempt := l.connectAttempt
	ctx, cancel := context.WithCancel(context.Background())
	l.connectCancel = cancel
	l.lock.Unlock()

	notificationChan := make(chan *Notification, 32)
	cn, err := newDialListenerConnContext(ctx, l.dialer, l.dsn, notificationChan)
	if err != nil {
		cancel()
		l.lock.Lock()
		if l.connectAttempt == attempt {
			l.connectCancel = nil
		}
		closed := l.isClosed
		l.lock.Unlock()
		if closed {
			return net.ErrClosed
		}
		return err
	}

	l.lock.Lock()
	if l.isClosed || l.connectAttempt != attempt {
		l.lock.Unlock()
		cancel()
		_ = cn.Close()
		return net.ErrClosed
	}
	l.pendingCN = cn
	l.lock.Unlock()

	// Freeze changes to the desired channel set between taking the snapshot and
	// publishing the resynchronized connection. Public operations may still be
	// interrupted by Close because Close does not acquire operationLock.
	if err = l.acquireOperationLock(ctx); err != nil {
		l.lock.Lock()
		if l.connectAttempt == attempt {
			if l.pendingCN == cn {
				l.pendingCN = nil
			}
			l.connectCancel = nil
		}
		closed := l.isClosed
		l.lock.Unlock()
		cancel()
		_ = cn.Close()
		if closed {
			return net.ErrClosed
		}
		return err
	}
	l.lock.Lock()
	if l.isClosed || l.connectAttempt != attempt || l.pendingCN != cn {
		l.lock.Unlock()
		l.releaseOperationLock()
		cancel()
		_ = cn.Close()
		return net.ErrClosed
	}
	channels := make([]string, 0, len(l.channels))
	for channel := range l.channels {
		channels = append(channels, channel)
	}
	l.lock.Unlock()

	err = l.resync(ctx, cn, notificationChan, channels)
	if err == nil {
		l.lock.Lock()
		if !l.isClosed && l.connectAttempt == attempt && l.pendingCN == cn {
			l.pendingCN = nil
			l.connectCancel = nil
			l.cn = cn
			l.connNotificationChan = notificationChan
			l.reconnectCond.Broadcast()
			l.lock.Unlock()
			l.releaseOperationLock()
			cancel()
			return nil
		}
		l.lock.Unlock()
	}

	l.lock.Lock()
	if l.connectAttempt == attempt {
		if l.pendingCN == cn {
			l.pendingCN = nil
		}
		l.connectCancel = nil
	}
	closed := l.isClosed
	l.lock.Unlock()
	l.releaseOperationLock()
	cancel()
	_ = cn.Close()
	if closed {
		return net.ErrClosed
	}
	return err
}

// Close disconnects the Listener from the database and shuts it down.
// Subsequent calls to its methods will return an error. Close returns an error
// if the connection has already been closed.
func (l *Listener) Close() error {
	l.lock.Lock()
	if l.isClosed {
		l.lock.Unlock()
		return net.ErrClosed
	}

	l.isClosed = true
	l.connectAttempt++
	if l.done == nil {
		l.done = make(chan struct{})
	}
	close(l.done)
	cancel := l.connectCancel
	pendingCN := l.pendingCN
	cn := l.cn
	l.connectCancel = nil
	l.pendingCN = nil
	l.cn = nil
	l.connNotificationChan = nil

	// Unblock calls to Listen()
	l.reconnectCond.Broadcast()
	l.lock.Unlock()

	if cancel != nil {
		cancel()
	}
	if pendingCN != nil {
		_ = pendingCN.Close()
	}
	if cn != nil && cn != pendingCN {
		_ = cn.Close()
	}

	return nil
}

func (l *Listener) emitEvent(event ListenerEventType, err error) {
	if l.eventCallback == nil {
		return
	}
	select {
	case <-l.done:
		return
	default:
	}

	l.eventLock.Lock()
	l.eventQueue = append(l.eventQueue, listenerEvent{typ: event, err: err})
	l.eventLock.Unlock()
	select {
	case l.eventWake <- struct{}{}:
	default:
	}
}

func (l *Listener) eventDispatcher() {
	for {
		select {
		case <-l.eventWake:
		case <-l.done:
			return
		}

		for {
			select {
			case <-l.done:
				return
			default:
			}

			l.eventLock.Lock()
			if len(l.eventQueue) == 0 {
				l.eventLock.Unlock()
				break
			}
			event := l.eventQueue[0]
			l.eventQueue[0] = listenerEvent{}
			l.eventQueue = l.eventQueue[1:]
			if len(l.eventQueue) == 0 {
				l.eventQueue = nil
			}
			l.eventLock.Unlock()
			l.eventCallback(event.typ, event.err)
		}
	}
}

// waitReconnect waits for the next connection attempt or for Close. It returns
// false when the listener has been closed.
func (l *Listener) waitReconnect(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-l.done:
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-l.done:
		return false
	}
}

// sendNotification places a notification on a finite internal delivery queue.
// A slow public Notify consumer must not stop protocol processing or reconnects.
// When the queue is full, the notification is dropped and notificationLost
// causes the dispatcher to deliver a coalesced nil loss marker when possible.
func (l *Listener) sendNotification(notification *Notification) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	// Once a gap has occurred, drop subsequent notifications until the
	// dispatcher claims the loss marker. That marker is an ordering barrier:
	// retained pre-gap notifications are delivered before it, and accepted
	// post-gap notifications after it.
	if l.notificationLost.Load() {
		return true
	}

	select {
	case l.notificationQueue <- notification:
		return true
	case <-l.done:
		return false
	default:
		l.notificationLost.Store(true)
		return true
	}
}

func (l *Listener) notificationDispatcher() {
	defer close(l.Notify)
	for {
		// Drain the retained prefix before reporting a gap. A non-blocking
		// receive is needed here so loss markers cannot overtake queued data.
		select {
		case notification := <-l.notificationQueue:
			select {
			case l.Notify <- notification:
			case <-l.done:
				return
			}
			continue
		default:
		}

		if l.notificationLost.CompareAndSwap(true, false) {
			select {
			case l.Notify <- nil:
				continue
			case <-l.done:
				return
			}
		}

		select {
		case notification := <-l.notificationQueue:
			select {
			case l.Notify <- notification:
			case <-l.done:
				return
			}
		case <-l.done:
			return
		}
	}
}

// Main logic here: maintain a connection to the server when possible, wait
// for notifications and emit events.
func (l *Listener) listenerConnLoop() {
	var (
		nextReconnect     time.Time
		reconnectInterval = l.minReconnectInterval
	)
	for {
		for {
			err := l.connect()
			if err == nil {
				break
			}
			if l.closed() {
				return
			}

			l.emitEvent(ListenerEventConnectionAttemptFailed, err)
			if !l.waitReconnect(reconnectInterval) {
				return
			}
			if reconnectInterval > l.maxReconnectInterval/2 {
				reconnectInterval = l.maxReconnectInterval
			} else {
				reconnectInterval *= 2
			}
		}

		l.lock.Lock()
		if l.isClosed {
			l.lock.Unlock()
			return
		}
		cn := l.cn
		notificationChan := l.connNotificationChan
		l.lock.Unlock()
		if cn == nil || notificationChan == nil {
			continue
		}

		if nextReconnect.IsZero() {
			l.emitEvent(ListenerEventConnected, nil)
		} else {
			l.emitEvent(ListenerEventReconnected, nil)
			if !l.sendNotification(nil) {
				return
			}
		}

		reconnectInterval = l.minReconnectInterval
		nextReconnect = time.Now().Add(reconnectInterval)

	connectionLoop:
		for {
			select {
			case notification, ok := <-notificationChan:
				if !ok { // lost connection, loop again
					break connectionLoop
				}
				if !l.sendNotification(notification) {
					return
				}
			case <-l.done:
				return
			}
		}

		err := l.disconnectCleanup(cn, notificationChan)
		if l.closed() {
			return
		}
		l.emitEvent(ListenerEventDisconnected, err)

		if !l.waitReconnect(time.Until(nextReconnect)) {
			return
		}
	}
}

func (l *Listener) listenerMain() {
	l.lock.Lock()
	if l.done == nil {
		l.done = make(chan struct{})
	}
	if l.notificationQueue == nil {
		l.notificationQueue = make(chan *Notification, listenerChannelCapacity)
	}
	if l.eventCallback != nil && l.eventWake == nil {
		l.eventWake = make(chan struct{}, 1)
	}
	l.lock.Unlock()

	if l.eventCallback != nil {
		go l.eventDispatcher()
	}
	go l.notificationDispatcher()
	l.listenerConnLoop()
}
