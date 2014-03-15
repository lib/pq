/*

NotifySemaphore is a utility type for consumers using LISTEN / NOTIFY to avoid
polling the database for new work.


Usage


NotifySemaphore supports multiple concurrent channels, but it does not support
concurrent access to the same notification channel.  An attempt to do so might
result in undefined behaviour or panics.

An example of the intended usage pattern:

    package main

    import (
        "github.com/lib/pq"
        "github.com/lib/pq/notifysemaphore"
        "database/sql"
        "time"
    )

    func work() {
        // Fetch and process work from the database.  It is crucial to process
        // *all* available work, not just one task.
        for {
            task := getWorkFromDatabase()
            if task == nil {
                return
            }

            go doWorkOnTask(task)
        }
    }

    func main() {
        listener := pq.NewListener("", 15 * time.Second, time.Minute, nil)
        notifysemaphore := notifysemaphore.NewNotifySemaphore(listener)

        // It is important here that the order of operations is:
        //   1) Listen()
        //   2) Process *all* work
        //   3) Wait for a notification (possibly queued while in step 2)
        //   4) Go to 2
        //
        // Following this order guarantees that there will never be work
        // available in the database for extended periods of time without your
        // application knowing about it.
        sem, err := notifysemaphore.Listen("getwork")
        if err != nil {
            panic(err)
        }

        for {
            work()
            <-sem
        }
    }
 */
package notifysemaphore

import (
	"github.com/lib/pq"
	"errors"
	"fmt"
	"sync"
	"time"
)

var errClosed = errors.New("NotifySemaphore has been closed")

type NotifySemaphore struct {
	listener *pq.Listener

	closeWaitGroup sync.WaitGroup
	closeChannel chan struct{}
	closed bool

	newPingIntervalChannel chan time.Duration
	broadcastOnPingTimeout bool

	lock sync.Mutex
	channels map[string] chan<- *pq.Notification
}

func NewNotifySemaphore(listener *pq.Listener) *NotifySemaphore {
	dispatcher := &NotifySemaphore{
		listener: listener,
		channels: make(map[string] chan<- *pq.Notification),
		newPingIntervalChannel: make(chan time.Duration, 1),
	}
	dispatcher.closeWaitGroup.Add(1)
	go dispatcher.mainDispatcherLoop()
	return dispatcher
}

func (s *NotifySemaphore) removeChannel(channel string, ch chan<- *pq.Notification) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Check that we're still in the channel list.  This should not happen
	// unless someone is misusing our interface.
	oldch, ok := s.channels[channel]
	if !ok {
		panic(fmt.Sprintf("channel %s not part of NotifySemaphore.channels", channel))
	}
	if oldch != ch {
		panic(fmt.Sprintf("unexpected channel %v in channel %s; expected %v", oldch, channel, ch))
	}
	delete(s.channels, channel)
}

// Listen starts listening on a notification channel.  The returned Go channel
// ("semaphore channel") will be guaranteed to have at least one notification
// in it any time one or more notifications have been received from the
// database since the last receive on that channel.
//
// It is not safe to call Listen if a concurrent Unlisten call on the same
// channel is in progress.  However, it is safe to Listen on a channel which
// was previously Unlistened by a different goroutine.
//
// If the channel is already active, ErrChannelAlreadyOpen is returned.  If the
// NotifySemaphore has been closed, an error is returned.
func (s *NotifySemaphore) Listen(channel string) (<-chan *pq.Notification, error) {
	s.lock.Lock()

	if s.closed {
		s.lock.Unlock()
		return nil, errClosed
	}

	_, ok := s.channels[channel]
	if ok {
		s.lock.Unlock()
		return nil, pq.ErrChannelAlreadyOpen
	}
	ch := make(chan *pq.Notification, 1)
	s.channels[channel] = ch
	s.lock.Unlock()

	err := s.listener.Listen(channel)
	if err != nil {
		s.removeChannel(channel, ch)
		return nil, err
	}

	return ch, nil
}

// Unlisten stops listening on the supplied notification channel and closes the
// semaphore channel associated with it.  It is not safe to call Unlisten if a
// concurrent Listen call on that same channel is in progress, but it is safe
// to Unlisten a channel from a different goroutine than the one that
// previously executed Listen.  It is also safe to call Unlisten while a
// goroutine is waiting on the semaphore channel.  The channel will be closed
// gracefully.
//
// Returns ErrChannelNotOpen if the channel is not currently active, or an
// error if the NotifySemaphore has been closed.
func (s *NotifySemaphore) Unlisten(channel string) error {
	s.lock.Lock()

	if s.closed {
		s.lock.Unlock()
		return errClosed
	}

	ch, ok := s.channels[channel]
	if !ok {
		s.lock.Unlock()
		return pq.ErrChannelNotOpen
	}
	s.lock.Unlock()

	err := s.listener.Unlisten(channel)
	if err != nil {
		return err
	}

	s.removeChannel(channel, ch)
	close(ch)

	return nil
}

func (s *NotifySemaphore) Ping() error {
	return s.listener.Ping()
}

func (s *NotifySemaphore) SetPingInterval(interval time.Duration) {
	s.newPingIntervalChannel <- interval
}

func (s *NotifySemaphore) SetBroadcastOnPingTimeout(broadcastOnPingTimeout bool) {
	s.lock.Lock()
	s.broadcastOnPingTimeout = broadcastOnPingTimeout
	s.lock.Unlock()
}

func (s *NotifySemaphore) pingTimeout() {
	go func() {
		s.listener.Ping()
	}()

	s.lock.Lock()
	if s.broadcastOnPingTimeout {
		s.broadcast()
	}
	s.lock.Unlock()
}

// Close closes the NotifySemaphore and all of its associated channels.  It
// does not return until all semaphore channels have been closed.  Calling
// Close on a closed NotifySemaphore returns an error.
func (s *NotifySemaphore) Close() error {
	s.lock.Lock()
	if s.closed {
		return errClosed
	}
	s.closed = true
	s.closeChannel <- struct{}{}
	s.lock.Unlock()

	// wait for all channels to be closed
	s.closeWaitGroup.Wait()
	return nil
}

// Broadcast a nil *Notification to all listeners.  Caller must be holding
// s.lock.
func (s *NotifySemaphore) broadcast() {
	for channel := range s.channels {
		s.notify(channel, nil)
	}
}

// Send notication on a channel.  Caller must be holding s.lock.
func (s *NotifySemaphore) notify(channel string, n *pq.Notification) {
	ch, ok := s.channels[channel]
	if !ok {
		return
	}

	select {
		case ch <- n:

		default:
			// There's already a notification waiting in the channel, we can
			// ignore this one.
	}
}

func (s *NotifySemaphore) shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, ch := range s.channels {
		close(ch)
	}

	// let Close know we're done
	s.closeWaitGroup.Done()
}

func (s *NotifySemaphore) mainDispatcherLoop() {
	pingTimer := time.NewTimer(1)
	var pingInterval *time.Duration
	for {
		if pingInterval != nil {
			pingTimer.Reset(*pingInterval)
		} else {
			pingTimer.Stop()
		}

		select {
			case n := <-s.listener.Notify:
				s.lock.Lock()
				if n == nil {
					s.broadcast()
				} else {
					s.notify(n.Channel, n)
				}
				s.lock.Unlock()

			case <-s.closeChannel:
				s.shutdown()
				return

			case <-pingTimer.C:
				s.pingTimeout()

			case newPingInterval := <-s.newPingIntervalChannel:
				pingInterval = &newPingInterval
		}
	}
}
