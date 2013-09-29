// Lock interface around PostgresSQL advisory locking, see docs:
// http://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS
package lock

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"sync"
)

type Lock struct {
	key   int32
	space int32
	conn  *sql.DB
}

var ErrLockNotHeld = errors.New("lock wasn't held")

func NewLock(dataSourceName string, space int32, key int32) (*Lock, error) {
	conn, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &Lock{
		key:   key,
		space: space,
		conn:  conn,
	}, nil
}

// Lock locks l. If the lock is already in use, the calling goroutine blocks until the lock is available.
func (l *Lock) Lock() error {
	return l.lock("SELECT pg_advisory_lock($1, $2)", l.space, l.key)
}

// RLock locks rw for reading.
func (l *Lock) RLock() error {
	return l.lock("SELECT pg_advisory_lock_shared($1, $2)", l.space, l.key)
}

func (l *Lock) lock(query string, args ...interface{}) error {
	_, err := l.conn.Exec(query, args...)
	return err
}

// Unlock unlocks l.
func (l *Lock) Unlock() error {
	return l.unlock("SELECT pg_advisory_unlock($1, $2)", l.space, l.key)
}

// RUnlock undoes a single RLock call
func (l *Lock) RUnlock() error {
	return l.unlock("SELECT pg_advisory_unlock_shared($1, $2)", l.space, l.key)
}

func (l *Lock) unlock(query string, args ...interface{}) error {
	var success bool
	err := l.conn.QueryRow(query, args...).Scan(&success)
	if err != nil {
		return err
	}
	if !success {
		return ErrLockNotHeld
	}
	return nil
}

// Locker returns a Locker interface that implements
// the Lock and Unlock methods.
func (l *Lock) Locker() sync.Locker {
	return (*locker)(l)
}

// RLocker returns a Locker interface that implements
// the Lock and Unlock methods by calling RLock and RUnlock.
func (l *Lock) RLocker() sync.Locker {
	return (*rlocker)(l)
}

type locker Lock

func (l *locker) Lock() {
	err := (*Lock)(l).Lock()
	if err != nil {
		panic(err)
	}
}

func (l *locker) Unlock() {
	err := (*Lock)(l).Unlock()
	if err != nil {
		panic(err)
	}
}

type rlocker Lock

func (l *rlocker) Lock() {
	err := (*Lock)(l).RLock()
	if err != nil {
		panic(err)
	}
}

func (l *rlocker) Unlock() {
	err := (*Lock)(l).RUnlock()
	if err != nil {
		panic(err)
	}
}
