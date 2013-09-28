package lock

import (
	"database/sql"
	_ "github.com/lib/pq"
	"sync"
)

type Lock struct {
	key   int32
	space int32
	conn  *sql.DB
}

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
	_, err := l.conn.Exec("SELECT pg_advisory_lock($1, $2)", l.space, l.key)
	return err
}

// Unlock unlocks l.
func (l *Lock) Unlock() error {
	_, err := l.conn.Exec("SELECT pg_advisory_unlock($1, $2)", l.space, l.key)
	return err
}

// Locker returns a Locker interface that implements
// the Lock and Unlock methods.
func (l *Lock) Locker() sync.Locker {
	return (*locker)(l)
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
