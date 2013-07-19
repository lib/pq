package pq

import (
	"database/sql"
	"hash/crc32"
)

type Lock struct {
	Key   int32
	Space int32
	conn  *sql.DB
}

func StringAsKey(name string) int32 {
	hash := crc32.NewIEEE()
	hash.Write([]byte(name))
	i := int32(hash.Sum32())
	return i
}

func NewLock(dataSourceName string) (*Lock, error) {
	conn, err := sql.Open("postgres", "")
	if err != nil {
		return nil, err
	}
	lock := new(Lock)
	lock.conn = conn
	lock.Space = -2147483648
	return lock, nil
}

// Lock locks l. If the lock is already in use, the calling goroutine blocks until the lock is available.
func (l *Lock) Lock() {
	_, err := l.conn.Exec("SELECT pg_advisory_lock($1, $2)", l.Space, l.Key)
	if err != nil {
		panic(err)
	}
}

// Unlock unlocks l.
func (l *Lock) Unlock() {
	_, err := l.conn.Exec("SELECT pg_advisory_unlock($1, $2)", l.Space, l.Key)
	if err != nil {
		panic(err)
	}
}
