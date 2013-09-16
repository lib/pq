package pq

import (
	"testing"
)

func newLock() (*Lock, error) {
	lock, err := NewLock("", StringAsKey("pg"), StringAsKey("lock"))
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func HammerLock(l *Lock, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		l.Lock()
		l.Unlock()
	}
	cdone <- true
}

func TestLock(t *testing.T) {
	l, _ := newLock()
	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerLock(l, 10, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}
