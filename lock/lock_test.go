package lock

import (
	"testing"
	"hash/crc32"
)

func stringToInt(name string) int32 {
	hash := crc32.NewIEEE()
	hash.Write([]byte(name))
	i := int32(hash.Sum32())
	return i
}

func newLock() (*Lock, error) {
	lock, err := NewLock("", stringToInt("pg"), stringToInt("lock"))
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
	l, err := newLock()
	if err != nil {
		t.Fatal(err)
	}
	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerLock(l, 10, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}
