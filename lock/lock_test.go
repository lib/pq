package lock

import (
	"hash/crc32"
	"testing"
)

func stringToInt(name string) int32 {
	hash := crc32.NewIEEE()
	hash.Write([]byte(name))
	i := int32(hash.Sum32())
	return i
}

func newLock(name string, t *testing.T) *Lock {
	lock, err := NewLock("", stringToInt("pg"), stringToInt(name))
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func HammerLock(l *Lock, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		l.Lock()
		l.Unlock()
	}
	cdone <- true
}

func TestLock(t *testing.T) {
	l := newLock("lock", t)
	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerLock(l, 10, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}

func TestNotHeld(t *testing.T) {
	l := newLock("held", t)
	l.Lock()
	l.Unlock()
	err := l.Unlock()
	if err != ErrLockNotHeld {
		t.Fatal(err)
	}
}

func TestSharedLocker(t *testing.T) {
	w := newLock("shared", t)
	l := newLock("shared", t)
	wlocked := make(chan bool, 1)
	rlocked := make(chan bool, 1)
	wl := w.Locker()
	rl := l.RLocker()
	n := 10
	go func() {
		for i := 0; i < n; i++ {
			rl.Lock()
			rl.Lock()
			rlocked <- true
			wl.Lock()
			wlocked <- true
		}
	}()
	for i := 0; i < n; i++ {
		<-rlocked
		rl.Unlock()
		select {
		case <-wlocked:
			t.Fatal("RLocker() didn't read-lock it")
		default:
		}
		rl.Unlock()
		<-wlocked
		select {
		case <-rlocked:
			t.Fatal("RLocker() didn't respect the write lock")
		default:
		}
		wl.Unlock()
	}
}
