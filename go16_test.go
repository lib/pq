// +build go1.6

package pq

import "testing"
import "time"

func TestLifetime(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	db.SetConnMaxLifetime(time.Millisecond * 10)
	for i := 0; i < 35; i++ {
		time.Sleep(time.Millisecond)
		err := db.Ping()
		if err != nil {
			t.Error(err)
		} else {
			t.Log("ok")
		}
	}
}
