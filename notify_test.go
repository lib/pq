package pq

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// Test reading notifications after they have been received
func TestWaitNotificationBefore(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	var channel = "channel_name"
	var payload = "PAYLOAD_DATA"
	var notify int
	var notcount = 5

	// Listen for notifications from 'channel_name'
	_, err := db.Exec("LISTEN " + channel)
	if err != nil {
		t.Fatal(err)
	}
	// Send notifications before accepting
	for notify = 1; notify <= notcount; notify++ {
		_, err = db.Exec(fmt.Sprintf("NOTIFY %s, '%s'", channel, payload+strconv.Itoa(notify)))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Wait for notifications up to timeout
	r, err := db.Query("ACCEPT 1000")
	if err != nil {
		t.Fatal(err)
	}
	// Read notifications
	for notify = 1; notify <= notcount; notify++ {
		if !r.Next() {
			if r.Err() != nil {
				t.Fatal(err)
			}
			t.Fatal("expected notification")
		}
		var not_channel string
		var not_payload string
		err = r.Scan(&not_channel, &not_payload)
		if err != nil {
			t.Fatal(err)
		}
		if not_channel != channel {
			t.Fatalf("expected channel:%s got:%s", channel, not_channel)
		}
		if not_payload != payload+strconv.Itoa(notify) {
			t.Fatalf("expected payload:%s got:%s", payload+strconv.Itoa(notify), not_payload)
		}
	}
	// Next must be EOF
	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("expected EOF")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// Test read notification timeout
func TestWaitNotificationTimeout(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	var channel = "channel_name"

	// Listen for notifications from 'channel_name'
	_, err := db.Exec("LISTEN " + channel)
	if err != nil {
		t.Fatal(err)
	}
	// NEVER notify

	// Wait for notification up to timeout
	timeout := 50
	start := time.Now()
	r, err := db.Query(" accept " + strconv.Itoa(timeout))
	if err != nil {
		t.Fatal(err)
	}
	if r.Next() {
		t.Fatal("expected EOF")
	}
	interval := time.Since(start)
	// Checks interval with 1 millisecond precision
	if interval < (time.Duration(timeout-1)*time.Millisecond) ||
		interval > (time.Duration(timeout+1)*time.Millisecond) {
		t.Fatalf("expected to timeout after:%dms got:%dms", timeout, interval/time.Millisecond)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// Test reading notifications before they have been received
func TestWaitNotificationAfter(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	var channel = "channel_name"
	var payload = "PAYLOAD_DATA"
	var notcount = 5

	// Listen for notifications from 'channel_name'
	_, err := db.Exec("LISTEN " + channel)
	if err != nil {
		t.Fatal(err)
	}
	// Send notifications some time after ACCEPT
	go func() {
		time.Sleep(50 * time.Millisecond)
		for i := 1; i <= notcount; i++ {
			_, err = db.Exec(fmt.Sprintf("NOTIFY %s, '%s'", channel, payload+strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	// Reads all notifications
	for notrx := 1; notrx <= notcount; notrx++ {
		// Wait for notifications up to timeout
		r, err := db.Query(" Accept 100 ")
		if err != nil {
			t.Fatal(err)
		}
		// Read notifications
		for {
			if !r.Next() {
				if r.Err() != nil {
					t.Fatal(err)
				}
				break
			}
			var not_channel string
			var not_payload string
			err = r.Scan(&not_channel, &not_payload)
			if err != nil {
				t.Fatal(err)
			}
			if not_channel != channel {
				t.Fatalf("expected channel:%s got:%s", channel, not_channel)
			}
			if not_payload != payload+strconv.Itoa(notrx) {
				t.Fatalf("expected payload:%s got:%s", payload+strconv.Itoa(notrx), not_payload)
			}
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Wait for notification up to timeout
	// Should timeout
	r, err := db.Query(" accept 10")
	if err != nil {
		t.Fatal(err)
	}
	if r.Next() {
		t.Fatal("expected EOF")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// Test if notification response interferes with SQL command
func TestWaitNotificationXX(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()
	var channel = "channel_name"
	var payload = "PAYLOAD_DATA"

	// Listen for notifications from 'channel_name'
	_, err := db.Exec("LISTEN " + channel)
	if err != nil {
		t.Fatal(err)
	}
	// Sends notification
	_, err = db.Exec(fmt.Sprintf("NOTIFY %s, '%s'", channel, payload))
	if err != nil {
		t.Fatal(err)
	}
	// Wait some to backend send the notification response
	time.Sleep(50 * time.Millisecond)
	// Executes an SQL query  
	r, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	// Checks SQL query result
	if !r.Next() {
		t.Fatal("expected result")
	}
	var value int
	err = r.Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("expected value:%d got:%d", 1, value)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Wait for notification
	r, err = db.Query(" accept 10")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Next() {
		t.Fatal("expected notification")
	}
	// Checks notification fields
	var not_channel string
	var not_payload string
	err = r.Scan(&not_channel, &not_payload)
	if err != nil {
		t.Fatal(err)
	}
	if not_channel != channel {
		t.Fatalf("expected channel:%s got:%s", channel, not_channel)
	}
	if not_payload != payload {
		t.Fatalf("expected payload:%s got:%s", payload, not_payload)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
