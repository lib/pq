// +build go1.9

package pq

import (
	"reflect"
	"testing"
)

func TestArrayArg(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expected := []int64{245, 231}

	r, err := db.Query("SELECT $1::int[]", expected)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	var i []int64
	if err := r.Scan(Array(&i)); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(i, expected) {
		t.Errorf("expect %v, got %v", expected, i)
	}
}
