//go:build go1.9

package pq

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestArrayArg(t *testing.T) {
	db := pqtest.MustDB(t)
	defer db.Close()

	tests := []struct {
		pgType  string
		in, out any
	}{
		{"int[]", []int{245, 231}, []int64{245, 231}},
		{"int[]", &[]int{245, 231}, []int64{245, 231}},
		{"int[]", []int64{245, 231}, nil},
		{"int[]", &[]int64{245, 231}, []int64{245, 231}},
		{"varchar[]", []string{"hello", "world"}, nil},
		{"varchar[]", &[]string{"hello", "world"}, []string{"hello", "world"}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if tt.out == nil {
				tt.out = tt.in
			}

			// XXX: hangs in recvMessage()'s io.ReadFull
			// Not when using -run=TestArrayArg though, then the next test hangs
			r, err := db.Query(fmt.Sprintf("SELECT $1::%s", tt.pgType), tt.in)
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

			defer func() {
				if r.Next() {
					t.Fatal("unexpected row")
				}
			}()

			have := reflect.New(reflect.TypeOf(tt.out))
			if err := r.Scan(Array(have.Interface())); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tt.out, have.Elem().Interface()) {
				t.Errorf("\nhave: %v\nwant %v", have, tt.out)
			}
		})
	}

}
