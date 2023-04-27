//go:build go1.9
// +build go1.9

package pq

import (
	"fmt"
	"reflect"
	"testing"
)

func TestArrayArg(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	for _, tc := range []struct {
		pgType  string
		in, out interface{}
	}{
		{
			pgType: "int[]",
			in:     []int{245, 231},
			out:    []int64{245, 231},
		},
		{
			pgType: "int[]",
			in:     &[]int{245, 231},
			out:    []int64{245, 231},
		},
		{
			pgType: "int[]",
			in:     []int64{245, 231},
		},
		{
			pgType: "int[]",
			in:     &[]int64{245, 231},
			out:    []int64{245, 231},
		},
		{
			pgType: "varchar[]",
			in:     []string{"hello", "world"},
		},
		{
			pgType: "varchar[]",
			in:     &[]string{"hello", "world"},
			out:    []string{"hello", "world"},
		},
	} {
		if tc.out == nil {
			tc.out = tc.in
		}
		t.Run(fmt.Sprintf("%#v", tc.in), func(t *testing.T) {
			r, err := db.Query(fmt.Sprintf("SELECT $1::%s", tc.pgType), tc.in)
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

			got := reflect.New(reflect.TypeOf(tc.out))
			if err := r.Scan(Array(got.Interface())); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tc.out, got.Elem().Interface()) {
				t.Errorf("got %v, want %v", got, tc.out)
			}
		})
	}

}
