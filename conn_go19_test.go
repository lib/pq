// +build go1.9

package pq

import (
	"reflect"
	"testing"
)

func TestArrayArg(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	for _, tc := range []struct {
		name string
		want interface{}
	}{
		{
			name: "array",
			want: &[...]int64{245, 231},
		},
		{
			name: "slice",
			want: &[]int64{245, 231},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := db.Query("SELECT $1::int[]", tc.want)
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

			got := reflect.New(reflect.TypeOf(tc.want)).Interface()
			if err := r.Scan(Array(got)); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tc.want, got) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}

}
