package pq

import (
	"reflect"
	"testing"
)

func TestFloat64ArrayNon1Based(t *testing.T) {
	var a Float64Array
	src := []byte("[0:5]={0,1,2,3,4,5}")
	err := a.Scan(src)
	if err != nil {
		t.Fatalf("Failed to scan non-1-based array: %v", err)
	}
	expected := Float64Array{0, 1, 2, 3, 4, 5}
	if !reflect.DeepEqual(a, expected) {
		t.Errorf("Expected %v, got %v", expected, a)
	}
}
