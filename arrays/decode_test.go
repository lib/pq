package arrays

import "testing"

func TestArrayIntOne(t *testing.T) {
	var v []int

	if err := Unmarshal([]byte("{1}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; v[0] != expected {
		t.Errorf("Expected array[0] to be %d, got %d", expected, v[0])
	}
}

func TestArrayIntTwo(t *testing.T) {
	var v []int

	if err := Unmarshal([]byte("{-1,2}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := -1; v[0] != expected {
		t.Errorf("Expected array[0] to be %d, got %d", expected, v[0])
	}

	if expected := 2; v[1] != expected {
		t.Errorf("Expected array[1] to be %d, got %d", expected, v[1])
	}
}

func TestArrayIntTwoWhitespace(t *testing.T) {
	var v []int

	if err := Unmarshal([]byte("{1, 2}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; v[0] != expected {
		t.Errorf("Expected array[0] to be %d, got %d", expected, v[0])
	}

	if expected := 2; v[1] != expected {
		t.Errorf("Expected array[1] to be %d, got %d", expected, v[1])
	}
}

func TestArrayIntMultidimension(t *testing.T) {
	var v [][]int

	if err := Unmarshal([]byte("{{1}, {2}}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; len(v[0]) != expected {
		t.Errorf("Expected array[0] to have length %d, got %d", expected, len(v[0]))
	}

	if expected := 1; v[0][0] != expected {
		t.Errorf("Expected array[0][0] to be %d, got %d", expected, v[0][0])
	}

	if expected := 1; len(v[1]) != expected {
		t.Errorf("Expected array[1] to have length %d, got %d", expected, len(v[0]))
	}

	if expected := 2; v[1][0] != expected {
		t.Errorf("Expected array[1][0] to be %d, got %d", expected, v[1][0])
	}
}

func TestArrayStringOne(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

func TestArrayUnquotedString(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{hello}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

func TestArrayMixedQuotingStrings(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{hello,\"there world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}

	if expected := "there world"; v[1] != expected {
		t.Errorf("Expected array[1] to be '%s', got '%s'", expected, v[1])
	}
}

func TestArrayNumbersAsStrings(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{123}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "123"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

func TestArrayNullsAsStrings(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"null\", NULL}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "null"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}

	if expected := ""; v[1] != expected {
		t.Errorf("Expected array[1] to be '%s', got '%s'", expected, v[1])
	}
}

func TestArrayStringTwo(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\",\"world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}

	if expected := "world"; v[1] != expected {
		t.Errorf("Expected array[1] to be '%s', got '%s'", expected, v[1])
	}
}

func TestArrayStringTwoWhitespace(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\", \"world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}

	if expected := "world"; v[1] != expected {
		t.Errorf("Expected array[1] to be '%s', got '%s'", expected, v[1])
	}
}

func TestArrayStringMultidimension(t *testing.T) {
	var v [][]string

	if err := Unmarshal([]byte("{{\"hello\"}, {\"world\"}}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; len(v[0]) != expected {
		t.Errorf("Expected array[0] to have length %d, got %d", expected, len(v[0]))
	}

	if expected := "hello"; v[0][0] != expected {
		t.Errorf("Expected array[0][0] to be '%s', got '%s'", expected, v[0][0])
	}

	if expected := 1; len(v[1]) != expected {
		t.Errorf("Expected array[1] to have length %d, got %d", expected, len(v[0]))
	}

	if expected := "world"; v[1][0] != expected {
		t.Errorf("Expected array[1][0] to be '%s', got '%s'", expected, v[1][0])
	}
}

func TestArrayFloat(t *testing.T) {
	var v []float64

	if err := Unmarshal([]byte("{-1.2, 0.2, 4.54356}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 3; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := -1.2; v[0] != expected {
		t.Errorf("Expected array[0] to be %f, got %f", expected, v[0])
	}

	if expected := 0.2; v[1] != expected {
		t.Errorf("Expected array[1] to be %f, got %f", expected, v[1])
	}

	if expected := 4.54356; v[2] != expected {
		t.Errorf("Expected array[2] to be %f, got %f", expected, v[2])
	}
}

func TestArrayBool(t *testing.T) {
	var v []bool

	if err := Unmarshal([]byte("{t, f}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := true; v[0] != expected {
		t.Errorf("Expected array[0] to be %t, got %t", expected, v[0])
	}

	if expected := false; v[1] != expected {
		t.Errorf("Expected array[1] to be %t, got %t", expected, v[1])
	}
}

func TestArrayBoolWithNulls(t *testing.T) {
	var v []bool

	if err := Unmarshal([]byte("{t, NULL}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := true; v[0] != expected {
		t.Errorf("Expected array[0] to be %t, got %t", expected, v[0])
	}

	if expected := false; v[1] != expected {
		t.Errorf("Expected array[1] to be %t, got %t", expected, v[1])
	}
}

func TestArrayBoolAsStrings(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{t, f}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "t"; v[0] != expected {
		t.Errorf("Expected array[0] to be %t, got %t", expected, v[0])
	}

	if expected := "f"; v[1] != expected {
		t.Errorf("Expected array[1] to be %t, got %t", expected, v[1])
	}
}

func TestArrayBoolWithNullsIntoInterfaceArray(t *testing.T) {
	var v []interface{}

	if err := Unmarshal([]byte("{t, NULL}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 2; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := true; v[0] != expected {
		t.Errorf("Expected array[0] to be %t, got %t", expected, v[0])
	}

	if v[1] != nil {
		t.Errorf("Expected array[1] to be nil, got %t", v[1])
	}
}

func TestArrayEscapedSlash(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\\\\world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello\\world"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

func TestArrayEscapedOpenArray(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\\{world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello{world"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

func TestArrayEscapedCloseArray(t *testing.T) {
	var v []string

	if err := Unmarshal([]byte("{\"hello\\}world\"}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := "hello}world"; v[0] != expected {
		t.Errorf("Expected array[0] to be '%s', got '%s'", expected, v[0])
	}
}

type testObj struct {
	id int
}

func TestArrayDecodeIntoObject(t *testing.T) {
	v := testObj{}

	if err := Unmarshal([]byte("{\"hello\\}world\"}"), &v); err == nil {
		t.Fatal("Expected error, didn't get one")
	}

	v2 := &testObj{}

	if err := Unmarshal([]byte("{\"hello\\}world\"}"), &v2); err == nil {
		t.Fatal("Expected error, didn't get one")
	}
}

func TestArrayDecodeIntoPreallocatedSlice(t *testing.T) {
	v := make([]int, 0, 10)

	if err := Unmarshal([]byte("{1}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 1; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; v[0] != expected {
		t.Errorf("Expected array[0] to be %d, got %d", expected, v[0])
	}
}

func TestArrayDecodeIntoArray(t *testing.T) {
	v := [10]int{}

	if err := Unmarshal([]byte("{1}"), &v); err != nil {
		t.Fatalf("Unexpected error, %v", err)
	}

	if expected := 10; len(v) != expected {
		t.Errorf("Expected array to have length %d, got %d", expected, len(v))
	}

	if expected := 1; v[0] != expected {
		t.Errorf("Expected array[0] to be %d, got %d", expected, v[0])
	}
}
