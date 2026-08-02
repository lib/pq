package hstore

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestAllocationHstoreTextValueEscaping(t *testing.T) {
	for _, tt := range []struct {
		name  string
		key   string
		value sql.NullString
		want  string
	}{
		{"plain", "plain", sql.NullString{String: "value", Valid: true}, `"plain"=>"value"`},
		{"escaped", `quote"\\key`, sql.NullString{String: `value"\\tail`, Valid: true}, `"quote\"\\\\key"=>"value\"\\\\tail"`},
		{"empty", "", sql.NullString{String: "", Valid: true}, `""=>""`},
		{"unicode", "κλειδί", sql.NullString{String: "τιμή", Valid: true}, `"κλειδί"=>"τιμή"`},
		{"null", "null", sql.NullString{}, `"null"=>NULL`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			value, err := (Hstore{Map: map[string]sql.NullString{tt.key: tt.value}}).Value()
			if err != nil {
				t.Fatal(err)
			}
			if got := string(value.([]byte)); got != tt.want {
				t.Fatalf("Hstore.Value() = %q; want %q", got, tt.want)
			}
		})
	}
}

func TestAllocationHstoreTextValueRoundTrip(t *testing.T) {
	want := map[string]sql.NullString{
		"plain":       {String: "value", Valid: true},
		`quote"key`:   {String: `slash\value`, Valid: true},
		"unicode-αβγ": {String: "δεζ", Valid: true},
		"empty":       {String: "", Valid: true},
		"null":        {Valid: false},
	}
	encoded, err := (Hstore{Map: want}).Value()
	if err != nil {
		t.Fatal(err)
	}

	var got Hstore
	if err := got.Scan(encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Map, want) {
		t.Fatalf("Hstore.Scan(Hstore.Value()) = %#v; want %#v", got.Map, want)
	}
}

func TestAllocationHstoreScanReusesTemporaryBuffersSafely(t *testing.T) {
	input := []byte(`"first"=>"short","second-key"=>"a longer second value","escaped\"key"=>"slash\\value","unicode-αβγ"=>"δεζ","quoted-null"=>"NuLl","null-key"=>NuLl`)
	var h Hstore
	if err := h.Scan(input); err != nil {
		t.Fatal(err)
	}
	want := map[string]sql.NullString{
		"first":       {String: "short", Valid: true},
		"second-key":  {String: "a longer second value", Valid: true},
		`escaped"key`: {String: `slash\value`, Valid: true},
		"unicode-αβγ": {String: "δεζ", Valid: true},
		"quoted-null": {String: "NuLl", Valid: true},
		"null-key":    {Valid: false},
	}
	if !reflect.DeepEqual(h.Map, want) {
		t.Fatalf("Hstore.Scan() = %#v; want %#v", h.Map, want)
	}
}

func TestAllocationHstoreScanGrowsTemporaryBuffersSafely(t *testing.T) {
	want := map[string]sql.NullString{
		strings.Repeat(`key"\`, 12): {String: strings.Repeat(`value"\`, 12), Valid: true},
		"tail":                      {String: "short", Valid: true},
	}
	encoded, err := (Hstore{Map: want}).Value()
	if err != nil {
		t.Fatal(err)
	}

	var got Hstore
	if err := got.Scan(encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Map, want) {
		t.Fatalf("Hstore.Scan(Hstore.Value()) = %#v; want %#v", got.Map, want)
	}
}

func TestAllocationHstoreBinaryValueExactSizing(t *testing.T) {
	for _, tt := range []struct {
		key   string
		value sql.NullString
	}{
		{"key", sql.NullString{String: "value", Valid: true}},
		{"κλειδί", sql.NullString{String: "τιμή", Valid: true}},
		{"", sql.NullString{String: "", Valid: true}},
		{"key", sql.NullString{}},
	} {
		h := Hstore{Map: map[string]sql.NullString{tt.key: tt.value}}
		got, err := h.BinaryValue()
		if err != nil {
			t.Fatal(err)
		}
		want := binary.BigEndian.AppendUint32(nil, 1)
		want = binary.BigEndian.AppendUint32(want, uint32(len(tt.key)))
		want = append(want, tt.key...)
		if tt.value.Valid {
			want = binary.BigEndian.AppendUint32(want, uint32(len(tt.value.String)))
			want = append(want, tt.value.String...)
		} else {
			want = binary.BigEndian.AppendUint32(want, math.MaxUint32)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Hstore.BinaryValue() = %x; want %x", got, want)
		}
	}
}
