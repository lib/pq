package pq

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/lib/pq/oid"
)

func TestCommandCompleteResponseParserMatchesStringParser(t *testing.T) {
	tags := []string{
		"ALTER TABLE",
		"INSERT 0 1",
		"UPDATE 100",
		"SELECT 100",
		"FETCH 100",
		"COPY",
		"MERGE 7",
		"UNKNOWNCOMMANDTAG",
		"INSERT 1",
		"UPDATE 0 1",
		"SELECT foo",
	}

	for _, tag := range tags {
		t.Run(tag, func(t *testing.T) {
			wantResult, wantCommand, wantErr := new(conn).parseComplete(tag)
			payload := readBuf(append(append([]byte(nil), tag...), 0))
			haveResult, haveCommand, haveErr := new(conn).parseCompleteResponse(&payload)

			if errorString(haveErr) != errorString(wantErr) {
				t.Fatalf("error: got %q, want %q", haveErr, wantErr)
			}
			if haveCommand != wantCommand {
				t.Fatalf("command: got %q, want %q", haveCommand, wantCommand)
			}
			if len(payload) != 0 {
				t.Fatalf("unconsumed payload: %q", []byte(payload))
			}
			if wantErr != nil {
				return
			}
			haveRows, err := haveResult.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			wantRows, err := wantResult.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if haveRows != wantRows {
				t.Fatalf("rows affected: got %d, want %d", haveRows, wantRows)
			}
		})
	}
}

func TestAppendEncodedParameterMatchesEncode(t *testing.T) {
	now := time.Date(2026, time.August, 2, 12, 34, 56, 789, time.FixedZone("test", -7*60*60))
	tests := []struct {
		name  string
		value any
		typ   oid.Oid
	}{
		{"int64", int64(-9223372036854775807), oid.T_int8},
		{"float64", math.Pi, oid.T_float8},
		{"string", "benchmark", oid.T_text},
		{"string bytea", "benchmark", oid.T_bytea},
		{"bytes", []byte("benchmark"), oid.T_text},
		{"bytes bytea", []byte{0, 1, 2, 0xfe, 0xff}, oid.T_bytea},
		{"nil bytes", []byte(nil), oid.T_bytea},
		{"true", true, oid.T_bool},
		{"false", false, oid.T_bool},
		{"time", now, oid.T_timestamptz},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, wantErr := encode(tt.value, tt.typ)
			var want []byte
			if wantErr == nil {
				if encoded == nil {
					want = []byte{0xff, 0xff, 0xff, 0xff}
				} else {
					want = binary.BigEndian.AppendUint32(nil, uint32(len(encoded)))
					want = append(want, encoded...)
				}
			}

			w := new(writeBuf)
			haveErr := appendEncodedParameter(w, tt.value, tt.typ)
			if errorString(haveErr) != errorString(wantErr) {
				t.Fatalf("error: got %q, want %q", haveErr, wantErr)
			}
			if !bytes.Equal(w.buf, want) {
				t.Fatalf("wire value: got %x, want %x", w.buf, want)
			}
		})
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
