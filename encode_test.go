package pq

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/internal/pqtime"
	"github.com/lib/pq/oid"
)

func TestTimeScan(t *testing.T) {
	tests := []struct {
		typ, in string
		want    time.Time
	}{
		{"time", "11:59:59", time.Date(0, 1, 1, 11, 59, 59, 0, time.UTC)},
		{"time", "24:00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"time", "24:00:00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"time", "24:00:00.0", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"time", "24:00:00.000000", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},

		{"timetz", "11:59:59+00:00", time.Date(0, 1, 1, 11, 59, 59, 0, time.UTC)},
		{"timetz", "11:59:59+04:00", time.Date(0, 1, 1, 11, 59, 59, 0, time.FixedZone("+04", 4*60*60))},
		{"timetz", "11:59:59+04:01:02", time.Date(0, 1, 1, 11, 59, 59, 0, time.FixedZone("+04:01:02", 4*60*60+1*60+2))},
		{"timetz", "11:59:59-04:01:02", time.Date(0, 1, 1, 11, 59, 59, 0, time.FixedZone("-04:01:02", -(4*60*60+1*60+2)))},
		{"timetz", "24:00+00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"timetz", "24:00Z", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"timetz", "24:00-04:00", time.Date(0, 1, 2, 0, 0, 0, 0, time.FixedZone("-04", -4*60*60))},
		{"timetz", "24:00:00+00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"timetz", "24:00:00.0+00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"timetz", "24:00:00.000000+00", time.Date(0, 1, 2, 0, 0, 0, 0, time.UTC)},

		{"timestamp", "2020-03-04 24:00:00", time.Date(2020, 3, 5, 0, 0, 0, 0, time.FixedZone("", 0))},
		{"timestamptz", "2020-03-04 24:00:00+02", time.Date(2020, 3, 4, 22, 0, 0, 0, time.UTC)},
		{"timestamptz", "2020-03-04 24:00:00-02", time.Date(2020, 3, 5, 2, 0, 0, 0, time.UTC)},

		{"timestamptz", "2001-02-03T12:13:14 UTC", time.Date(2001, 2, 3, 12, 13, 14, 0, time.UTC)},
		{"timestamptz", "2001-02-03T12:13:14 Etc/UTC", time.Date(2001, 2, 3, 12, 13, 14, 0, time.UTC)},
		{"timestamptz", "2001-02-03T12:13:14 Asia/Makassar", time.Date(2001, 2, 3, 04, 13, 14, 0, time.UTC)},
	}

	db := pqtest.MustDB(t)
	t.Parallel()
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			have := pqtest.QueryRow[time.Time](t, db, fmt.Sprintf(`select $1::%s as t`, tt.typ), tt.in)["t"]
			if !tt.want.Equal(have) {
				t.Errorf("\nhave: %s\nwant: %s", have, tt.want)
			}
		})
	}
}

func TestTimeWithZone(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	for _, locName := range []string{"UTC", "America/Chicago", "America/New_York", "Australia/Darwin", "Australia/Perth"} {
		loc, err := time.LoadLocation(locName)
		if err != nil {
			t.Fatalf("could not load time zone %s", locName)
		}

		refTime := time.Date(2012, 11, 6, 10, 23, 42, 123456000, loc)

		for _, pgTimezone := range []string{"America/New_York", "Australia/Darwin"} {
			// Switch timezone to test different output timestamp formats.
			pqtest.Exec(t, db, fmt.Sprintf("set time zone '%s'", pgTimezone))

			var have time.Time
			err := db.QueryRow("select $1::timestamp with time zone", refTime).Scan(&have)
			if err != nil {
				t.Fatal(err)
			}
			if !refTime.Equal(have) {
				t.Fatalf("\nhave: %s\nwant: %s", have, refTime)
			}

			// Check that the time zone is set correctly based on Timezone.
			pgLoc, err := time.LoadLocation(pgTimezone)
			if err != nil {
				t.Fatalf("could not load time zone %s", pgLoc)
			}
			in := refTime.In(pgLoc)
			if in.String() != have.String() {
				t.Fatalf("\nhave: %s\nwant: %s", in, have)
			}
		}
	}

	t.Run("UTC aliases", func(t *testing.T) {
		for _, z := range []string{"UTC", "Etc/UTC", "Etc/Universal", "Etc/Zulu", "Etc/UCT"} {
			t.Run(z, func(t *testing.T) {
				have := pqtest.QueryRow[time.Time](t, pqtest.MustDB(t, "timezone="+z),
					`select '2001-02-03 12:13:14Z'::timestamptz`)["timestamptz"]
				if l := have.Location(); l != time.UTC {
					t.Errorf("%s", l)
				}
			})
		}
	})
}

func TestTimeWithoutZone(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	tests := []struct {
		in   string
		want time.Time
	}{
		{"2000-01-01T00:00:00", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"2013-01-04 20:14:58.80033", time.Date(2013, 1, 4, 20, 14, 58, 800330000, time.UTC)}, // Higher precision time
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have := pqtest.QueryRow[time.Time](t, db, "select $1::timestamp", tt.in)["timestamp"]
			if !have.Equal(tt.want) {
				t.Fatalf("\nhave: %s\nwant: %s", have, tt.want)
			}
		})
	}
}

func TestTimeInfinity(t *testing.T) {
	db := pqtest.MustDB(t)

	// Test without registering
	t.Run("not registered", func(t *testing.T) {
		tests := []struct {
			query   string
			want    any
			wantErr string
		}{
			{"select '-infinity'::timestamp", "-infinity", `unsupported Scan, storing driver.Value type []uint8 into type *time.Time`},
			{"select '-infinity'::timestamptz", "-infinity", `unsupported Scan, storing driver.Value type []uint8 into type *time.Time`},
			{"select 'infinity'::timestamp", "infinity", `unsupported Scan, storing driver.Value type []uint8 into type *time.Time`},
			{"select 'infinity'::timestamptz", "infinity", `unsupported Scan, storing driver.Value type []uint8 into type *time.Time`},
		}

		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				{ // PostgreSQL just returns "infinity" as text, which we don't recognize.
					// TODO: surely we can give a better errors?
					var have time.Time
					err := db.QueryRow(tt.query).Scan(&have)
					if !pqtest.ErrorContains(err, tt.wantErr) {
						t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
					}
				}

				{ // We can scan as string/[]byte/any though
					var a any
					err := db.QueryRow(tt.query).Scan(&a)
					if err != nil {
						t.Fatal(err)
					}
					have, ok := a.([]byte)
					if !ok {
						t.Fatalf("wrong type: %#v", have)
					}
					if string(have) != tt.want {
						t.Errorf("\nhave: %s\nwant: %s", have, tt.want)
					}
				}

			})
		}
	})

	t.Run("works", func(t *testing.T) {
		t.Cleanup(disableInfinityTS)
		infNeg := time.Date(1500, time.January, 1, 0, 0, 0, 0, time.UTC)
		infPos := time.Date(2500, time.January, 1, 0, 0, 0, 0, time.UTC)
		EnableInfinityTs(infNeg, infPos)

		tests := []struct {
			query      string
			want       time.Time
			wantString string
		}{
			{`select 'infinity'::timestamp`, infPos, "infinity"},
			{`select 'infinity'::timestamptz`, infPos, "infinity"},
			{`select '-infinity'::timestamp`, infNeg, "-infinity"},
			{`select '-infinity'::timestamptz`, infNeg, "-infinity"},
		}

		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				var have time.Time
				err := db.QueryRow(tt.query).Scan(&have)
				if err != nil {
					t.Fatal(err)
				}
				if !have.Equal(tt.want) {
					t.Errorf("\nhave: %s\nwant: %s", have, tt.want)
				}

				// Round-trip
				var haveScan string
				err = db.QueryRow("select $1::timestamp::text", have).Scan(&haveScan)
				if err != nil {
					t.Fatal(err)
				}
				if haveScan != tt.wantString {
					t.Errorf("\nhave: %s\nwant: %s", have, tt.wantString)
				}
			})
		}
	})

	t.Run("negative smaller", func(t *testing.T) {
		var r any
		func() {
			defer func() { r = recover() }()
			EnableInfinityTs(time.Now().Add(time.Hour), time.Now())
		}()
		have, ok := r.(string)
		if !ok {
			t.Fatalf("wrong panic type: %#v", r)
		}
		if want := "negative value must be smaller"; !strings.Contains(have, want) {
			t.Errorf("\nhave: %s\nwant: %s", have, want)
		}
	})
}

func TestDecodeScan(t *testing.T) {
	var (
		uuid     = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
		uuidb, _ = hex.DecodeString(strings.ReplaceAll(uuid, "-", ""))
	)

	tests := []struct {
		query      string
		params     []any
		want       any
		wantErr    string
		wantErrBin string
	}{
		{`select $1::text`, []any{"hello\x00world"}, nil, `invalid byte sequence`, `X`},
		{`select $1::text`, []any{[]byte("hello world")}, "hello world", ``, `X`},
		{`select $1::bytea`, []any{[]byte("hello world")}, []byte("hello world"), ``, `X`},

		{`select $1::uuid`, []any{[]byte(uuid)}, []byte(uuid), ``, `pq: incorrect binary data format in bind parameter 1 (22P03)`},
		{`select $1::uuid`, []any{uuidb}, []byte(uuid), `invalid byte sequence`, ``},
		{`select $1::uuid`, []any{uuid}, []byte(uuid), ``, `X`},

		{`select $1::int`, []any{fmt.Append(nil, 12345678)}, int64(12345678), ``, `pq: incorrect binary data format in bind parameter 1 (22P03)`},
		{`select $1::int`, []any{[]byte{0x00, 0xbc, 0x61, 0x4e}}, int64(12345678), `invalid byte sequence`, ``},
	}

	t.Parallel()
	db := pqtest.MustDB(t)
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			var have any
			err := db.QueryRow(tt.query, tt.params...).Scan(&have)
			wantErr := tt.wantErr
			if pqtest.ForceBinaryParameters() && tt.wantErrBin != "X" {
				wantErr = tt.wantErrBin
			}
			if !pqtest.ErrorContains(err, wantErr) {
				t.Fatalf("wrong error:\nhave: %s\nwant: %s", err, wantErr)
			}
			if wantErr == "" && !reflect.DeepEqual(have, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	tests := []struct {
		typ     oid.Oid
		format  format
		in      []byte
		want    any
		wantErr string
	}{
		{oid.T_char, formatText, []byte("hello world"), "hello world", ``},
		{oid.T_bpchar, formatText, []byte("hello world"), "hello world", ``},
		{oid.T_varchar, formatText, []byte("hello world"), "hello world", ``},
		{oid.T_text, formatText, []byte("hello world"), "hello world", ``},

		{oid.T_uuid, formatBinary, []byte{0x12, 0x34}, ([]byte)(nil), `pq: unable to decode uuid; bad length: 2`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			have, err := decode(nil, tt.in, tt.typ, tt.format)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			if !reflect.DeepEqual(have, tt.want) {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func TestEncodeBytea(t *testing.T) {
	have, err := encode([]byte("\\x\x00\x01\x02\xFF\xFEabcdefg0123"), oid.T_bytea)
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("\\x5c78000102fffe6162636465666730313233"); !bytes.Equal(want, have) {
		t.Errorf("\nhave: %v\nwant: %v", have, want)
	}
}

func TestByteaOutputFormats(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	for _, format := range []string{"hex", "escape"} {
		t.Run("", func(t *testing.T) {
			// Use transaction to avoid relying on getting the same connection
			tx := pqtest.Begin(t, db)
			pqtest.Exec(t, tx, `set local bytea_output to `+format)

			rows := pqtest.Query[[]byte](t, tx, `select decode('5c7800ff6162630108', 'hex')`)
			want := []byte("\x5c\x78\x00\xff\x61\x62\x63\x01\x08")
			if !bytes.Equal(rows[0]["decode"], want) {
				t.Errorf("\nhave: %v\nwant: %v", rows[0]["decode"], want)
			}

			{ // Same but with Prepare
				stmt := pqtest.Prepare(t, tx, `select decode('5c7800ff6162630108', 'hex')`)
				rows, err := stmt.Query()
				if err != nil {
					t.Fatal(err)
				}
				if !rows.Next() {
					t.Fatal(rows.Err())
				}
				var have []byte
				err = rows.Scan(&have)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(have, want) {
					t.Errorf("\nhave: %v\nwant: %v", have, want)
				}
			}
		})
	}
}

func TestAppendEncodedText(t *testing.T) {
	must := func(buf []byte, x any) []byte {
		t.Helper()
		buf, err := appendEncodedText(buf, x)
		if err != nil {
			t.Fatal(err)
		}
		return buf
	}

	buf := must(nil, int64(10))
	buf = append(buf, '\t')
	buf = must(buf, 42.0000000001)
	buf = append(buf, '\t')
	buf = must(buf, "hello\tworld")
	buf = append(buf, '\t')
	buf = must(buf, []byte{0, 128, 255})
	if string(buf) != "10\t42.0000000001\thello\\tworld\t\\\\x0080ff" {
		t.Fatal(string(buf))
	}
}

func TestAppendEscapedText(t *testing.T) {
	buf := appendEscapedText(nil, "hallo\tescape")
	buf = appendEscapedText(buf, "hallo\\tescape\n")
	buf = appendEscapedText(buf, "\n\r\t\f")
	if string(buf) != "hallo\\tescapehallo\\\\tescape\\n\\n\\r\\t\f" {
		t.Fatal(string(buf))
	}
}

func BenchmarkDecode(b *testing.B) {
	b.Run("int64", func(b *testing.B) {
		x := []byte("1234")
		for i := 0; i < b.N; i++ {
			decode(nil, x, oid.T_int8, formatText)
		}
	})
	b.Run("float64", func(b *testing.B) {
		x := []byte("3.14159")
		for i := 0; i < b.N; i++ {
			decode(nil, x, oid.T_float8, formatText)
		}
	})
	b.Run("bool", func(b *testing.B) {
		x := []byte{'t'}
		for i := 0; i < b.N; i++ {
			decode(nil, x, oid.T_bool, formatText)
		}
	})
	b.Run("uuid_binary", func(b *testing.B) {
		x := []byte{0x03, 0xa3, 0x52, 0x2f, 0x89, 0x28, 0x49, 0x87, 0x84, 0xd6, 0x93, 0x7b, 0x36, 0xec, 0x27, 0x6f}
		for i := 0; i < b.N; i++ {
			decodeUUIDBinary(x)
		}
	})
	b.Run("timestamptz", func(b *testing.B) {
		x := []byte("2013-09-17 22:15:32.360754-07")
		for i := 0; i < b.N; i++ {
			decode(&parameterStatus{}, x, oid.T_timestamptz, formatText)
		}
	})
	b.Run("timestamptz_thread", func(b *testing.B) {
		oldProcs := runtime.GOMAXPROCS(0)
		defer runtime.GOMAXPROCS(oldProcs)
		runtime.GOMAXPROCS(runtime.NumCPU())
		pqtime.Reset()

		x := []byte("2013-09-17 22:15:32.360754-07")
		f := func(wg *sync.WaitGroup, loops int) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				decode(&parameterStatus{}, x, oid.T_timestamptz, formatText)
			}
		}

		wg := &sync.WaitGroup{}
		b.ResetTimer()
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go f(wg, b.N/10)
		}
		wg.Wait()
	})
}

func BenchmarkEncode(b *testing.B) {
	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(int64(1234), oid.T_int8)
		}
	})
	b.Run("float64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(3.14159, oid.T_float8)
		}
	})
	b.Run("bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(true, oid.T_bool)
		}
	})
	b.Run("timestamptz", func(b *testing.B) {
		x := time.Date(2001, time.January, 1, 0, 0, 0, 0, time.Local)
		for i := 0; i < b.N; i++ {
			encode(x, oid.T_timestamptz)
		}
	})
	b.Run("bytea_hex", func(b *testing.B) {
		x := []byte("abcdefghijklmnopqrstuvwxyz")
		for i := 0; i < b.N; i++ {
			encode(x, oid.T_bytea)
		}
	})
	b.Run("bytea_escape", func(b *testing.B) {
		x := []byte("abcdefghijklmnopqrstuvwxyz")
		for i := 0; i < b.N; i++ {
			encode(x, oid.T_bytea)
		}
	})
}

func BenchmarkAppendEscapedText(b *testing.B) {
	b.Run("100 lines", func(b *testing.B) {
		s := strings.Repeat("123456789\n", 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			appendEscapedText(nil, s)
		}
	})
	b.Run("noescape", func(b *testing.B) {
		s := strings.Repeat("1234567890", 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			appendEscapedText(nil, s)
		}
	})
}
