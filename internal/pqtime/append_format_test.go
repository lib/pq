package pqtime

import (
	"testing"
	"time"
)

func TestAppendFormat(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{"zero", time.Time{}, "prefix:0001-01-01 00:00:00Z"},
		{
			"fraction and offset",
			time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)),
			"prefix:2001-02-03 04:05:06.123456789+02:00",
		},
		{
			"offset seconds",
			time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))),
			"prefix:2001-02-03 04:05:06-07:30:09",
		},
		{
			"BC",
			time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.UTC),
			"prefix:0001-02-03 04:05:06.123456789Z BC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AppendFormat([]byte("prefix:"), tt.in)
			if string(got) != tt.want {
				t.Fatalf("AppendFormat() = %q; want %q", got, tt.want)
			}
		})
	}
}
