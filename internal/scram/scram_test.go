package scram

import (
	"crypto/sha1"
	"strings"
	"testing"
)

func TestExamples(t *testing.T) {
	tests := [][]string{
		{
			"U: user pencil",
			"N: fyko+d2lbbFgONRv9qkxdawL",
			"C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			"S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			"C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			"S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=",
		},
		{
			"U: root fe8c89e308ec08763df36333cbf5d3a2",
			"N: OTcxNDk5NjM2MzE5",
			"C: n,,n=root,r=OTcxNDk5NjM2MzE5",
			"S: r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,s=XRDkVrFC9JuL7/F4tG0acQ==,i=10000",
			"C: c=biws,r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,p=6y1jp9R7ETyouTXS9fW9k5UHdBc=",
			"S: v=LBnd9dUJRxdqZiEq91NKP3z/bHA=",
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			auth := strings.Fields(tt[0][3:])
			client := NewClient(sha1.New, auth[0], auth[1])
			first, done := true, false
			for _, step := range tt[1:] {
				switch step[:3] {
				case "N: ":
					client.setNonce([]byte(step[3:]))
				case "C: ":
					if first {
						first = false
						done = client.Step(nil)
					}
					if done {
						t.Fatal()
					}
					if client.Err() != nil {
						t.Fatal(client.Err())
					}
					if have := client.Out(); string(have) != string(step[3:]) {
						t.Fatalf("\nhave: %q\nwant: %q", have, step[3:])
					}
				case "S: ":
					first = false
					done = client.Step([]byte(step[3:]))
				default:
					t.Fatalf("invalid test line: %q", step)
				}
			}
			if !done {
				t.Fatal()
			}
			if client.Err() != nil {
				t.Fatal(client.Err())
			}
		})
	}
}
