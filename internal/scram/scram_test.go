package scram

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"regexp"
	"strings"
	"testing"
)

func TestExamples(t *testing.T) {
	tests := []struct {
		hash              func() hash.Hash
		user, pass, nonce string
		setIter           []uint
		wantClientFirst   string
		serverFirst       string
		wantClientFinal   string
		serverFinal       string
		wantErr           string
	}{
		{ // Examples from RFC5802
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			serverFinal:     "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=",
		},
		{
			hash:            sha1.New,
			user:            "root",
			pass:            "fe8c89e308ec08763df36333cbf5d3a2",
			nonce:           "OTcxNDk5NjM2MzE5",
			wantClientFirst: "n,,n=root,r=OTcxNDk5NjM2MzE5",
			serverFirst:     "r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,s=XRDkVrFC9JuL7/F4tG0acQ==,i=10000",
			wantClientFinal: "c=biws,r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,p=6y1jp9R7ETyouTXS9fW9k5UHdBc=",
			serverFinal:     "v=LBnd9dUJRxdqZiEq91NKP3z/bHA=",
		},
		{ // Example as sha256
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=qQRLRHGPDGjB+7iVAE7NNi5xEoHKHuLCHPNQ8BTmvds=",
			serverFinal:     "v=XKW6VuW1FANROQabnJBz1KaeCnQL/HZByQtX/iU+o30=",
		},
		{ // server-final message with error set
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			serverFinal:     "e=oh noes",
			wantErr:         `oh noes`,
		},
		{ // Iteration count lower boundary
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			setIter:         []uint{0, 100},
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=1",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=VHKBZlS7NnLf7FWvPC3oebt9s2B4jTDxPEcQePFoTfA=",
			serverFinal:     "v=Sn7DPipwU7QHGh5KA2GMj162aySiqN/Wpt9871lguWI=",
		}, {
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=0",
			wantErr:         `server sent an invalid SCRAM-SHA-256 iteration count: "i=0"`,
		}, {
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=999",
			wantErr:         `server iteration count 999 lower than minimum of 1000`,
		},
		{ // Iteration count upper boundary
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=10000000",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=zttdALwd6KHmXFkNaSl68ZQe/16yoec9/KCWwnHPMxg=",
			serverFinal:     "v=efY1v5jENR1NCYtlv13NkVNJ+Z8UeAWWzhULoGgxL/A=",
		}, {
			hash:            sha256.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=10000001",
			wantErr:         `server iteration count 10000001 higher than maximum of 10000000`,
		},
		{ // Various malformed server-first messages
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096,x=abc",
			wantErr:         `expected 3 fields in first SCRAM-SHA-256 server message, got 4: "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096,x=abc"`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,x=4096",
			wantErr:         `server sent an invalid SCRAM-SHA-256 iteration count: "x=4096"`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,x=QSXCR+Q6sek8bf92,i=4096",
			wantErr:         `server sent an invalid SCRAM-SHA-256 salt: "x=QSXCR+Q6sek8bf92"`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "x=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantErr:         `server sent an invalid SCRAM-SHA-256 nonce: "x=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=Xyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantErr:         `server SCRAM-SHA-256 nonce is not prefixed by client nonce; have: "Xyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=Q=XCR+Q6sek8bf92,i=4096",
			wantErr:         `decoding SCRAM-SHA-256 salt sent by server: "s=Q=XCR+Q6sek8bf92": illegal base64 data at input byte 1`,
		},
		{ // Various malformed server-final messages
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			serverFinal:     "x=rmF9pqV8S7suAoZWja4dJRkFsKQ=",
			wantErr:         `unsupported SCRAM-SHA-256 final message from server: "x=rmF9pqV8S7suAoZWja4dJRkFsKQ="`,
		}, {
			hash:            sha1.New,
			user:            "user",
			pass:            "pencil",
			nonce:           "fyko+d2lbbFgONRv9qkxdawL",
			wantClientFirst: "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			serverFirst:     "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			wantClientFinal: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			serverFinal:     "v=XmF9pqV8S7suAoZWja4dJRkFsKQ=",
			wantErr:         `cannot authenticate SCRAM-SHA-256 server signature: "XmF9pqV8S7suAoZWja4dJRkFsKQ="`,
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			sc := NewClient(tt.hash, tt.user, tt.pass)
			sc.setNonce([]byte(tt.nonce))
			if len(tt.setIter) > 0 {
				sc.AcceptIterations(tt.setIter[0], tt.setIter[1])
			}
			sc.Step(nil)
			if sc.Err() != nil {
				t.Error(sc.Err())
			}
			if have := string(sc.Out()); have != tt.wantClientFirst {
				t.Fatalf("client-first wrong\nhave: %q\nwant: %q", have, tt.wantClientFirst)
			}
			sc.Step([]byte(tt.serverFirst))

			if have := string(sc.Out()); have != tt.wantClientFinal {
				t.Fatalf("client-final wrong\nhave: %q\nwant: %q", have, tt.wantClientFinal)
			}
			done := sc.Step([]byte(tt.serverFinal))
			if !errorContains(sc.Err(), tt.wantErr) {
				fmt.Printf("%q\n", sc.serverSignature())
				t.Fatalf("wrong error\nhave: %s\nwant: %s", sc.Err(), tt.wantErr)
			}
			if tt.wantErr == "" && tt.serverFinal != "" && !done {
				t.Fatal("last step didn't return true")
			} else if tt.wantErr != "" && tt.serverFinal == "" && done {
				t.Fatal("last step returned true")
			}
		})
	}
}

func TestGenerateNonce(t *testing.T) {
	nonces := make(map[string]struct{})
	for range 128 {
		sc := NewClient(sha256.New, "user", "pass")
		sc.Step(nil)
		if l := len(sc.clientNonce); l != 24 {
			t.Fatal(l)
		}

		dec := make([]byte, 16)
		_, err := base64.StdEncoding.Decode(dec, sc.clientNonce)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := nonces[string(dec)]; ok {
			t.Fatal("nonce seen more than once?!")
		}
		nonces[string(dec)] = struct{}{}
	}
}

func errorContains(have error, want string) bool {
	if have == nil {
		return want == ""
	}
	if want == "" {
		return false
	}
	if strings.HasPrefix(want, "re:") {
		m, err := regexp.MatchString(want[3:], have.Error())
		if err != nil {
			panic(fmt.Errorf("errorContains: %w", err))
		}
		return m
	}
	if strings.HasPrefix(want, "or:") {
		for _, w := range strings.Split(want[3:], "|") {
			if strings.Contains(have.Error(), w) {
				return true
			}
		}
		return false
	}
	return strings.Contains(have.Error(), want)
}
