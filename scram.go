package pq

// This file provides support for authentication based on the SCRAM-SHA-256
// SASL mechanism available in PostgreSQL v10 and above. Most of the comments
// in this file refer to terms from RFC 5802. Also refer RFC 3454 for the
// stringprep algorithm and RFC 4013 for the SASLprep profile of this algorithm.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/xdg-go/stringprep"
	"golang.org/x/crypto/pbkdf2"
)

func (cn *conn) doScramAuth(password string) {
	s := scramCtx{cn: cn, password: password}
	s.step1() // C: n,,n=,r=nonce
	s.step2() // S: r=nonce,s=salt,i=iters
	s.step3() // C: c=biws,r=nonce,p=proof
	s.step4() // S: v=verifier
}

//------------------------------------------------------------------------------

type scramCtx struct {
	cn       *conn
	password string
	cnonce   string // client's nonce
	sfm      string // server-first-message
	fnonce   string // full nonce (client || server)
	salt     []byte
	iters    int    // iteration count
	sp       []byte // salted password
	am       []byte // auth message
}

// step1: client sends a nonce to the server
func (s *scramCtx) step1() {
	// make a client nonce
	s.cnonce = makeNonce()

	// make "client-first-message"
	msg := []byte("n,,n=,r=" + s.cnonce)

	// send it to postgres
	w := s.cn.writeBuf('p')
	w.string("SCRAM-SHA-256")
	w.int32(len(msg))
	w.bytes(msg)
	s.cn.send(w)
}

// step2: server sends nonce, salt and iteration count
func (s *scramCtx) step2() {
	// receive postgres reply
	t, r := s.cn.recv()
	if t != 'R' {
		errorf("unexpected password response: %q", t)
	}
	if r.int32() != 11 {
		errorf("unexpected authentication response: %q", t)
	}

	// parse the "server-first-message"
	s.sfm = string(*r)
	parts := strings.Split(s.sfm, ",")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "r=") ||
		!strings.HasPrefix(parts[1], "s=") || !strings.HasPrefix(parts[2], "i=") {
		errorf("invalid SCRAM server-first-message from server")
	}

	// r=<client nonce || server nonce>
	s.fnonce = parts[0][2:]
	if len(s.fnonce) == len(s.cnonce) || !strings.HasPrefix(s.fnonce, s.cnonce) {
		errorf("invalid SCRAM nonce from server")
	}

	// s=<salt>
	var err error
	s.salt, err = base64.StdEncoding.DecodeString(parts[1][2:])
	if err != nil {
		errorf("invalid SCRAM salt from server: %v", err)
	}

	// i=<iterations>
	s.iters, err = strconv.Atoi(parts[2][2:])
	if err != nil {
		errorf("invalid SCRAM iteration count from server: %v", err)
	}
	if s.iters <= 0 {
		errorf("invalid SCRAM iteration count (%d) from server", s.iters)
	}
}

// step3: client sends full nonce and client proof
func (s *scramCtx) step3() {
	// client-final-message-without-proof
	cfmwo := "c=biws,r=" + s.fnonce // "biws" = base64("n,,")

	// Normalize(password)
	np, err := stringprep.SASLprep.Prepare(s.password)
	if err != nil {
		// As per RFC 4013, we should fail here, with:
		//errorf("unsupported characters in password: %v", err)
		// However, Postgres will successfully authenticate even if the
		// characters in the password do not fit the 4013 profile. See
		// test case TestSCRAMStrangePasswords in scram_test.go.
		np = s.password
	}

	// SaltedPassword  := Hi(Normalize(password), salt, i)
	s.sp = pbkdf2.Key([]byte(np), s.salt, s.iters, 32, sha256.New)

	// AuthMessage     := client-first-message-bare + "," +
	//                    server-first-message + "," +
	//                    client-final-message-without-proof
	s.am = []byte("n=,r=" + s.cnonce + "," + s.sfm + "," + cfmwo)

	// make client proof
	cp := computeClientProof(s.sp, s.am)

	// client-final-message = client-final-message-without-proof "," proof
	cfm := []byte(fmt.Sprintf("%s,p=%s", cfmwo, cp))

	// send it to postgres
	w := s.cn.writeBuf('p')
	w.bytes(cfm)
	s.cn.send(w)
}

// step4: server sends signature
func (s *scramCtx) step4() {
	t, r := s.cn.recv()
	if t != 'R' {
		errorf("unexpected password response: %q", t)
	}
	if v := r.int32(); v != 12 {
		errorf("unexpected authentication response: %q", v)
	}

	// server-final-message
	sfm := string(*r)
	if !strings.HasPrefix(sfm, "v=") {
		errorf("invalid SCRAM server-final-message from server")
	}

	// compute the required server signature, and compare
	reqd := computeServerSignature(s.sp, s.am)
	if subtle.ConstantTimeCompare([]byte(reqd), []byte(sfm[2:])) != 1 {
		errorf("invalid SCRAM ServerSignature from server")
	}
}

//------------------------------------------------------------------------------

func makeNonce() string {
	data := make([]byte, 24)
	if _, err := rand.Read(data); err != nil {
		errorf("failed to read random data: %v", err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func computeClientProof(salted_password []byte, auth_message []byte) string {
	// ClientKey       := HMAC(SaltedPassword, "Client Key")
	// StoredKey       := H(ClientKey)
	// ClientSignature := HMAC(StoredKey, AuthMessage)
	// ClientProof     := ClientKey XOR ClientSignature

	ck := computeHMAC(salted_password, []byte("Client Key"))
	sk := sha256.Sum256(ck)
	cs := computeHMAC(sk[:], auth_message)
	proof := make([]byte, len(cs))
	for i := 0; i < len(cs); i++ {
		proof[i] = ck[i] ^ cs[i]
	}
	return base64.StdEncoding.EncodeToString(proof)
}

func computeServerSignature(salted_password []byte, auth_message []byte) string {
	// ServerKey       := HMAC(SaltedPassword, "Server Key")
	// ServerSignature := HMAC(ServerKey, AuthMessage)

	sk := computeHMAC(salted_password, []byte("Server Key"))
	ss := computeHMAC(sk, auth_message)
	return base64.StdEncoding.EncodeToString(ss)
}

func computeHMAC(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}
