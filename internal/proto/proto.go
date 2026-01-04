// From src/include/libpq/protocol.h, PostgreSQL 18.1

package proto

import "strconv"

// RequestCode is a request codes sent by the frontend.
type RequestCode byte

// These are the request codes sent by the frontend.
const (
	Bind                = RequestCode('B')
	Close               = RequestCode('C')
	Describe            = RequestCode('D')
	Execute             = RequestCode('E')
	FunctionCall        = RequestCode('F')
	Flush               = RequestCode('H')
	Parse               = RequestCode('P')
	Query               = RequestCode('Q')
	Sync                = RequestCode('S')
	Terminate           = RequestCode('X')
	CopyFail            = RequestCode('f')
	GSSResponse         = RequestCode('p')
	PasswordMessage     = RequestCode('p')
	SASLInitialResponse = RequestCode('p')
	SASLResponse        = RequestCode('p')
)

// ResponseCode is a response codes sent by the backend.
type ResponseCode byte

// These are the response codes sent by the backend.
const (
	ParseComplete            = ResponseCode('1')
	BindComplete             = ResponseCode('2')
	CloseComplete            = ResponseCode('3')
	NotificationResponse     = ResponseCode('A')
	CommandComplete          = ResponseCode('C')
	DataRow                  = ResponseCode('D')
	ErrorResponse            = ResponseCode('E')
	CopyInResponse           = ResponseCode('G')
	CopyOutResponse          = ResponseCode('H')
	EmptyQueryResponse       = ResponseCode('I')
	BackendKeyData           = ResponseCode('K')
	NoticeResponse           = ResponseCode('N')
	AuthenticationRequest    = ResponseCode('R')
	ParameterStatus          = ResponseCode('S')
	RowDescription           = ResponseCode('T')
	FunctionCallResponse     = ResponseCode('V')
	CopyBothResponse         = ResponseCode('W')
	ReadyForQuery            = ResponseCode('Z')
	NoData                   = ResponseCode('n')
	PortalSuspended          = ResponseCode('s')
	ParameterDescription     = ResponseCode('t')
	NegotiateProtocolVersion = ResponseCode('v')
)

// These are the codes sent by both the frontend and backend.
// #define PqMsg_CopyDone				'c'
// #define PqMsg_CopyData				'd'

// These are the codes sent by parallel workers to leader processes.
// #define PqMsg_Progress              'P'

// AuthCode are authentication request codes sent by the backend.
type AuthCode int32

// These are the authentication request codes sent by the backend.
const (
	AuthReqOk       = AuthCode(0)  // User is authenticated
	AuthReqKrb4     = AuthCode(1)  // Kerberos V4. Not supported any more.
	AuthReqKrb5     = AuthCode(2)  // Kerberos V5. Not supported any more.
	AuthReqPassword = AuthCode(3)  // Password
	AuthReqCrypt    = AuthCode(4)  // crypt password. Not supported any more.
	AuthReqMD5      = AuthCode(5)  // md5 password
	_               = AuthCode(6)  // 6 is available.  It was used for SCM creds, not supported any more.
	AuthReqGSS      = AuthCode(7)  // GSSAPI without wrap()
	AuthReqGSSCont  = AuthCode(8)  // Continue GSS exchanges
	AuthReqSSPI     = AuthCode(9)  // SSPI negotiate without wrap()
	AuthReqSASL     = AuthCode(10) // Begin SASL authentication
	AuthReqSASLCont = AuthCode(11) // Continue SASL authentication
	AuthReqSASLFin  = AuthCode(12) // Final SASL message
)

func (a AuthCode) String() string {
	s, ok := map[AuthCode]string{
		AuthReqOk:       "ok",
		AuthReqKrb4:     "krb4",
		AuthReqKrb5:     "krb5",
		AuthReqPassword: "password",
		AuthReqCrypt:    "crypt",
		AuthReqMD5:      "md5",
		AuthReqGSS:      "GDD",
		AuthReqGSSCont:  "GSSCont",
		AuthReqSSPI:     "SSPI",
		AuthReqSASL:     "SASL",
		AuthReqSASLCont: "SASLCont",
		AuthReqSASLFin:  "SASLFin",
	}[a]
	if !ok {
		s = "<unknown>"
	}
	return s + " (" + strconv.Itoa(int(a)) + ")"
}
