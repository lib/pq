//
// FEBE Message type constants
//
// All the constants in this file have a special naming convention:
// "(msg)(NameInManual)(characterCode)".  This results in long and
// awkward constant names, but also makes it easy to determine what
// the author's intent is quickly in code (consider that both
// msgDescribeD and msgDataRowD appear on the wire as 'D') as well as
// debugging against captured wire protocol traffic (where one will
// only see 'D', but has a sense what state the protocol is in).
//
package pq

type pqMsgType byte

const (
	// Special sub-message coding for Close and Describe
	msgIsPortalP    = 'P'
	msgIsStatementS = 'S'

	// Sub-message character coding that is part of ReadyForQuery
	msgInIdleI        = 'I'
	msgInTransactionT = 'T'
	msgInErrorE       = 'E'

	// Message tags
	msgAuthenticationOkR                = 'R'
	msgAuthenticationCleartextPasswordR = 'R'
	msgAuthenticationMD5PasswordR       = 'R'
	msgAuthenticationSCMCredentialR     = 'R'
	msgAuthenticationGSSR               = 'R'
	msgAuthenticationSSPIR              = 'R'
	msgAuthenticationGSSContinueR       = 'R'
	msgBackendKeyDataK                  = 'K'
	msgBindB                            = 'B'
	msgBindComplete2                    = '2'
	// CancelRequest, not seen here, is formatted differently
	msgCloseC                = 'C'
	msgCloseComplete3        = '3'
	msgCommandCompleteC      = 'C'
	msgCopyDatad             = 'd'
	msgCopyDonec             = 'c'
	msgCopyFailf             = 'f'
	msgCopyInResponseG       = 'G'
	msgCopyOutResponseH      = 'H'
	msgCopyBothResponseW     = 'W'
	msgDataRowD              = 'D'
	msgDescribeD             = 'D'
	msgEmptyQueryResponseI   = 'I'
	msgErrorResponseE        = 'E'
	msgExecuteE              = 'E'
	msgFlushH                = 'H'
	msgFunctionCallF         = 'F'
	msgFunctionCallResponseV = 'V'
	msgNoDatan               = 'n'
	msgNoticeResponseN       = 'N'
	msgNotificationResponseA = 'A'
	msgParameterDescriptiont = 't'
	msgParameterStatusS      = 'S'
	msgParseP                = 'P'
	msgParseComplete1        = '1'
	msgPasswordMessagep      = 'p'
	msgPortalSuspendeds      = 's'
	msgQueryQ                = 'Q'
	msgReadyForQueryZ        = 'Z'
	msgRowDescriptionT       = 'T'
	// SSLRequest and StartupMessage are not seen here, and are
	// formatted differently
	msgSyncS      = 'S'
	msgTerminateX = 'X'
)
