// PostgreSQL Array value parser state machine.
// Heavily lifted from the Go JSON decoder, with modifications for
// PGs inconsistent array format.

package arrays

// nextValue splits data after the next whole value,
// returning that value and the bytes that follow it as separate slices.
// scan is passed in for use by nextValue to avoid an allocation.
func nextValue(data []byte, scan *scanner) (value, rest []byte, err error) {
	scan.reset()
	for i, c := range data {
		v := scan.step(scan, int(c))
		if v >= scanEnd {
			switch v {
			case scanError:
				return nil, nil, scan.err
			case scanEnd:
				return data[0:i], data[i:], nil
			}
		}
	}
	if scan.eof() == scanError {
		return nil, nil, scan.err
	}
	return data, nil, nil
}

// A SyntaxError is a description of a syntax error.
type SyntaxError struct {
	msg    string // description of error
	Offset int64  // error occurred after reading Offset bytes
}

func (e *SyntaxError) Error() string { return e.msg }

// A scanner is a PostgreSQL Array scanning state machine.
// Callers call scan.reset() and then pass bytes in one at a time
// by calling scan.step(&scan, c) for each byte.
// The return value, referred to as an opcode, tells the
// caller about significant parsing events like beginning
// and ending literals and arrays, so that the caller can follow
// along if it wishes.
// The return value scanEnd indicates that a single top-level
// array has been completed, *before* the byte that just got passed
// in.  (The indication must be delayed in order to recognize the
// end of numbers: is 123 a whole value or the beginning of 12345e+6?).
type scanner struct {
	// The step is a func to be called to execute the next transition.
	// Also tried using an integer constant and a single func
	// with a switch, but using the func directly was 10% faster
	// on a 64-bit Mac Mini, and it's nicer to read.
	step func(*scanner, int) opcode

	// Reached end of top-level value.
	endTop bool

	// Stack of what we're in the middle of - array values, object keys, object values.
	parseState []parsecode

	// Error that happened, if any.
	err error

	// 1-byte redo (see undo method)
	redo      bool
	redoCode  opcode
	redoState func(*scanner, int) opcode

	// total bytes consumed, updated by decoder.Decode
	bytes int64
}

// An opcode is returned by the state transition functions. They
// give details about the current state of the scan that callers
// may be interested to know about.
type opcode int

const (
	// Continue.
	scanContinue     opcode = iota // uninteresting byte
	scanBeginLiteral               // end implied by next result != scanContinue
	scanBeginArray                 // begin array
	scanArrayValue                 // just finished array value
	scanEndArray                   // end array (implies scanArrayValue if possible)
	scanSkipSpace                  // space byte; can skip; known to be last "continue" result

	// Stop.
	scanEnd   // top-level value ended *before* this byte; known to be first "stop" result
	scanError // hit an error, scanner.err.
)

type parsecode int

// These values are stored in the parseState stack.
// They give the current state of a composite value
// being scanned.  If the parser is inside a nested value
// the parseState describes the nested state, outermost at entry 0.
const (
	parseArrayValue parsecode = iota // parsing array value
)

// reset prepares the scanner for use.
// It must be called before calling s.step.
func (s *scanner) reset() {
	s.step = stateBeginValue
	s.parseState = s.parseState[0:0]
	s.err = nil
	s.redo = false
	s.endTop = false
}

// eof tells the scanner that the end of input has been reached.
// It returns a scan status just as s.step does.
func (s *scanner) eof() opcode {
	if s.err != nil {
		return scanError
	}
	if s.endTop {
		return scanEnd
	}
	s.step(s, ' ')
	if s.endTop {
		return scanEnd
	}
	if s.err == nil {
		s.err = &SyntaxError{"unexpected end of input", s.bytes}
	}
	return scanError
}

// pushParseState pushes a new parse state p onto the parse stack.
func (s *scanner) pushParseState(p parsecode) {
	s.parseState = append(s.parseState, p)
}

// popParseState pops a parse state (already obtained) off the stack
// and updates s.step accordingly.
func (s *scanner) popParseState() {
	n := len(s.parseState) - 1
	s.parseState = s.parseState[0:n]
	s.redo = false
	if n == 0 {
		s.step = stateEndTop
		s.endTop = true
	} else {
		s.step = stateEndValue
	}
}

// stateBeginValueOrEmpty is the state after reading `{`.
func stateBeginValueOrEmpty(s *scanner, c int) opcode {
	if c == ' ' {
		return scanSkipSpace
	}
	if c == '}' {
		return stateEndValue(s, c)
	}
	return stateBeginValue(s, c)
}

// stateBeginValue is the state at the beginning of the input.
func stateBeginValue(s *scanner, c int) opcode {
	if c == ' ' {
		return scanSkipSpace
	}
	switch c {
	default: // unquoted value (could be a string if we're an array of strings)
		s.step = stateInUnquotedString
		return scanBeginLiteral
	case '"': // beginning of quoted string
		s.step = stateInString
		return scanBeginLiteral
	case '{': // beginning of new array
		s.step = stateBeginValueOrEmpty
		s.pushParseState(parseArrayValue)
		return scanBeginArray
	}
	return s.error(c, "looking for beginning of value")
}

// stateEndValue is the state after completing a value,
// such as after reading `{}` or `true` or `["x"`.
func stateEndValue(s *scanner, c int) opcode {
	n := len(s.parseState)
	if n == 0 {
		// Completed top-level before the current byte.
		s.step = stateEndTop
		s.endTop = true
		return stateEndTop(s, c)
	}
	if c == ' ' {
		s.step = stateEndValue
		return scanSkipSpace
	}
	ps := s.parseState[n-1]
	switch ps {
	case parseArrayValue:
		if c == ',' {
			s.step = stateBeginValue
			return scanArrayValue
		}
		if c == '}' {
			s.popParseState()
			return scanEndArray
		}
		return s.error(c, "after array element")
	}
	return s.error(c, "")
}

// stateEndTop is the state after finishing the top-level value,
// such as after reading `{}`.
func stateEndTop(s *scanner, c int) opcode {
	return scanEnd
}

// stateInString is the state after reading `"`.
func stateInString(s *scanner, c int) opcode {
	if c == '"' {
		s.step = stateEndValue
		return scanContinue
	}
	if c == '\\' {
		s.step = stateInStringEsc
		return scanContinue
	}
	if c < 0x20 {
		return s.error(c, "in string literal")
	}
	return scanContinue
}

// stateInUnquotedString is the state for an unquoted string
func stateInUnquotedString(s *scanner, c int) opcode {
	if c == ',' || c == '}' {
		return stateEndValue(s, c)
	}
	if c < 0x20 {
		return s.error(c, "in string literal")
	}
	return scanContinue
}

// stateInStringEsc is the state after reading `"\` during a quoted string.
func stateInStringEsc(s *scanner, c int) opcode {
	s.step = stateInString
	return scanContinue
}

// stateError is the state after reaching a syntax error,
// such as after reading `[1}` or `5.1.2`.
func stateError(s *scanner, c int) opcode {
	return scanError
}

// error records an error and switches to the error state.
func (s *scanner) error(c int, context string) opcode {
	s.step = stateError
	s.err = &SyntaxError{"invalid character " + string(c) + " " + context, s.bytes}
	return scanError
}

// undo causes the scanner to return scanCode from the next state transition.
// This gives callers a simple 1-byte undo mechanism.
func (s *scanner) undo(scanCode opcode) {
	if s.redo {
		panic("invalid use of scanner")
	}
	s.redoCode = scanCode
	s.redoState = s.step
	s.step = stateRedo
	s.redo = true
}

// stateRedo helps implement the scanner's 1-byte undo.
func stateRedo(s *scanner, c int) opcode {
	s.redo = false
	s.step = s.redoState
	return s.redoCode
}
