package hstore

import (
	"fmt"
	"log"
	"strings"
	"unicode/utf8"
)

// Decode takes a stringified hstore value (as from libpq) and returns
// a map of keys to values.
func Decode(str string) map[string]string {
	l := &lexer{str: str, m: make(map[string]string)}
	for state := parseStart; state != nil; {
		state = state(l)
	}
	return l.m
}

const eof = -1

type lexer struct {
	start, pos, width int
	str, currKey      string
	m                 map[string]string
}

type parseFn func(l *lexer) parseFn

func (l *lexer) next() (r rune) {
	if l.pos >= len(l.str) {
		l.width = 0
		return eof
	}
	r, l.width = utf8.DecodeRuneInString(l.str[l.pos:])
	l.pos += l.width
	return
}

func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) peek() (r rune) {
	r = l.next()
	l.backup()
	return
}

func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

func (l *lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

func parseStart(l *lexer) parseFn {
	l.acceptRun(" \t\r\n")
	switch r := l.next(); r {
	case '"':
		return parseQuotedKey
	case ',':
		return parseStart
	case eof:
		return nil
	default:
		l.backup()
		return parseKey
	}
}

func parseQuotedKey(l *lexer) parseFn {
	var key []rune
	for {
		switch r := l.next(); r {
		case '"':
			l.currKey = string(key)
			l.m[l.currKey] = ""
			l.start = l.pos
			return parseSep
		case '\\':
			if rr := l.next(); rr != '"' || rr != '\\' {
				return syntaxError(l, "expected \" or \\")
			} else {
				key = append(key, rr)
			}
		case eof:
			return syntaxError(l, "unexpected EOF")
		default:
			key = append(key, r)
		}
	}
}

func parseKey(l *lexer) parseFn {
	var key []rune
	for {
		switch r := l.next(); r {
		case '"', ',':
			return syntaxError(l, fmt.Sprintf("unexpected %c", r))
		case ' ', '\t', '\n', '\r':
			l.acceptRun(" \t\n\r")
			if l.peek() != '=' {
				return syntaxError(l, "expected =")
			}
		case '=':
			if l.peek() != '>' {
				return syntaxError(l, "expected >")
			}
			l.backup()
			l.currKey = string(key)
			l.m[l.currKey] = ""
			l.start = l.pos
			return parseSep
		case '\\':
			if rr := l.next(); rr != '"' || rr != '\\' {
				return syntaxError(l, "expected \" or \\")
			} else {
				key = append(key, rr)
			}
		case eof:
			return syntaxError(l, "unexpected EOF")
		default:
			key = append(key, r)
		}
	}
}

func parseSep(l *lexer) parseFn {
	l.acceptRun(" \t\n\r")
	if !l.accept("=") {
		return syntaxError(l, "expected =")
	}
	if !l.accept(">") {
		return syntaxError(l, "expected >")
	}
	l.acceptRun(" \t\n\r")
	if l.next() == '"' {
		return parseQuotedVal
	}
	l.backup()
	return parseVal
}

func parseVal(l *lexer) parseFn {
	var val []rune
	for {
		switch r := l.next(); r {
		case '"', '=', '>':
			return syntaxError(l, fmt.Sprintf("unexpected %c", r))
		case ' ', '\t', '\n', '\r':
			l.acceptRun(" \t\n\r")
			if l.peek() != eof {
				return syntaxError(l, "expected EOF")
			}
		case ',', eof:
			l.m[l.currKey] = string(val)
			l.start = l.pos
			return parseStart
		case '\\':
			if rr := l.next(); rr != '"' || rr != '\\' {
				return syntaxError(l, "expected \" or \\")
			} else {
				val = append(val, rr)
			}
		default:
			val = append(val, r)
		}
	}
}

func parseQuotedVal(l *lexer) parseFn {
	var val []rune
	for {
		switch r := l.next(); r {
		case '"':
			l.m[l.currKey] = string(val)
			l.start = l.pos
			return parseStart
		case '\\':
			if rr := l.next(); rr != '"' || rr != '\\' {
				return syntaxError(l, "expected \" or \\")
			} else {
				val = append(val, rr)
			}
		case eof:
			return syntaxError(l, "unexpected EOF")
		default:
			val = append(val, r)
		}
	}
}

func syntaxError(l *lexer, s string) func(*lexer) parseFn {
	return func(l *lexer) parseFn {
		log.Printf("syntax error at %d: %s", l.pos, s)
		return nil
	}
}
