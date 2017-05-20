package ranges

import (
	"fmt"
)

func readNumber(buf []byte, pos int) ([]byte, int, error) {
	var (
		s          []byte
		b          byte
		canEnd     = false
		inMantissa bool
	)
	for pos < len(buf) {
		b = buf[pos]
		if b == '-' && len(s) == 0 {
			s = append(s, b)
			canEnd = false
		} else if b >= 48 && b <= 57 {
			s = append(s, b)
			canEnd = true
		} else if b == '.' && !inMantissa {
			s = append(s, b)
			canEnd = false
		} else {
			break
		}
		pos++
	}
	if !canEnd {
		return s, pos, fmt.Errorf("unexpected character '%c' at position %d", b, pos)
	}
	return s, pos, nil
}

func readByte(buf []byte, pos int, expect byte) (int, error) {
	if pos >= len(buf) {
		return pos, fmt.Errorf("unexpected end of input at position %d", pos)
	}
	if buf[pos] != expect {
		return pos, fmt.Errorf("unexpected character '%c' at position %d", buf[pos], pos)
	}
	return pos + 1, nil
}

func readRangeBound(buf []byte, pos int, incl, excl byte) (bool, int, error) {
	if pos >= len(buf) {
		return false, 0, fmt.Errorf("unexpected end of input at position %d", pos)
	}
	switch buf[pos] {
	case incl:
		return true, pos + 1, nil
	case excl:
		return false, pos + 1, nil
	default:
		return false, pos, fmt.Errorf("unexpected character '%c' at position %d", buf[pos], pos)
	}
}

func readRange(buf []byte) (minIncl bool, maxIncl bool, min []byte, max []byte, err error) {
	var pos int
	minIncl, pos, err = readRangeBound(buf, pos, '[', '(')
	if err != nil {
		return
	}
	min, pos, err = readNumber(buf, pos)
	if err != nil {
		return
	}
	pos, err = readByte(buf, pos, ',')
	if err != nil {
		return
	}
	max, pos, err = readNumber(buf, pos)
	if err != nil {
		return
	}
	maxIncl, pos, err = readRangeBound(buf, pos, ']', ')')
	if err != nil {
		return
	}
	return
}

func readDiscreteRange(buf []byte) (min []byte, max []byte, err error) {
	var pos int
	pos, err = readByte(buf, pos, '[')
	if err != nil {
		return
	}
	min, pos, err = readNumber(buf, pos)
	if err != nil {
		return
	}
	pos, err = readByte(buf, pos, ',')
	if err != nil {
		return
	}
	max, pos, err = readNumber(buf, pos)
	if err != nil {
		return
	}
	pos, err = readByte(buf, pos, ')')
	if err != nil {
		return
	}
	return
}
