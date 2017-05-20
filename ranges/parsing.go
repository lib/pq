package ranges

import (
	"fmt"
)

func readDigits(buf []byte, pos int) ([]byte, int, error) {
	var s []byte
	for pos < len(buf) && buf[pos] >= 48 && buf[pos] <= 57 {
		s = append(s, buf[pos])
		pos++
	}
	if len(s) == 0 {
		return s, pos, fmt.Errorf("unexpected end of input at position %d", pos)
	}
	return s, pos, nil
}

func readInteger(buf []byte, pos int) ([]byte, int, error) {
	var s []byte
	if pos < len(buf) && buf[pos] == '-' {
		s = append(s, '-')
		pos++
	}
	digs, pos, err := readDigits(buf, pos)
	if err != nil {
		return nil, pos, err
	}
	return append(s, digs...), pos, nil
}

func readFloat(buf []byte, pos int) ([]byte, int, error) {
	s, pos, err := readInteger(buf, pos)
	if err != nil {
		return nil, pos, err
	}

	if pos < len(buf) && buf[pos] == '.' {
		var digs []byte

		s = append(s, '.')
		pos++

		digs, pos, err = readDigits(buf, pos)
		if err != nil {
			return nil, pos, err
		}
		s = append(s, digs...)
	}

	return s, pos, nil
}

func readSeparator(buf []byte, pos int) (int, error) {
	if pos >= len(buf) {
		return pos, fmt.Errorf("unexpected end of input at position %d", pos)
	}
	if buf[pos] != ',' {
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

func readFloatRange(buf []byte) (minIncl bool, maxIncl bool, min []byte, max []byte, err error) {
	var pos int
	minIncl, pos, err = readRangeBound(buf, pos, '[', '(')
	if err != nil {
		return
	}
	min, pos, err = readFloat(buf, pos)
	if err != nil {
		return
	}
	pos, err = readSeparator(buf, pos)
	if err != nil {
		return
	}
	max, pos, err = readFloat(buf, pos)
	if err != nil {
		return
	}
	maxIncl, pos, err = readRangeBound(buf, pos, ']', ')')
	if err != nil {
		return
	}
	return
}
