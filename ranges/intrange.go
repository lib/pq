package ranges

import (
	"errors"
	"strconv"
)

func parseIntRange(buf []byte, bitSize int) (int64, int64, error) {
	lowerb, upperb, err := readDiscreteRange(buf)
	if err != nil {
		return 0, 0, err
	}
	lower, err := strconv.ParseInt(string(lowerb), 10, bitSize)
	if err != nil {
		return 0, 0, err
	}
	upper, err := strconv.ParseInt(string(upperb), 10, bitSize)
	if err != nil {
		return 0, 0, err
	}
	if lower > upper {
		return 0, 0, errors.New("lower value is greater than the upper value")
	}
	return lower, upper, nil
}
