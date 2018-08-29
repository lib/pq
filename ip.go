package pq

import (
	"database/sql/driver"
	"fmt"
	"net"
)

type IP net.IP

func (a *IP) Scan(src interface{}) error {
	switch x := src.(type) {
	case []uint8:
		*a = IP(net.ParseIP(string(x)))
		return nil
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to net.IP", src)
}

func (a IP) Value() (driver.Value, error) {
	return net.IP(a).String(), nil
}
