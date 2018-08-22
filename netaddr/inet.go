package netaddr

import (
	"database/sql/driver"
	"errors"
	"net"
)

// A wrapper for transferring Inet values back and forth easily.
type Inet struct {
	Inet net.IP
}

// Scan implements the Scanner interface.
func (i *Inet) Scan(value interface{}) error {
	i.Inet = nil
	ipAsBytes, ok := value.([]byte)
	if !ok {
		return errors.New("Could not convert scanned value to bytes")
	}
	parsedIP := net.ParseIP(string(ipAsBytes))
	if parsedIP == nil {
		return nil
	}
	i.Inet = parsedIP
	return nil
}

// Value implements the driver Valuer interface. Note if i.Valid is false
// or i.IP is nil the database column value will be set to NULL.
func (i Inet) Value() (driver.Value, error) {
	if i.Inet == nil {
		return nil, nil
	}
	return []byte(i.Inet.String()), nil
}
