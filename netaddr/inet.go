package netaddr

import (
	"database/sql/driver"
	"errors"
	"net"
)

// A wrapper for transferring Inet values back and forth easily.
type Inet struct {
	Inet  net.IP
	Valid bool
}

// Scan implements the Scanner interface.
func (i *Inet) Scan(value interface{}) error {
	i.Inet = nil
	i.Valid = false
	if value == nil {
		i.Valid = false
		return nil
	}
	ipAsBytes, ok := value.([]byte)
	if !ok {
		return errors.New("Could not convert scanned value to bytes")
	}
	parsedIP := net.ParseIP(string(ipAsBytes))
	if parsedIP == nil {
		i.Valid = false
		return nil
	}
	i.Valid = true
	i.Inet = parsedIP
	return nil
}

// Value implements the driver Valuer interface. Note if i.Valid is false
// or i.IP is nil the database column value will be set to NULL.
func (i Inet) Value() (driver.Value, error) {
	if i.Valid == false || i.Inet == nil {
		return nil, nil
	}
	return []byte(i.Inet.String()), nil
}
