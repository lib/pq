package netaddr

import (
	"database/sql/driver"
	"errors"
	"net"
)

// A wrapper for transferring Macaddr values back and forth easily.
type Macaddr struct {
	Macaddr net.HardwareAddr
	Valid   bool
}

// Scan implements the Scanner interface.
func (m *Macaddr) Scan(value interface{}) error {
	m.Macaddr = nil
	m.Valid = false
	if value == nil {
		m.Valid = false
		return nil
	}
	macaddrAsBytes, ok := value.([]byte)
	if !ok {
		return errors.New("Could not convert scanned value to bytes")
	}
	parsedMacaddr, error := net.ParseMAC(string(macaddrAsBytes))
	if error != nil {
		return error
	}
	m.Valid = true
	m.Macaddr = parsedMacaddr
	return nil
}

// Value implements the driver Valuer interface. Note if m.Valid is false
// or m.Macaddr is nil the database column value will be set to NULL.
func (m Macaddr) Value() (driver.Value, error) {
	if m.Valid == false || m.Macaddr == nil {
		return nil, nil
	}
	return []byte(m.Macaddr.String()), nil
}
