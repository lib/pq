package netaddr

import (
	"database/sql/driver"
	"errors"
	"net"
)

// Cidr is a wrapper for transferring CIDR values back and forth easily.
type Cidr struct {
	Cidr  net.IPNet
	Valid bool
}

// Scan implements the Scanner interface.
func (c *Cidr) Scan(value interface{}) error {
	c.Cidr.IP = nil
	c.Cidr.Mask = nil
	c.Valid = false
	if value == nil {
		c.Valid = false
		return nil
	}
	cidrAsBytes, ok := value.([]byte)
	if !ok {
		return errors.New("Could not convert scanned value to bytes")
	}
	_, parsedIPNet, error := net.ParseCIDR(string(cidrAsBytes))
	if error != nil {
		return error
	}
	c.Valid = true
	c.Cidr.IP = parsedIPNet.IP
	c.Cidr.Mask = parsedIPNet.Mask
	return nil
}

// Value implements the driver Valuer interface. Note if c.Valid is false
// or c.Cidr.IP is nil the database column value will be set to NULL.
func (c Cidr) Value() (driver.Value, error) {
	if !c.Valid || c.Cidr.IP == nil {
		return nil, nil
	}
	return []byte(c.Cidr.String()), nil
}
