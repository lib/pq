// Package driver registers the lib/pq driver for use with database/sql.
package driver

import (
	"database/sql"
	"database/sql/driver"

	"github.com/lib/pq"
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return pq.Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}
