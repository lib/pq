// +build go1.9

package pq

import (
	"database/sql/driver"
	"reflect"
)

func (c *conn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	if _, ok := nv.Value.(driver.Valuer); ok {
		// Ignore Valuer, for backward compatiblity with pq.Array()
		return driver.ErrSkip
	}

	// Ignoring []byte / []uint8
	if _, ok := nv.Value.([]uint8); ok {
		return driver.ErrSkip
	}

	if k := reflect.ValueOf(nv.Value).Kind(); k == reflect.Array || k == reflect.Slice {
		nv.Value, err = Array(nv.Value).Value()
		return err
	}

	return driver.ErrSkip
}
