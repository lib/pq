//go:build go1.9
// +build go1.9

package pq

import (
	"database/sql/driver"
	"reflect"
)

var _ driver.NamedValueChecker = (*conn)(nil)

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(driver.Valuer); ok {
		// Ignore Valuer, for backward compatibility with pq.Array().
		return driver.ErrSkip
	}

	// Ignoring []byte / []uint8.
	if _, ok := nv.Value.([]uint8); ok {
		return driver.ErrSkip
	}

	v := reflect.ValueOf(nv.Value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice {
		var err error
		nv.Value, err = Array(v.Interface()).Value()
		return err
	}

	return driver.ErrSkip
}
