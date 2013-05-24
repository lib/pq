// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.
package pq

import (
	"database/sql/driver"
)

type notification struct {
	bePid   int
	relname string
	extra   string
}

func recvNotify(r *readBuf) notification {
	bePid := r.int32()
	relname := r.string()
	extra := r.string()

	return notification{bePid, relname, extra}
}

type notificationRows struct {
	ns *notificationStmnt
}

func (rs *notificationRows) Columns() []string {
	return []string{
		"bePid",
		"relname",
		"extra",
	}
}

func (rs *notificationRows) Close() error {
	return nil
}

func (rs *notificationRows) Next(dest []driver.Value) error {
	for {
		t, r := rs.ns.cn.recv1()
		switch t {
		case 'A':
			n := recvNotify(r)
			dest[0] = n.bePid
			dest[1] = n.relname
			dest[2] = n.extra
			return nil
		}
	}

	panic("not reached")
}

type notificationStmnt struct {
	cn *conn
	q  string
}

func (ns *notificationStmnt) Close() error {
	// We know the query string starts with "LISTEN ".
	_, err := ns.cn.Exec("UN"+ns.q, []driver.Value{})

	if err != nil {
		return err
	}

	return nil
}

func (ns *notificationStmnt) NumInput() int {
	// PostgreSQL doesn't seem to support constructs like
	// "LISTEN $1" anyway.
	return 0
}

func (ns *notificationStmnt) Exec(args []driver.Value) (driver.Result, error) {
	panic("unsupported")
}

func (ns *notificationStmnt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, ErrNotSupported
	}

	_, err := ns.cn.Exec(ns.q, args)

	if err != nil {
		return nil, err
	}

	return &notificationRows{ns}, nil
}
