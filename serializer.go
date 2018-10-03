package pq

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
)

func Dump(driverRows driver.Rows) ([]byte, error) {
	var err error
	var builder bytes.Buffer
	if rs, ok := driverRows.(*rows); ok {
		defer func() {
			err := rs.Close()
			if err != nil {
				panic(err)
			}
		}()
		values := make([]driver.Value, len(rs.Columns()))
		for err = nil; err == nil; err = rs.Next(values) {
			builder.Write([]byte("("))
			for i, v := range values {
				_, err = builder.Write(encode(&rs.cn.parameterStatus, v, rs.colTyps[i].OID))
				if err != nil {
					return nil, err
				}
				builder.Write([]byte(", "))
			}
			builder.Write([]byte("),\n"))
		}
		if err != io.EOF {
			return nil, err
		}
		return builder.Bytes(), nil
	}
	return nil, errors.New("not a postgres rows struct")
}
