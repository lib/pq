package pq

import (
	"database/sql/driver"
)

type listenrows struct {
	st *stmt
}

func (lr *listenrows) Close() error {
	_, err := lr.st.cn.Exec("UNLISTEN *", nil)
	return err
}

func (lr *listenrows) Columns() []string {
	return []string{"NOTIFICATION"}
}

func (lr *listenrows) Next(dest []driver.Value) (err error) {
	defer errRecover(&err)

	for {
		t, r := lr.st.cn.recv1()
		switch t {
		case 'E':
			panic(parseError(r))
		case 'Z':
			continue
		case 'A':
			// discard backend pid and channel name
			r.int32()
			r.string()

			// get message
			dest[0] = r.string()
			return nil
		default:
			errorf("unknown notify response: %q", t)
		}
	}
	panic("not reached")
}
