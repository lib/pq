package pq

import "time"

// contextInterface and background are copied from the context package.

// contextInterface is named this way so that files tagged go1.8 can import
// context without conflict.
type contextInterface interface {
	Deadline() (deadline time.Time, ok bool)
	Done() <-chan struct{}
	Err() error
	Value(key interface{}) interface{}
}

type emptyCtx int

func (*emptyCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyCtx) Done() <-chan struct{} {
	return nil
}

func (*emptyCtx) Err() error {
	return nil
}

func (*emptyCtx) Value(key interface{}) interface{} {
	return nil
}

func (e *emptyCtx) String() string {
	switch e {
	case background:
		return "context.Background"
	}
	return "unknown empty Context"
}

var (
	background = new(emptyCtx)
)
