package pq

type Copier struct {
	c       *conn
	sending bool
}

func NewCopier(name string) *Copier {
	cy := Copier{}
	err := cy.Open(name)
	if err != nil {
		panic(err)
	}
	return &cy
}

func (cop *Copier) Open(name string) error {
	d := drv{}
	con, err := d.Open(name)
	if err == nil {
		cop.c = con.(*conn)
	}
	return err
}

func (cy *Copier) Start(q string) (err error) {
	defer errRecover(&err)

	b := newWriteBuf('Q')
	b.string(q)
	cy.c.send(b)
	for {
		t, r := cy.c.recv1()
		switch t {
		case 'E':
			err = parseError(r)
			return err
		case 'G':
			cy.sending = true
			return nil
		default:
			errorf("unknown response for start copy: %q", t)
		}
	}
}

func (cy *Copier) Send(buf []byte) (err error) {
	if !cy.sending {
		errorf("Trying to send copy data when not in send mode")
	}

	b := newWriteBuf('d')
	b.bytes(buf)
	cy.c.send(b)
	return nil
}

func (cy *Copier) End() (err error) {
	if !cy.sending {
		errorf("Trying to send copy data when not in send mode")
	}

	b := newWriteBuf('c')
	cy.c.send(b)
	for {
		t, r := cy.c.recv1()
		switch t {
		case 'C':
		case 'E':
			err = parseError(r)
			return err
		case 'Z':
			// done
			return
		default:
			errorf("Unknown response for end copy data: %q", t)
		}
	}
}
