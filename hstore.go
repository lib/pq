package pq

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/lib/pq/hstore"
	"github.com/lib/pq/oid"
)

// RegisterHstore tests that the 'hstore' extension is created in the database,
// and must be called prior to any queries that use the Hstore type.
func RegisterHstore(db *sql.DB) {
	// TODO: assert db is postgres driver
	var hstoreOid int
	if err := db.QueryRow("SELECT 'hstore'::regtype::oid").Scan(&hstoreOid); err != nil {
		panic(err)
	}
	oid.T_hstore = oid.Oid(hstoreOid)
}

// Hstore represents the PostgreSQL extension data type 'hstore', which stores
// sets of key/value pairs in the database. It corresponds closely to Go's
// map[string]string type.
type Hstore map[string]string

// ErrInvalidHstoreScan is returned if the type of data the hstore value is
// stored as in the database can't be scanned into an Hstore object.
var ErrInvalidHstoreScan = errors.New("invalid hstore scan type")

// Scan decodes the hstore value from the database and modifies the object with
// a new Hstore value.
//
// (*Hstore).Scan implements the sql.Scanner interface.
func (hs *Hstore) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		*hs = Hstore(hstore.Decode(src))
	case []byte:
		*hs = Hstore(hstore.Decode(string(src)))
	case nil:
		*hs = nil
	default:
		return ErrInvalidHstoreScan
	}
	return nil
}

var _ sql.Scanner = &Hstore{}

// Value encodes the Hstore object as a value that can be stored in the
// database as the hstore data type (i.e., a serialized string or null).
//
// (Hstore).Value implements the driver.Valuer interface.
func (hs Hstore) Value() (driver.Value, error) {
	if !hs.Valid() {
		return nil, nil
	}
	return hstore.Encode(hs), nil
}

var _ driver.Valuer = Hstore{}

// Valid returns true if the Hstore is non-null, false otherwise.
func (hs Hstore) Valid() bool {
	return hs != nil
}
