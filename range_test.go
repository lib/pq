//go:build go1.18
// +build go1.18

package pq

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRange(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r := Range[int64]{}

	// test for empty range
	err := db.QueryRow("SELECT 'empty'::int4range").Scan(&r)
	if err != nil {
		t.Fatal(err)
	}
	if !r.IsEmpty() {
		t.Fatalf("expected empty range")
	}

	err = db.QueryRow("SELECT $1::int4range", r).Scan(&r)
	if err != nil {
		t.Fatalf("re-query empty range failed: %s", err.Error())
	}
	if !r.IsEmpty() {
		t.Fatalf("expected empty range")
	}

	testBidirectionalRange(t, db, "int4range", NewRange(toPtr(1), toPtr(6)))
	testBidirectionalRange(t, db, "int8range", NewRange(toPtr(1), toPtr(6)))
	testBidirectionalRange(t, db, "numrange", NewRange(toPtr(1), toPtr(6)))
	testBidirectionalRange(t, db, "tsrange", NewRange(
		toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
		toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
	))
	testBidirectionalRange(t, db, "tstzrange", NewRange(
		toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
		toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
	))
	testBidirectionalRange(t, db, "daterange", NewRange(
		toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
		toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
	))

	// custom valuer and scanner
	Default := func(i int) RangeCustomValuer {
		return RangeCustomValuer{i}
	}
	testBidirectionalRange(t, db, "int4range", NewRange(toPtr(Default(1)), toPtr(Default(6))))

	// infinite
	testBidirectionalRange(t, db, "int4range", Range[int]{
		Lower:      nil,
		LowerBound: RangeLowerBoundExclusive,
		Upper:      toPtr(6),
		UpperBound: RangeUpperBoundExclusive,
	})
}

type RangeCustomValuer struct {
	i int
}

func (v *RangeCustomValuer) Scan(src any) error {
	b, _ := src.([]byte)
	v.i, _ = strconv.Atoi(string(b))
	return nil
}

func (v RangeCustomValuer) Value() (driver.Value, error) {
	return v.i, nil
}

func TestMultiRange(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r := MultiRange[int64]{}

	// test for empty multirange
	err := db.QueryRow("SELECT '{}'::int4multirange").Scan(&r)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow("SELECT $1::int4multirange", r).Scan(&r)
	if err != nil {
		t.Fatalf("re-query empty multirange failed: %s", err.Error())
	}

	testBidirectionalMultiRange(t, db, "int4multirange", MultiRange[int]{
		NewRange(toPtr(-1), toPtr(0)),
		NewRange(toPtr(1), toPtr(6)),
	})
	testBidirectionalMultiRange(t, db, "int8multirange", MultiRange[int]{
		NewRange(toPtr(-1), toPtr(0)),
		NewRange(toPtr(1), toPtr(6)),
	})
	testBidirectionalMultiRange(t, db, "nummultirange", MultiRange[int]{
		NewRange(toPtr(-1), toPtr(0)),
		NewRange(toPtr(1), toPtr(6)),
	})
	testBidirectionalMultiRange(t, db, "tsmultirange", MultiRange[time.Time]{
		NewRange(
			toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
		NewRange(
			toPtr(time.Date(1996, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1996, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
	})
	testBidirectionalMultiRange(t, db, "tstzmultirange", MultiRange[time.Time]{
		NewRange(
			toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
		NewRange(
			toPtr(time.Date(1996, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1996, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
	})
	testBidirectionalMultiRange(t, db, "datemultirange", MultiRange[time.Time]{
		NewRange(
			toPtr(time.Date(1995, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1995, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
		NewRange(
			toPtr(time.Date(1996, time.December, 1, 0, 0, 0, 0, time.FixedZone("", 0))),
			toPtr(time.Date(1996, time.December, 2, 0, 0, 0, 0, time.FixedZone("", 0))),
		),
	})
}

func testBidirectionalRange[T any](t *testing.T, db *sql.DB, rt string, h Range[T]) {
	var r Range[T]
	err := db.QueryRow("SELECT $1::"+rt, h).Scan(&r)
	if err != nil {
		t.Fatalf("re-query range failed: %s", err.Error())
	}
	compareRange(t, r, h)
}

func testBidirectionalMultiRange[T any](t *testing.T, db *sql.DB, rt string, hs MultiRange[T]) {
	var rs MultiRange[T]
	err := db.QueryRow("SELECT $1::"+rt, hs).Scan(&rs)
	if err != nil {
		t.Fatalf("re-query ranges failed: %s", err.Error())
	}

	for i := range rs {
		compareRange(t, rs[i], hs[i])
	}
}

func compareRange[T any](t *testing.T, r, h Range[T]) {
	if r.LowerBound != h.LowerBound || r.UpperBound != h.UpperBound {
		t.Fatalf("failed to compare bounds ranges: %+v / %+v", h, r)
	}
	if (h.Lower == nil) != (r.Lower == nil) ||
		(h.Lower != nil && r.Lower != nil && !reflect.DeepEqual(*h.Lower, *r.Lower)) {
		t.Fatalf("failed to compare lower ranges: %+v / %+v", h.Lower, r.Lower)
	}
	if (h.Upper == nil) != (r.Upper == nil) ||
		(h.Upper != nil && r.Upper != nil && !reflect.DeepEqual(*h.Upper, *r.Upper)) {
		t.Fatalf("failed to compare upper ranges: %+v / %+v", h.Upper, r.Upper)
	}
}

func toPtr[T any](t T) *T {
	return &t
}
