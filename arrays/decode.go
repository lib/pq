// Parses PG Arrays into Go arrays

package arrays

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

var byteSliceType = reflect.TypeOf([]byte(nil))

// Unmarshal parses the PG Array data and stores the result
// in the value pointed to by v.
//
// Heavily lifted from the Go JSON decoder. Main difference is
// PG Arrays can contain unquoted strings if they're string arrays
// and the strings don't have whitespace or special characters.
func Unmarshal(data []byte, v interface{}) error {
	var d decodeState
	d.init(data)
	return d.unmarshal(v)
}

// An UnmarshalTypeError describes a value that was
// not appropriate for a value of a specific Go type.
type UnmarshalTypeError struct {
	Value string       // description of value - "bool", "array", "number -5"
	Type  reflect.Type // type of Go value it could not be assigned to
}

func (e *UnmarshalTypeError) Error() string {
	return "cannot unmarshal " + e.Value + " into Go value of type " + e.Type.String()
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "Unmarshal(nil " + e.Type.String() + ")"
}

func (d *decodeState) unmarshal(v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	rv := reflect.ValueOf(v)
	if (rv.Kind() != reflect.Ptr) || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}

	d.scan.reset()
	// We decode rv not rv.Elem because the Unmarshaler interface
	// test must be applied at the top level of the value.
	d.value(rv)
	return d.savedError
}

// decodeState represents the state while decoding an array value.
type decodeState struct {
	data       []byte
	off        int // read offset in data
	scan       scanner
	nextscan   scanner // for calls to nextValue
	savedError error
	tempstr    string // scratch space to avoid some allocations
}

// errPhase is used for errors that should not happen unless
// there is a bug in the decoder or something is editing
// the data slice while the decoder executes.
var errPhase = errors.New("decoder out of sync - data changing underfoot?")

func (d *decodeState) init(data []byte) *decodeState {
	d.data = data
	d.off = 0
	d.savedError = nil
	return d
}

// error aborts the decoding by panicking with err.
func (d *decodeState) error(err error) {
	panic(err)
}

// saveError saves the first err it is called with,
// for reporting at the end of the unmarshal.
func (d *decodeState) saveError(err error) {
	if d.savedError == nil {
		d.savedError = err
	}
}

// next cuts off and returns the next full value in d.data[d.off:].
// The next value is known to be an object or array, not a literal.
func (d *decodeState) next() []byte {
	// c := d.data[d.off]
	item, rest, err := nextValue(d.data[d.off:], &d.nextscan)
	if err != nil {
		d.error(err)
	}
	d.off = len(d.data) - len(rest)

	return item
}

// scanWhile processes bytes in d.data[d.off:] until it
// receives a scan code not equal to op.
// It updates d.off and returns the new scan code.
func (d *decodeState) scanWhile(op opcode) opcode {
	var newOp opcode
	for {
		if d.off >= len(d.data) {
			newOp = d.scan.eof()
			d.off = len(d.data) + 1 // mark processed EOF with len+1
		} else {
			c := int(d.data[d.off])
			d.off++
			newOp = d.scan.step(&d.scan, c)
		}
		if newOp != op {
			break
		}
	}
	return newOp
}

// value decodes a value from d.data[d.off:] into the value.
// it updates d.off to point past the decoded value.
func (d *decodeState) value(v reflect.Value) {
	switch op := d.scanWhile(scanSkipSpace); op {
	default:
		d.error(errPhase)

	case scanBeginArray:
		d.array(v)

	case scanBeginLiteral:
		d.literal(v)
	}
}

// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
// if decodingNull is true, indirect stops at the last pointer so it can be set to nil.
func (d *decodeState) indirect(v reflect.Value, decodingNull bool) reflect.Value {
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.Elem().Kind() != reflect.Ptr && decodingNull && v.CanSet() {
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}

// array consumes an array from d.data[d.off-1:], decoding into the value v.
// the first byte of the array ('{') has been read already.
func (d *decodeState) array(v reflect.Value) {
	v = d.indirect(v, false)

	// Check type of target.
	switch v.Kind() {
	case reflect.Interface:
		if v.NumMethod() == 0 {
			// Decoding into nil interface?  Switch to non-reflect code.
			v.Set(reflect.ValueOf(d.arrayInterface()))
			return
		}
		// Otherwise it's invalid.
		fallthrough
	default:
		d.saveError(&UnmarshalTypeError{"array", v.Type()})
		d.off--
		d.next()
		return
	case reflect.Array:
	case reflect.Slice:
		break
	}

	i := 0
	for {
		// Look ahead for } - can only happen on first iteration.
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}

		// Back up so d.value can have the byte we just read.
		d.off--
		d.scan.undo(op)

		// Get element of array, growing if necessary.
		if v.Kind() == reflect.Slice {
			// Grow slice if necessary
			if i >= v.Cap() {
				newcap := v.Cap() + v.Cap()/2
				if newcap < 4 {
					newcap = 4
				}
				newv := reflect.MakeSlice(v.Type(), v.Len(), newcap)
				reflect.Copy(newv, v)
				v.Set(newv)
			}
			if i >= v.Len() {
				v.SetLen(i + 1)
			}
		}

		if i < v.Len() {
			// Decode into element.
			d.value(v.Index(i))
		} else {
			// Ran out of fixed array: skip.
			d.value(reflect.Value{})
		}
		i++

		// Next token must be , or }.
		op = d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}
		if op != scanArrayValue {
			d.error(errPhase)
		}
	}

	if i < v.Len() {
		if v.Kind() == reflect.Array {
			// Array.  Zero the rest.
			z := reflect.Zero(v.Type().Elem())
			for ; i < v.Len(); i++ {
				v.Index(i).Set(z)
			}
		} else {
			v.SetLen(i)
		}
	}
	if i == 0 && v.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
}

// literal consumes a literal from d.data[d.off-1:], decoding into the value v.
// The first byte of the literal has been read already
// (that's how the caller knows it's a literal).
func (d *decodeState) literal(v reflect.Value) {
	// All bytes inside literal return scanContinue op code.
	start := d.off - 1
	op := d.scanWhile(scanContinue)

	// Scan read one byte too far; back up.
	d.off--
	d.scan.undo(op)

	d.literalStore(d.data[start:d.off], v, false)
}

// convertNumber converts the number literal s to a float64
func (d *decodeState) convertNumber(s string) (interface{}, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, &UnmarshalTypeError{"number " + s, reflect.TypeOf(0.0)}
	}
	return f, nil
}

// literalStore decodes a literal stored in item into v.
//
// fromQuoted indicates whether this literal came from unwrapping a
// string from the ",string" struct tag option. this is used only to
// produce more helpful error messages.
func (d *decodeState) literalStore(item []byte, v reflect.Value, fromQuoted bool) {
	// Check for unmarshaler.
	if len(item) == 0 {
		//Empty string given
		d.saveError(fmt.Errorf("invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
		return
	}
	wantptr := item[0] == 'N' // null
	v = d.indirect(v, wantptr)

	switch c := item[0]; c {
	case 'N': // null
		switch v.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
			// otherwise, ignore null for primitives/string
		}
	case 't', 'f': // true, false
		value := c == 't'
		switch v.Kind() {
		default:
			if fromQuoted {
				d.saveError(fmt.Errorf("invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.saveError(&UnmarshalTypeError{"bool", v.Type()})
			}
		case reflect.Bool:
			v.SetBool(value)
		case reflect.String:
			v.SetString(string(c))
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(value))
			} else {
				d.saveError(&UnmarshalTypeError{"bool", v.Type()})
			}
		}

	case '"': // string
		s, ok := unquoteBytes(item)
		if !ok {
			if fromQuoted {
				d.error(fmt.Errorf("invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.error(errPhase)
			}
		}
		switch v.Kind() {
		default:
			d.saveError(&UnmarshalTypeError{"string", v.Type()})
		case reflect.Slice:
			if v.Type() != byteSliceType {
				d.saveError(&UnmarshalTypeError{"string", v.Type()})
				break
			}
			b := make([]byte, base64.StdEncoding.DecodedLen(len(s)))
			n, err := base64.StdEncoding.Decode(b, s)
			if err != nil {
				d.saveError(err)
				break
			}
			v.Set(reflect.ValueOf(b[0:n]))
		case reflect.String:
			v.SetString(string(s))
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(string(s)))
			} else {
				d.saveError(&UnmarshalTypeError{"string", v.Type()})
			}
		}

	default: // number or unquoted string
		s := string(item)
		switch v.Kind() {
		default:
			d.error(&UnmarshalTypeError{s, v.Type()})

		case reflect.String:
			v.SetString(string(s))

		case reflect.Interface:
			n, err := d.convertNumber(s)
			if err != nil {
				d.saveError(err)
				break
			}
			if v.NumMethod() != 0 {
				d.saveError(&UnmarshalTypeError{"number", v.Type()})
				break
			}
			v.Set(reflect.ValueOf(n))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil || v.OverflowInt(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type()})
				break
			}
			v.SetInt(n)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil || v.OverflowUint(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type()})
				break
			}
			v.SetUint(n)

		case reflect.Float32, reflect.Float64:
			n, err := strconv.ParseFloat(s, v.Type().Bits())
			if err != nil || v.OverflowFloat(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type()})
				break
			}
			v.SetFloat(n)
		}
	}
}

// The xxxInterface routines build up a value to be stored
// in an empty interface.  They are not strictly necessary,
// but they avoid the weight of reflection in this common case.

// valueInterface is like value but returns interface{}
func (d *decodeState) valueInterface() interface{} {
	switch d.scanWhile(scanSkipSpace) {
	default:
		d.error(errPhase)
		panic("unreachable")
	case scanBeginArray:
		return d.arrayInterface()
	case scanBeginLiteral:
		return d.literalInterface()
	}
}

// arrayInterface is like array but returns []interface{}.
func (d *decodeState) arrayInterface() []interface{} {
	var v = make([]interface{}, 0)
	for {
		// Look ahead for ] - can only happen on first iteration.
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}

		// Back up so d.value can have the byte we just read.
		d.off--
		d.scan.undo(op)

		v = append(v, d.valueInterface())

		// Next token must be , or ].
		op = d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}
		if op != scanArrayValue {
			d.error(errPhase)
		}
	}
	return v
}

// literalInterface is like literal but returns an interface value.
func (d *decodeState) literalInterface() interface{} {
	// All bytes inside literal return scanContinue op code.
	start := d.off - 1
	op := d.scanWhile(scanContinue)

	// Scan read one byte too far; back up.
	d.off--
	d.scan.undo(op)
	item := d.data[start:d.off]

	switch c := item[0]; c {
	case 'N': // null
		return nil

	case 't', 'f': // true, false
		return c == 't'

	case '"': // string
		s, ok := unquote(item)
		if !ok {
			d.error(errPhase)
		}
		return s

	default: // number
		if c != '-' && (c < '0' || c > '9') {
			d.error(errPhase)
		}
		n, err := d.convertNumber(string(item))
		if err != nil {
			d.saveError(err)
		}
		return n
	}
}

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
func getu4(s []byte) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	r, err := strconv.ParseUint(string(s[2:6]), 16, 64)
	if err != nil {
		return -1
	}
	return rune(r)
}

// unquote converts a quoted string literal s into an actual string t.
// The rules are different than for Go, so cannot use strconv.Unquote.
func unquote(s []byte) (t string, ok bool) {
	s, ok = unquoteBytes(s)
	t = string(s)
	return
}

func unquoteBytes(s []byte) (t []byte, ok bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return
	}
	s = s[1 : len(s)-1]

	// Check for unusual characters. If there are none,
	// then no unquoting is needed, so return a slice of the
	// original bytes.
	r := 0
	for r < len(s) {
		c := s[r]
		if c == '\\' || c == '"' || c < ' ' {
			break
		}
		if c < utf8.RuneSelf {
			r++
			continue
		}
		rr, size := utf8.DecodeRune(s[r:])
		if rr == utf8.RuneError && size == 1 {
			break
		}
		r += size
	}
	if r == len(s) {
		return s, true
	}

	b := make([]byte, len(s)+2*utf8.UTFMax)
	w := copy(b, s[0:r])
	for r < len(s) {
		// Out of room?  Can only happen if s is full of
		// malformed UTF-8 and we're replacing each
		// byte with RuneError.
		if w >= len(b)-2*utf8.UTFMax {
			nb := make([]byte, (len(b)+utf8.UTFMax)*2)
			copy(nb, b[0:w])
			b = nb
		}
		switch c := s[r]; {
		case c == '\\':
			r++
			if r >= len(s) {
				return
			}
			switch s[r] {
			default:
				b[w] = s[r]
				r++
				w++
			case 'u':
				r--
				rr := getu4(s[r:])
				if rr < 0 {
					return
				}
				r += 6
				if utf16.IsSurrogate(rr) {
					rr1 := getu4(s[r:])
					if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
						// A valid pair; consume.
						r += 6
						w += utf8.EncodeRune(b[w:], dec)
						break
					}
					// Invalid surrogate; fall back to replacement rune.
					rr = unicode.ReplacementChar
				}
				w += utf8.EncodeRune(b[w:], rr)
			}

		// Quote, control characters are invalid.
		case c == '"', c < ' ':
			return

		// ASCII
		case c < utf8.RuneSelf:
			b[w] = c
			r++
			w++

		// Coerce to well-formed UTF-8.
		default:
			rr, size := utf8.DecodeRune(s[r:])
			r += size
			w += utf8.EncodeRune(b[w:], rr)
		}
	}
	return b[0:w], true
}
