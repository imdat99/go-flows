package flows

import (
	"bytes"
	"encoding"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"reflect"
	"sync"
)

// Ported from saga (jsonslot.go, same author).
//
// jsonSlot encodes values of T through a pooled *T when that provably gives
// the same bytes as encoding the value.
//
// Why: encoding a non-pointer value boxes it into an interface (an
// allocation unless it is pointer-shaped or a small integer) and makes the
// json package copy it into a fresh addressable value (reflect.New, another
// allocation). Through a pointer taken from a pool, neither happens.
//
// When it is the same: under encoding/json's (v1) rules, methods with a
// pointer receiver (MarshalJSON, MarshalText, ...) are only called on
// addressable values. Encoding &v makes v, and everything reached from v
// through struct fields and array elements, addressable; values reached
// through pointers, slices, maps and interfaces are equally addressable (or
// not) either way. So the bytes can differ only if one of those types has
// such a method on its pointer and not on its value: then the value is
// encoded as before.
type jsonSlot[T any] struct {
	byPtr    bool
	hasIface bool      // decoding T can create interface values
	pool     sync.Pool // of *T, zeroed
}

func newJSONSlot[T any]() *jsonSlot[T] {
	t := reflect.TypeFor[T]()
	return &jsonSlot[T]{byPtr: addrIndependent(t), hasIface: holdsInterface(t, map[reflect.Type]bool{})}
}

// marshal is json.Marshal(v).
func (s *jsonSlot[T]) marshal(v T) ([]byte, error) {
	if !s.byPtr {
		return json.Marshal(v)
	}
	p := s.get(v)
	defer s.put(p)
	return jsonv2.Marshal(p, jsonV1)
}

func (s *jsonSlot[T]) get(v T) *T {
	p, _ := s.pool.Get().(*T)
	if p == nil {
		p = new(T)
	}
	*p = v
	return p
}

// put zeroes p (no reference to the caller's data stays in the pool).
func (s *jsonSlot[T]) put(p *T) {
	var zero T
	*p = zero
	s.pool.Put(p)
}

var marshalIfaces = []reflect.Type{
	reflect.TypeFor[json.Marshaler](),
	reflect.TypeFor[jsonv2.MarshalerTo](),
	reflect.TypeFor[encoding.TextMarshaler](),
	reflect.TypeFor[encoding.TextAppender](),
}

// addrIndependent reports whether encoding a value of type t gives the same
// bytes as encoding a pointer to it (see jsonSlot). Interfaces are left
// alone (encoded by value) to stay on the safe side.
func addrIndependent(t reflect.Type) bool {
	if t.Kind() == reflect.Interface {
		return false
	}
	return addrWalk(t, map[reflect.Type]bool{})
}

func addrWalk(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return true
	}
	seen[t] = true
	if t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface {
		pt := reflect.PointerTo(t)
		for _, i := range marshalIfaces {
			if pt.Implements(i) && !t.Implements(i) {
				return false
			}
		}
	}
	switch t.Kind() {
	case reflect.Struct:
		for f := range t.Fields() { // unexported fields too: embedded ones are encoded
			if !addrWalk(f.Type, seen) {
				return false
			}
		}
	case reflect.Array:
		return addrWalk(t.Elem(), seen)
	default:
	}
	return true
}

// jsonV1 makes encoding/json/v2 produce exactly what encoding/json.Marshal
// does.
var jsonV1 = json.DefaultOptionsV1()

// codec encodes and decodes the values of one type for storage (JSON,
// in bytea columns: any byte is storable, so encoded values never need the
// UTF-8 and NUL checks text and jsonb columns would).
type codec[T any] struct{ slot *jsonSlot[T] }

// codecs caches one slot per type: building one walks the type with
// reflection and owns a pool, so it must not happen per call.
var codecs sync.Map // reflect.Type -> any (*jsonSlot[T])

// newCodec returns the codec of T (cached).
func newCodec[T any]() codec[T] {
	t := reflect.TypeFor[T]()
	if s, ok := codecs.Load(t); ok {
		if slot, ok := s.(*jsonSlot[T]); ok { // keyed by T: always true
			return codec[T]{slot: slot}
		}
	}
	s, _ := codecs.LoadOrStore(t, newJSONSlot[T]())
	slot, _ := s.(*jsonSlot[T])
	return codec[T]{slot: slot}
}

func (c codec[T]) encode(v T) ([]byte, error) { return c.slot.marshal(v) }

// decode decodes b; empty or nil is the zero value. Types that hold
// interfaces decode numbers as json.Number, so integers beyond 2^53 keep
// their value (GW-18: go-workflows went through float64).
func (c codec[T]) decode(b []byte) (T, error) {
	var v T
	if len(b) == 0 {
		return v, nil
	}
	if !c.slot.hasIface {
		err := json.Unmarshal(b, &v)
		return v, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return v, err
	}
	if d.InputOffset() != int64(len(bytes.TrimRight(b, " \t\r\n"))) {
		return v, errors.New("invalid character after top-level value")
	}
	return v, nil
}

// holdsInterface reports whether decoding into t can create interface
// values (where numbers become float64 unless UseNumber).
func holdsInterface(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Interface:
		return true
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return holdsInterface(t.Elem(), seen)
	case reflect.Map:
		return holdsInterface(t.Key(), seen) || holdsInterface(t.Elem(), seen)
	case reflect.Struct:
		for f := range t.Fields() {
			if holdsInterface(f.Type, seen) {
				return true
			}
		}
	default:
	}
	return false
}
