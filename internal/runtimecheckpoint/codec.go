package runtimecheckpoint

import (
	"encoding"
	"encoding/gob"
	"fmt"
	"reflect"
	"strconv"
)

// Typed encoding keeps the domain's concrete structs. Gob alone loses a known
// zero behind a pointer; the small presence tree restores those NULL/zero
// distinctions without converting integer/decimal values or binary identities
// to JSON. No registry, untyped events or database facts are defined here.
type presence struct {
	Pointer    bool
	NilElement bool
	Children   map[string]presence
}
type typedEnvelope struct {
	Version uint32
	Body    []byte
	Present presence
}

var gobEncoderType = reflect.TypeOf((*gob.GobEncoder)(nil)).Elem()
var binaryEncoderType = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()

func atomicCodecType(t reflect.Type) bool {
	return t.Implements(gobEncoderType) || reflect.PointerTo(t).Implements(gobEncoderType) || t.Implements(binaryEncoderType) || reflect.PointerTo(t).Implements(binaryEncoderType)
}
func mapKey(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	default:
		return "", fmt.Errorf("runtime codec unsupported map key %s", v.Type())
	}
}

func capturePresence(v reflect.Value, depth int) (presence, error) {
	var p presence
	if depth > 64 {
		return p, ErrCapacity
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return p, nil
		}
		p.Pointer = true
		inner, err := capturePresence(v.Elem(), depth+1)
		if err != nil {
			return p, err
		}
		p.Children = map[string]presence{"*": inner}
		return p, nil
	}
	if atomicCodecType(v.Type()) {
		return p, nil
	}
	add := func(key string, child reflect.Value) error {
		nested, err := capturePresence(child, depth+1)
		if err != nil {
			return err
		}
		if v.Kind() != reflect.Struct && child.Kind() == reflect.Pointer && child.IsNil() {
			nested.NilElement = true
		}
		if nested.Pointer || nested.NilElement || len(nested.Children) > 0 {
			if p.Children == nil {
				p.Children = map[string]presence{}
			}
			p.Children[key] = nested
		}
		return nil
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				if err := add(strconv.Itoa(i), v.Field(i)); err != nil {
					return p, err
				}
			}
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := add(strconv.Itoa(i), v.Index(i)); err != nil {
				return p, err
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			key, err := mapKey(iter.Key())
			if err != nil {
				return p, err
			}
			if err = add(key, iter.Value()); err != nil {
				return p, err
			}
		}
	case reflect.Interface:
		return p, fmt.Errorf("runtime codec requires concrete domain types")
	}
	return p, nil
}

func restorePresence(v reflect.Value, p presence, depth int) error {
	if depth > 64 {
		return ErrCapacity
	}
	if p.NilElement {
		if p.Pointer || len(p.Children) != 0 || v.Kind() != reflect.Pointer || (!v.IsNil() && !v.Elem().IsZero()) {
			return ErrRecovery
		}
		v.SetZero()
		return nil
	}
	if v.Kind() == reflect.Pointer {
		if !p.Pointer {
			if !v.IsNil() {
				return fmt.Errorf("pointer presence mismatch: %w", ErrRecovery)
			}
			return nil
		}
		if len(p.Children) != 1 {
			return ErrRecovery
		}
		inner, ok := p.Children["*"]
		if !ok {
			return ErrRecovery
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return restorePresence(v.Elem(), inner, depth+1)
	}
	if p.Pointer {
		return ErrRecovery
	}
	if atomicCodecType(v.Type()) {
		if len(p.Children) > 0 {
			return ErrRecovery
		}
		return nil
	}
	visited := 0
	child := func(key string, val reflect.Value) error {
		nested, ok := p.Children[key]
		if ok {
			visited++
		}
		return restorePresence(val, nested, depth+1)
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				if err := child(strconv.Itoa(i), v.Field(i)); err != nil {
					return err
				}
			}
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := child(strconv.Itoa(i), v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			key, err := mapKey(iter.Key())
			if err != nil {
				return err
			}
			value := reflect.New(v.Type().Elem()).Elem()
			value.Set(iter.Value())
			if err = child(key, value); err != nil {
				return err
			}
			v.SetMapIndex(iter.Key(), value)
		}
	case reflect.Interface:
		return fmt.Errorf("runtime codec requires concrete domain types")
	}
	if visited != len(p.Children) {
		return fmt.Errorf("unknown pointer presence member: %w", ErrRecovery)
	}
	return nil
}

func MarshalTyped(v any) ([]byte, error) {
	x := reflect.ValueOf(v)
	if !x.IsValid() {
		return nil, ErrRecovery
	}
	if x.Kind() == reflect.Pointer {
		if x.IsNil() {
			return nil, ErrRecovery
		}
		x = x.Elem()
		v = x.Interface()
	}
	p, err := capturePresence(x, 0)
	if err != nil {
		return nil, err
	}
	body, err := encode(gobSafe(x, false).Interface())
	if err != nil {
		return nil, err
	}
	return encode(typedEnvelope{Version: 1, Body: body, Present: p})
}

// Gob disallows nil pointer elements in maps/slices. Encode a zero placeholder
// only in those positions; the explicit NilElement marker restores the nil.
// Struct fields retain gob's native nil representation. The caller is never
// mutated, including nested map values and immutable worker views.
func gobSafe(v reflect.Value, element bool) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if element {
				return reflect.New(v.Type().Elem())
			}
			return v
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(gobSafe(v.Elem(), false))
		return out
	}
	if atomicCodecType(v.Type()) {
		return v
	}
	switch v.Kind() {
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(gobSafe(v.Field(i), false))
			}
		}
		return out
	case reflect.Array, reflect.Slice:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Slice {
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		}
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(gobSafe(v.Index(i), true))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), gobSafe(iter.Value(), true))
		}
		return out
	default:
		return v
	}
}
func UnmarshalTyped(raw []byte, destination any) error {
	x := reflect.ValueOf(destination)
	if !x.IsValid() || x.Kind() != reflect.Pointer || x.IsNil() {
		return ErrRecovery
	}
	var env typedEnvelope
	if err := decode(raw, &env); err != nil {
		return err
	}
	if env.Version != 1 {
		return ErrRecovery
	}
	// Decode into a fresh value: gob's omitted zero fields must never inherit
	// fields left over in a reused destination object.
	fresh := reflect.New(x.Elem().Type())
	if err := decode(env.Body, fresh.Interface()); err != nil {
		return err
	}
	if err := restorePresence(fresh.Elem(), env.Present, 0); err != nil {
		return err
	}
	x.Elem().Set(fresh.Elem())
	return nil
}
