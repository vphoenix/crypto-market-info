package justlendkeeper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/big"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

// FactBytes is a versioned deterministic wire encoding for member digests.
// Gob remains only a local-state/frozen-batch container; its global type IDs
// make gob bytes unsuitable for persistent cross-process fact identities.
func FactBytes(row any) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString("jl-keeper-fact-v1\x00")
	e := encodeFact(&out, reflect.ValueOf(row))
	return out.Bytes(), e
}
func factText(out *bytes.Buffer, b []byte) {
	var n [10]byte
	k := binary.PutUvarint(n[:], uint64(len(b)))
	out.Write(n[:k])
	out.Write(b)
}
func encodeFact(out *bytes.Buffer, v reflect.Value) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			out.WriteByte(0)
			return nil
		}
		out.WriteByte(1)
		return encodeFact(out, v.Elem())
	}
	if v.CanInterface() {
		switch x := v.Interface().(type) {
		case time.Time:
			return binary.Write(out, binary.BigEndian, UTC(x).UnixMicro())
		case big.Int:
			if x.Sign() < 0 {
				out.WriteByte(1)
			} else {
				out.WriteByte(0)
			}
			factText(out, x.Bytes())
			return nil
		case decimal.Decimal:
			fixed := decimal.NewFromBigInt(x.Shift(18).BigInt(), -18)
			if !fixed.Equal(x) {
				return errors.New("canonical_decimal_precision_loss")
			}
			n := fixed.Coefficient()
			if n.Sign() < 0 {
				out.WriteByte(1)
			} else {
				out.WriteByte(0)
			}
			factText(out, n.Bytes())
			return nil
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		factText(out, []byte(v.Type().Name()))
		for i := 0; i < v.NumField(); i++ {
			factText(out, []byte(v.Type().Field(i).Name))
			if e := encodeFact(out, v.Field(i)); e != nil {
				return e
			}
		}
	case reflect.String:
		factText(out, []byte(v.String()))
	case reflect.Bool:
		if v.Bool() {
			out.WriteByte(1)
		} else {
			out.WriteByte(0)
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return binary.Write(out, binary.BigEndian, v.Uint())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return binary.Write(out, binary.BigEndian, v.Int())
	case reflect.Slice, reflect.Array:
		// An empty source list and an empty database list have the same facts.
		if e := binary.Write(out, binary.BigEndian, uint64(v.Len())); e != nil {
			return e
		}
		for i := 0; i < v.Len(); i++ {
			if e := encodeFact(out, v.Index(i)); e != nil {
				return e
			}
		}
	default:
		return errors.New("unsupported_canonical_fact_type")
	}
	return nil
}
