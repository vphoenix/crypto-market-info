package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Encode writes a version byte, 128-bit column type fingerprint, followed by
// bounded typed data. Integers use big endian; Decimal(38,18) is signed 128-bit.
func Encode(t *Type, value any) ([]byte, error) {
	return encode(t, value, MaxElements)
}
func encode(t *Type, value any, limit int) ([]byte, error) {
	if t == nil {
		return nil, fmt.Errorf("nil type")
	}
	b := bytes.NewBuffer(make([]byte, 0, 128))
	b.WriteByte(Version)
	b.Write(t.Tag[:])
	budget := limit
	if e := writeValue(b, t, reflect.ValueOf(value), &budget); e != nil {
		return nil, e
	}
	if b.Len() > MaxBytes {
		return nil, fmt.Errorf("encoded value exceeds byte limit")
	}
	return b.Bytes(), nil
}
func indirect(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}
func writeValue(b *bytes.Buffer, t *Type, v reflect.Value, budget *int) error {
	if b.Len() > MaxBytes {
		return fmt.Errorf("value exceeds byte limit")
	}
	v = indirect(v)
	if t.Name == "Nullable" {
		if !v.IsValid() {
			b.WriteByte(0)
			return nil
		}
		b.WriteByte(1)
		return writeValue(b, t.Children[0], v, budget)
	}
	if !v.IsValid() {
		return fmt.Errorf("NULL for %s", t.Text)
	}
	if t.Name == "LowCardinality" {
		return writeValue(b, t.Children[0], v, budget)
	}
	switch t.Name {
	case "Array":
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return fmt.Errorf("expected array")
		}
		n := v.Len()
		if n > *budget {
			return fmt.Errorf("array element limit")
		}
		*budget -= n
		binary.Write(b, binary.BigEndian, uint32(n))
		for i := 0; i < n; i++ {
			if e := writeValue(b, t.Children[0], v.Index(i), budget); e != nil {
				return fmt.Errorf("element %d: %w", i, e)
			}
		}
	case "Tuple":
		for i, name := range t.Fields {
			var f reflect.Value
			switch v.Kind() {
			case reflect.Map:
				if v.Type().Key().Kind() != reflect.String {
					return fmt.Errorf("tuple key type")
				}
				f = v.MapIndex(reflect.ValueOf(name).Convert(v.Type().Key()))
			case reflect.Struct:
				for j := 0; j < v.NumField(); j++ {
					sf := v.Type().Field(j)
					if sf.Tag.Get("ch") == name || sf.Tag.Get("json") == name {
						f = v.Field(j)
						break
					}
				}
			default:
				return fmt.Errorf("expected named tuple")
			}
			if !f.IsValid() {
				return fmt.Errorf("missing tuple field %s", name)
			}
			if e := writeValue(b, t.Children[i], f, budget); e != nil {
				return fmt.Errorf("%s: %w", name, e)
			}
		}
	case "Bool":
		if v.Kind() != reflect.Bool {
			return fmt.Errorf("expected bool")
		}
		if v.Bool() {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
	case "UInt8", "UInt16", "UInt32", "UInt64":
		if v.Kind() < reflect.Uint || v.Kind() > reflect.Uint64 {
			return fmt.Errorf("expected unsigned integer")
		}
		n := v.Uint()
		size := map[string]int{"UInt8": 1, "UInt16": 2, "UInt32": 4, "UInt64": 8}[t.Name]
		if size < 8 && n > uint64(1)<<(8*size)-1 {
			return fmt.Errorf("unsigned overflow")
		}
		var a [8]byte
		binary.BigEndian.PutUint64(a[:], n)
		b.Write(a[8-size:])
	case "Int32", "Int64":
		if v.Kind() < reflect.Int || v.Kind() > reflect.Int64 {
			return fmt.Errorf("expected signed integer")
		}
		n := v.Int()
		if t.Name == "Int32" && (n < -1<<31 || n > 1<<31-1) {
			return fmt.Errorf("signed overflow")
		}
		if t.Name == "Int32" {
			binary.Write(b, binary.BigEndian, int32(n))
		} else {
			binary.Write(b, binary.BigEndian, n)
		}
	case "UInt256":
		n, ok := v.Interface().(big.Int)
		if !ok {
			return fmt.Errorf("expected big.Int")
		}
		if n.Sign() < 0 || n.BitLen() > 256 {
			return fmt.Errorf("UInt256 out of range")
		}
		var a [32]byte
		n.FillBytes(a[:])
		b.Write(a[:])
	case "Decimal":
		d, ok := v.Interface().(decimal.Decimal)
		if !ok {
			return fmt.Errorf("expected decimal.Decimal")
		}
		s, e := DecimalCoefficient(d)
		if e != nil {
			return e
		}
		if s.Sign() < 0 {
			s.Add(s, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		var a [16]byte
		s.FillBytes(a[:])
		b.Write(a[:])
	case "String", "FixedString":
		var a []byte
		if v.Kind() == reflect.String {
			a = []byte(v.String())
		} else if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			a = v.Bytes()
		} else if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			a = make([]byte, v.Len())
			for i := range a {
				a[i] = byte(v.Index(i).Uint())
			}
		} else {
			return fmt.Errorf("expected bytes or string")
		}
		if t.Name == "FixedString" {
			if len(a) != t.Size {
				return fmt.Errorf("fixed length %d != %d", len(a), t.Size)
			}
		} else {
			if len(a) > MaxBytes {
				return fmt.Errorf("string length limit")
			}
			binary.Write(b, binary.BigEndian, uint32(len(a)))
		}
		if b.Len()+len(a) > MaxBytes {
			return fmt.Errorf("byte limit")
		}
		b.Write(a)
	case "UUID":
		u, ok := v.Interface().(uuid.UUID)
		if !ok {
			return fmt.Errorf("expected UUID")
		}
		b.Write(u[:])
	case "Enum8":
		if v.Kind() == reflect.String {
			n, ok := t.Enums[v.String()]
			if !ok {
				return fmt.Errorf("unknown enum value")
			}
			b.WriteByte(byte(n))
		} else {
			return fmt.Errorf("expected named enum")
		}
	case "DateTime", "DateTime64":
		ts, ok := v.Interface().(time.Time)
		if !ok || ts.IsZero() {
			return fmt.Errorf("expected timestamp")
		}
		unit := int64(time.Second)
		if t.Scale == 3 {
			unit = int64(time.Millisecond)
		} else if t.Scale == 6 {
			unit = int64(time.Microsecond)
		}
		if ts.Nanosecond()%int(unit) != 0 {
			return fmt.Errorf("timestamp precision loss")
		}
		if ts.Year() < 1970 || ts.Year() > 9999 {
			return fmt.Errorf("timestamp range")
		}
		n := ts.Unix()
		if t.Scale == 3 {
			n = ts.UnixMilli()
		} else if t.Scale == 6 {
			n = ts.UnixMicro()
		}
		binary.Write(b, binary.BigEndian, n)
	default:
		return fmt.Errorf("unsupported type")
	}
	return nil
}

// DecimalCoefficient rejects precision loss and overflow instead of rounding.
func DecimalCoefficient(d decimal.Decimal) (*big.Int, error) {
	if d.Coefficient().Sign() == 0 {
		return new(big.Int), nil
	}
	if d.Exponent() < -4096 || d.Exponent() > 38 || d.Coefficient().BitLen() > 13600 {
		return nil, fmt.Errorf("decimal representation exceeds bounded precision")
	}
	s := d.Shift(18)
	n := s.BigInt()
	if !s.Equal(decimal.NewFromBigInt(n, 0)) {
		return nil, fmt.Errorf("decimal exceeds scale 18")
	}
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if new(big.Int).Abs(new(big.Int).Set(n)).Cmp(limit) >= 0 {
		return nil, fmt.Errorf("decimal exceeds precision 38")
	}
	return n, nil
}

func Decode(t *Type, data []byte) (any, error) {
	return decode(t, data, MaxElements)
}
func decode(t *Type, data []byte, limit int) (any, error) {
	if t == nil || len(data) < 17 || len(data) > MaxBytes || data[0] != Version || !bytes.Equal(data[1:17], t.Tag[:]) {
		return nil, fmt.Errorf("invalid codec header")
	}
	r := bytes.NewReader(data[17:])
	budget := limit
	v, e := readValue(r, t, &budget)
	if e != nil {
		return nil, e
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("trailing bytes")
	}
	return v, nil
}
func take(r *bytes.Reader, n int) ([]byte, error) {
	if n < 0 || n > r.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	a := make([]byte, n)
	_, e := io.ReadFull(r, a)
	return a, e
}
func readValue(r *bytes.Reader, t *Type, budget *int) (any, error) {
	if t.Name == "Nullable" {
		n, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		if n == 0 {
			return nil, nil
		}
		if n != 1 {
			return nil, fmt.Errorf("invalid NULL marker")
		}
		return readValue(r, t.Children[0], budget)
	}
	if t.Name == "LowCardinality" {
		return readValue(r, t.Children[0], budget)
	}
	switch t.Name {
	case "Array":
		var n uint32
		if e := binary.Read(r, binary.BigEndian, &n); e != nil {
			return nil, e
		}
		if uint64(n) > uint64(*budget) {
			return nil, fmt.Errorf("array limit")
		}
		if uint64(n)*uint64(minimumSize(t.Children[0])) > uint64(r.Len()) {
			return nil, io.ErrUnexpectedEOF
		}
		*budget -= int(n)
		a := make([]any, int(n))
		for i := range a {
			v, e := readValue(r, t.Children[0], budget)
			if e != nil {
				return nil, e
			}
			a[i] = v
		}
		return a, nil
	case "Tuple":
		a := make(map[string]any, len(t.Fields))
		for i, n := range t.Fields {
			v, e := readValue(r, t.Children[i], budget)
			if e != nil {
				return nil, e
			}
			a[n] = v
		}
		return a, nil
	case "Bool":
		n, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		if n > 1 {
			return nil, fmt.Errorf("invalid bool")
		}
		return n == 1, nil
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int32", "Int64":
		size := map[string]int{"UInt8": 1, "UInt16": 2, "UInt32": 4, "UInt64": 8, "Int32": 4, "Int64": 8}[t.Name]
		a, e := take(r, size)
		if e != nil {
			return nil, e
		}
		var raw [8]byte
		copy(raw[8-size:], a)
		n := binary.BigEndian.Uint64(raw[:])
		switch t.Name {
		case "UInt8":
			return uint8(n), nil
		case "UInt16":
			return uint16(n), nil
		case "UInt32":
			return uint32(n), nil
		case "UInt64":
			return n, nil
		case "Int32":
			return int32(n), nil
		default:
			return int64(n), nil
		}
	case "UInt256":
		a, e := take(r, 32)
		if e != nil {
			return nil, e
		}
		return *new(big.Int).SetBytes(a), nil
	case "Decimal":
		a, e := take(r, 16)
		if e != nil {
			return nil, e
		}
		n := new(big.Int).SetBytes(a)
		if a[0]&128 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		d := decimal.NewFromBigInt(n, -18)
		if _, e = DecimalCoefficient(d); e != nil {
			return nil, e
		}
		return d, nil
	case "String", "FixedString":
		n := t.Size
		if t.Name == "String" {
			var size uint32
			if e := binary.Read(r, binary.BigEndian, &size); e != nil {
				return nil, e
			}
			if size > MaxBytes {
				return nil, fmt.Errorf("string limit")
			}
			n = int(size)
		}
		a, e := take(r, n)
		return string(a), e
	case "UUID":
		a, e := take(r, 16)
		if e != nil {
			return nil, e
		}
		return uuid.FromBytes(a)
	case "Enum8":
		n, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		for s, v := range t.Enums {
			if v == int8(n) {
				return s, nil
			}
		}
		return nil, fmt.Errorf("invalid enum code")
	case "DateTime", "DateTime64":
		var n int64
		if e := binary.Read(r, binary.BigEndian, &n); e != nil {
			return nil, e
		}
		var ts time.Time
		switch t.Scale {
		case 0:
			ts = time.Unix(n, 0)
		case 3:
			ts = time.UnixMilli(n)
		case 6:
			ts = time.UnixMicro(n)
		}
		if ts.Year() < 1970 || ts.Year() > 9999 {
			return nil, fmt.Errorf("timestamp range")
		}
		return ts.UTC(), nil
	}
	return nil, fmt.Errorf("unsupported type")
}
