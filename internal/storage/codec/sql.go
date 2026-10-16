package codec

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// SQLValue validates the exact logical type before presenting a native scalar
// or versioned column BLOB to database/sql. No value ever passes through float.
func SQLValue(t *Type, value any) (any, error) {
	if _, e := Encode(t, value); e != nil {
		return nil, e
	}
	v := indirect(reflect.ValueOf(value))
	if t.Name == "Nullable" {
		if !v.IsValid() {
			return nil, nil
		}
		return SQLValue(t.Children[0], v.Interface())
	}
	if t.Name == "LowCardinality" {
		return SQLValue(t.Children[0], v.Interface())
	}
	switch t.Name {
	case "Array", "Tuple":
		return Encode(t, value)
	case "UInt256":
		n := v.Interface().(big.Int)
		a := make([]byte, 32)
		n.FillBytes(a)
		return a, nil
	case "Decimal":
		return v.Interface().(decimal.Decimal).StringFixed(18), nil
	case "UUID":
		u := v.Interface().(uuid.UUID)
		return u[:], nil
	case "Enum8":
		return int64(t.Enums[v.String()]), nil
	case "Bool":
		return v.Bool(), nil
	case "String", "FixedString":
		if v.Kind() == reflect.String {
			return []byte(v.String()), nil
		}
		if v.Kind() == reflect.Slice {
			return append([]byte(nil), v.Bytes()...), nil
		}
		a := make([]byte, v.Len())
		for i := range a {
			a[i] = byte(v.Index(i).Uint())
		}
		return a, nil
	case "DateTime", "DateTime64":
		return v.Interface().(time.Time).UTC(), nil
	case "UInt8", "UInt16", "UInt32", "UInt64":
		return v.Uint(), nil
	case "Int32", "Int64":
		return v.Int(), nil
	}
	return nil, fmt.Errorf("unsupported SQL type")
}

func FromSQL(t *Type, value any) (any, error) {
	if t.Name == "Nullable" {
		if value == nil {
			return nil, nil
		}
		return FromSQL(t.Children[0], value)
	}
	if value == nil {
		return nil, fmt.Errorf("unexpected SQL NULL for %s", t.Text)
	}
	if t.Name == "LowCardinality" {
		return FromSQL(t.Children[0], value)
	}
	var s string
	switch v := value.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	case int64:
		s = strconv.FormatInt(v, 10)
	case uint64:
		s = strconv.FormatUint(v, 10)
	}
	var out any
	switch t.Name {
	case "Array", "Tuple":
		a, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected BLOB")
		}
		return Decode(t, a)
	case "String", "FixedString":
		out = s
	case "UUID":
		a, ok := value.([]byte)
		if !ok || len(a) != 16 {
			return nil, fmt.Errorf("invalid UUID bytes")
		}
		u, e := uuid.FromBytes(a)
		if e != nil {
			return nil, e
		}
		out = u
	case "UInt256":
		a, ok := value.([]byte)
		if !ok || len(a) != 32 {
			return nil, fmt.Errorf("invalid UInt256 bytes")
		}
		out = *new(big.Int).SetBytes(a)
	case "Decimal":
		d, e := decimal.NewFromString(s)
		if e != nil {
			return nil, e
		}
		out = d
	case "Bool":
		n, e := strconv.ParseUint(s, 10, 8)
		if e != nil || n > 1 {
			return nil, fmt.Errorf("invalid SQL bool")
		}
		out = n == 1
	case "Enum8":
		n, e := strconv.ParseInt(s, 10, 8)
		if e != nil {
			return nil, e
		}
		found := false
		for name, code := range t.Enums {
			if int64(code) == n {
				out = name
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("invalid SQL enum")
		}
	case "UInt8", "UInt16", "UInt32", "UInt64":
		width := map[string]int{"UInt8": 8, "UInt16": 16, "UInt32": 32, "UInt64": 64}[t.Name]
		n, e := strconv.ParseUint(s, 10, width)
		if e != nil {
			return nil, e
		}
		switch t.Name {
		case "UInt8":
			out = uint8(n)
		case "UInt16":
			out = uint16(n)
		case "UInt32":
			out = uint32(n)
		case "UInt64":
			out = n
		}
	case "Int32", "Int64":
		width := 64
		if t.Name == "Int32" {
			width = 32
		}
		n, e := strconv.ParseInt(s, 10, width)
		if e != nil {
			return nil, e
		}
		if width == 32 {
			out = int32(n)
		} else {
			out = n
		}
	case "DateTime", "DateTime64":
		ts, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("timestamp not parsed")
		}
		out = ts.UTC()
	default:
		return nil, fmt.Errorf("unsupported SQL type")
	}
	if _, e := Encode(t, out); e != nil {
		return nil, e
	}
	return out, nil
}

// Assign adapts decoded typed arrays and named tuples to existing domain
// models. Numeric conversion must be exact and within the destination width.
func Assign(dst reflect.Value, src any) error {
	if !dst.CanSet() {
		return fmt.Errorf("destination cannot be set")
	}
	if src == nil {
		switch dst.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			dst.SetZero()
			return nil
		}
		return fmt.Errorf("unexpected NULL destination")
	}
	v := reflect.ValueOf(src)
	if v.Type().AssignableTo(dst.Type()) {
		dst.Set(v)
		return nil
	}
	if dst.Kind() == reflect.Pointer {
		dst.Set(reflect.New(dst.Type().Elem()))
		return Assign(dst.Elem(), src)
	}
	if dst.Kind() == reflect.Slice {
		a, ok := src.([]any)
		if !ok {
			return fmt.Errorf("expected decoded array")
		}
		dst.Set(reflect.MakeSlice(dst.Type(), len(a), len(a)))
		for i := range a {
			if e := Assign(dst.Index(i), a[i]); e != nil {
				return e
			}
		}
		return nil
	}
	if dst.Kind() == reflect.Struct {
		m, ok := src.(map[string]any)
		if !ok {
			return fmt.Errorf("expected decoded tuple")
		}
		for i := 0; i < dst.NumField(); i++ {
			f := dst.Type().Field(i)
			name := f.Tag.Get("ch")
			if name == "" {
				name = f.Tag.Get("json")
			}
			x, ok := m[name]
			if !ok {
				return fmt.Errorf("tuple field missing %s", name)
			}
			if e := Assign(dst.Field(i), x); e != nil {
				return e
			}
		}
		return nil
	}
	if dst.Kind() == reflect.String && v.Kind() == reflect.String {
		dst.SetString(v.String())
		return nil
	}
	if dst.Kind() >= reflect.Uint && dst.Kind() <= reflect.Uint64 && v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
		n := v.Uint()
		if dst.OverflowUint(n) {
			return fmt.Errorf("unsigned assignment overflow")
		}
		dst.SetUint(n)
		return nil
	}
	if dst.Kind() >= reflect.Int && dst.Kind() <= reflect.Int64 && v.Kind() >= reflect.Int && v.Kind() <= reflect.Int64 {
		n := v.Int()
		if dst.OverflowInt(n) {
			return fmt.Errorf("signed assignment overflow")
		}
		dst.SetInt(n)
		return nil
	}
	return fmt.Errorf("cannot assign %s to %s", v.Type(), dst.Type())
}
