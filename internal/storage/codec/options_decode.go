package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

var optionMinimums sync.Map

// The wire schema describes fixed Go arrays as Array. Their actual fixed
// lengths still matter for allocation admission: [60]Quality is not four bytes.
func minimumOptionSize(t reflect.Type) int {
	if v, ok := optionMinimums.Load(t); ok {
		return v.(int)
	}
	var n int
	switch t {
	case timeType:
		n = 2
	case uuidType, decimalType:
		n = 16
	default:
		switch t.Kind() {
		case reflect.Pointer, reflect.Bool, reflect.Uint8:
			n = 1
		case reflect.Uint16:
			n = 2
		case reflect.Uint32, reflect.Int32, reflect.String, reflect.Slice:
			n = 4
		case reflect.Uint64, reflect.Int64:
			n = 8
		case reflect.Array:
			n = 4 + t.Len()*minimumOptionSize(t.Elem())
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				n += minimumOptionSize(t.Field(i).Type)
			}
		default:
			n = 1
		}
	}
	optionMinimums.Store(t, n)
	return n
}

// Decode directly into the fixed domain model. The wire schema and all length
// limits are unchanged; avoiding an intermediate map for every tuple bounds
// the large quality/evidence arrays during preparation.
func readOption(r *bytes.Reader, t *Type, dst reflect.Value, budget *int) error {
	if t.Name == "Nullable" {
		marker, e := r.ReadByte()
		if e != nil {
			return e
		}
		if dst.Kind() != reflect.Pointer {
			return fmt.Errorf("invalid delivery nullable destination")
		}
		if marker == 0 {
			dst.SetZero()
			return nil
		}
		if marker != 1 {
			return fmt.Errorf("invalid NULL marker")
		}
		dst.Set(reflect.New(dst.Type().Elem()))
		return readOption(r, t.Children[0], dst.Elem(), budget)
	}
	if dst.Type() == timeType {
		present, e := readValue(r, t.Children[0], budget)
		if e != nil {
			return e
		}
		value, e := readValue(r, t.Children[1], budget)
		if e != nil {
			return e
		}
		if !present.(bool) {
			if value != nil {
				return fmt.Errorf("invalid zero time")
			}
			dst.SetZero()
			return nil
		}
		if value == nil {
			return fmt.Errorf("missing delivery time")
		}
		dst.Set(reflect.ValueOf(value.(time.Time)))
		return nil
	}
	if dst.Type() == uuidType || dst.Type() == decimalType {
		v, e := readValue(r, t, budget)
		if e != nil {
			return e
		}
		return Assign(dst, v)
	}
	switch t.Name {
	case "Array":
		var count uint32
		if e := binary.Read(r, binary.BigEndian, &count); e != nil {
			return e
		}
		if uint64(count) > uint64(*budget) {
			return fmt.Errorf("array limit")
		}
		// Enforce the domain's existing cardinality limits before allocating
		// wide fixed quality arrays, rather than only in Validate afterwards.
		limit := uint32(1 << 21)
		switch dst.Type() {
		case reflect.TypeOf([]model.DerivativeMinuteBatch{}), reflect.TypeOf([]options.QualityEvidence{}):
			limit = options.MaxLiveBooks
		case reflect.TypeOf([]options.IndexMinute{}):
			limit = 4
		case reflect.TypeOf([]model.BookDelta{}):
			limit = 59
		}
		if count > limit {
			return fmt.Errorf("delivery member array limit")
		}
		if uint64(count)*uint64(minimumOptionSize(dst.Type().Elem())) > uint64(r.Len()) {
			return io.ErrUnexpectedEOF
		}
		*budget -= int(count)
		if dst.Kind() == reflect.Array {
			if dst.Len() != int(count) {
				return fmt.Errorf("delivery fixed array length")
			}
		} else if dst.Kind() == reflect.Slice {
			dst.Set(reflect.MakeSlice(dst.Type(), int(count), int(count)))
		} else {
			return fmt.Errorf("delivery array required")
		}
		for n := 0; n < int(count); n++ {
			if e := readOption(r, t.Children[0], dst.Index(n), budget); e != nil {
				return fmt.Errorf("element %d: %w", n, e)
			}
		}
		return nil
	case "Tuple":
		if dst.Kind() != reflect.Struct || dst.NumField() != len(t.Children) {
			return fmt.Errorf("delivery tuple required")
		}
		for n, child := range t.Children {
			if dst.Type().Field(n).Name != t.Fields[n] {
				return fmt.Errorf("delivery field mismatch")
			}
			if e := readOption(r, child, dst.Field(n), budget); e != nil {
				return fmt.Errorf("%s: %w", t.Fields[n], e)
			}
		}
		return nil
	default:
		v, e := readValue(r, t, budget)
		if e != nil {
			return e
		}
		return Assign(dst, v)
	}
}
