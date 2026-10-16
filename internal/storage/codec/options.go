package codec

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Option delivery uses the same versioned, bounded scalar/array/tuple codec as
// facts. This schema is derived solely from these fixed domain structs; maps,
// interface values, floating point, and source JSON are not accepted.
var optionSchemas sync.Map
var timeType = reflect.TypeOf(time.Time{})
var decimalType = reflect.TypeOf(decimal.Decimal{})
var uuidType = reflect.TypeOf(uuid.UUID{})

func optionSchema(t reflect.Type) (*Type, error) {
	if cached, ok := optionSchemas.Load(t); ok {
		return cached.(*Type), nil
	}
	s, e := optionTypeText(t)
	if e != nil {
		return nil, e
	}
	v, e := Parse(s)
	if e != nil {
		return nil, e
	}
	actual, _ := optionSchemas.LoadOrStore(t, v)
	return actual.(*Type), nil
}
func optionTypeText(t reflect.Type) (string, error) {
	switch t {
	case timeType:
		return "Tuple(present Bool, value Nullable(DateTime64(6, 'UTC')))", nil
	case decimalType:
		return "Decimal(38, 18)", nil
	case uuidType:
		return "UUID", nil
	}
	switch t.Kind() {
	case reflect.Pointer:
		s, e := optionTypeText(t.Elem())
		return "Nullable(" + s + ")", e
	case reflect.Slice, reflect.Array:
		s, e := optionTypeText(t.Elem())
		return "Array(" + s + ")", e
	case reflect.Struct:
		fields := []string{}
		for n := 0; n < t.NumField(); n++ {
			f := t.Field(n)
			if !f.IsExported() {
				return "", fmt.Errorf("unexported delivery field")
			}
			s, e := optionTypeText(f.Type)
			if e != nil {
				return "", e
			}
			fields = append(fields, f.Name+" "+s)
		}
		return "Tuple(" + strings.Join(fields, ", ") + ")", nil
	case reflect.Bool:
		return "Bool", nil
	case reflect.String:
		return "String", nil
	case reflect.Uint8:
		return "UInt8", nil
	case reflect.Uint16:
		return "UInt16", nil
	case reflect.Uint32:
		return "UInt32", nil
	case reflect.Uint64:
		return "UInt64", nil
	case reflect.Int32:
		return "Int32", nil
	case reflect.Int64:
		return "Int64", nil
	}
	return "", fmt.Errorf("unsupported delivery field %s", t)
}
func optionValue(v reflect.Value) any {
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		if t.IsZero() {
			return map[string]any{"present": false, "value": nil}
		}
		return map[string]any{"present": true, "value": t}
	}
	if v.Type() == decimalType || v.Type() == uuidType {
		return v.Interface()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return optionValue(v.Elem())
	case reflect.Array, reflect.Slice:
		a := make([]any, v.Len())
		for n := range a {
			a[n] = optionValue(v.Index(n))
		}
		return a
	case reflect.Struct:
		m := map[string]any{}
		for n := 0; n < v.NumField(); n++ {
			m[v.Type().Field(n).Name] = optionValue(v.Field(n))
		}
		return m
	}
	return v.Interface()
}
func assignOption(dst reflect.Value, src any) error {
	if dst.Type() == timeType {
		m, ok := src.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid delivery time")
		}
		if !m["present"].(bool) {
			if m["value"] != nil {
				return fmt.Errorf("invalid zero time")
			}
			dst.SetZero()
			return nil
		}
		if m["value"] == nil {
			return fmt.Errorf("missing delivery time")
		}
		dst.Set(reflect.ValueOf(m["value"].(time.Time)))
		return nil
	}
	if dst.Type() == uuidType || dst.Type() == decimalType {
		return Assign(dst, src)
	}
	if src == nil {
		if dst.Kind() != reflect.Pointer {
			return fmt.Errorf("invalid delivery NULL")
		}
		dst.SetZero()
		return nil
	}
	switch dst.Kind() {
	case reflect.Pointer:
		dst.Set(reflect.New(dst.Type().Elem()))
		return assignOption(dst.Elem(), src)
	case reflect.Array, reflect.Slice:
		a, ok := src.([]any)
		if !ok {
			return fmt.Errorf("delivery array required")
		}
		if dst.Kind() == reflect.Array {
			if dst.Len() != len(a) {
				return fmt.Errorf("delivery fixed array length")
			}
		} else {
			dst.Set(reflect.MakeSlice(dst.Type(), len(a), len(a)))
		}
		for n, v := range a {
			if e := assignOption(dst.Index(n), v); e != nil {
				return e
			}
		}
		return nil
	case reflect.Struct:
		m, ok := src.(map[string]any)
		if !ok {
			return fmt.Errorf("delivery tuple required")
		}
		for n := 0; n < dst.NumField(); n++ {
			v, ok := m[dst.Type().Field(n).Name]
			if !ok {
				return fmt.Errorf("missing delivery field")
			}
			if e := assignOption(dst.Field(n), v); e != nil {
				return e
			}
		}
		return nil
	}
	return Assign(dst, src)
}
func encodeOption(value any) ([]byte, error) {
	v := reflect.ValueOf(value)
	t, e := optionSchema(v.Type())
	if e != nil {
		return nil, e
	}
	return encode(t, optionValue(v), 1<<21)
}
func decodeOption(raw []byte, dest any) error {
	v := reflect.ValueOf(dest).Elem()
	t, e := optionSchema(v.Type())
	if e != nil {
		return e
	}
	if len(raw) < 17 || len(raw) > MaxBytes || raw[0] != Version || !bytes.Equal(raw[1:17], t.Tag[:]) {
		return fmt.Errorf("invalid codec header")
	}
	r := bytes.NewReader(raw[17:])
	budget := 1 << 21
	if e := readOption(r, t, v, &budget); e != nil {
		return e
	}
	if r.Len() != 0 {
		return fmt.Errorf("trailing bytes")
	}
	return nil
}
func EncodeOptionsLive(e options.LiveEnvelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return encodeOption(e)
}
func DecodeOptionsLive(raw []byte) (options.LiveEnvelope, error) {
	var e options.LiveEnvelope
	if err := decodeOption(raw, &e); err != nil {
		return e, err
	}
	return e, e.Validate()
}
func EncodeOptionsCatalog(e options.CatalogEnvelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return encodeOption(e)
}
func DecodeOptionsCatalog(raw []byte) (options.CatalogEnvelope, error) {
	var e options.CatalogEnvelope
	if err := decodeOption(raw, &e); err != nil {
		return e, err
	}
	return e, e.Validate()
}

// Parts preserve domain member boundaries in Redis. Quality/evidence slots and
// price-key deltas remain typed arrays, with no loss of null or empty values.
type OptionPart struct {
	Kind, ID string
	Raw      []byte
}

func OptionsParts(kind string, raw []byte) ([]byte, []OptionPart, error) {
	var live options.LiveEnvelope
	var catalog options.CatalogEnvelope
	var err error
	switch kind {
	case "options_live":
		live, err = DecodeOptionsLive(raw)
	case "options_catalog":
		catalog, err = DecodeOptionsCatalog(raw)
		live = catalog.Live
	default:
		return nil, nil, fmt.Errorf("unsupported option kind")
	}
	if err != nil {
		return nil, nil, err
	}
	parts := []OptionPart{}
	add := func(kind, id string, v any) error {
		raw, e := encodeOption(v)
		if e != nil {
			return e
		}
		parts = append(parts, OptionPart{kind, id, raw})
		return nil
	}
	for _, b := range live.Books {
		if err = add("book", fmt.Sprint(b.InstrumentID), b); err != nil {
			return nil, nil, err
		}
	}
	for _, m := range live.Indexes {
		if err = add("index", m.IndexID, m); err != nil {
			return nil, nil, err
		}
	}
	for _, q := range catalog.Evidence {
		if err = add("evidence", fmt.Sprint(q.InstrumentID), q); err != nil {
			return nil, nil, err
		}
	}
	live.Books = nil
	live.Indexes = nil
	if kind == "options_live" {
		meta, e := encodeOption(live)
		return meta, parts, e
	}
	catalog.Live = live
	catalog.Evidence = nil
	meta, e := encodeOption(catalog)
	return meta, parts, e
}
func JoinOptionsParts(kind string, meta []byte, parts []OptionPart) ([]byte, uint32, error) {
	var live options.LiveEnvelope
	var catalog options.CatalogEnvelope
	var err error
	if kind == "options_live" {
		err = decodeOption(meta, &live)
	} else if kind == "options_catalog" {
		err = decodeOption(meta, &catalog)
		live = catalog.Live
	} else {
		return nil, 0, fmt.Errorf("unsupported option kind")
	}
	if err != nil {
		return nil, 0, err
	}
	if len(live.Books) != 0 || len(live.Indexes) != 0 || len(catalog.Evidence) != 0 {
		return nil, 0, fmt.Errorf("nonempty option manifest")
	}
	phase := 0
	for _, p := range parts {
		switch p.Kind {
		case "book":
			if phase != 0 {
				return nil, 0, fmt.Errorf("part order")
			}
			var b model.DerivativeMinuteBatch
			if err = decodeOption(p.Raw, &b); err != nil {
				return nil, 0, err
			}
			if fmt.Sprint(b.InstrumentID) != p.ID {
				return nil, 0, fmt.Errorf("book part identity")
			}
			live.Books = append(live.Books, b)
		case "index":
			if phase > 1 {
				return nil, 0, fmt.Errorf("part order")
			}
			phase = 1
			var m options.IndexMinute
			if err = decodeOption(p.Raw, &m); err != nil {
				return nil, 0, err
			}
			if m.IndexID != p.ID {
				return nil, 0, fmt.Errorf("index part identity")
			}
			live.Indexes = append(live.Indexes, m)
		case "evidence":
			if kind != "options_catalog" {
				return nil, 0, fmt.Errorf("unexpected catalog evidence")
			}
			phase = 2
			var q options.QualityEvidence
			if err = decodeOption(p.Raw, &q); err != nil {
				return nil, 0, err
			}
			if fmt.Sprint(q.InstrumentID) != p.ID {
				return nil, 0, fmt.Errorf("evidence part identity")
			}
			catalog.Evidence = append(catalog.Evidence, q)
		default:
			return nil, 0, fmt.Errorf("unknown option part")
		}
	}
	if kind == "options_live" {
		raw, e := EncodeOptionsLive(live)
		return raw, uint32(len(live.Books)), e
	}
	catalog.Live = live
	raw, e := EncodeOptionsCatalog(catalog)
	return raw, uint32(len(live.Books)), e
}
func EncodePreview(frame contracts.PreviewFrame) ([]byte, error) {
	if (frame.Kind != "cex" && frame.Kind != "options_live" && frame.Kind != "options_catalog") || frame.Scope == "" || frame.At.IsZero() || !frame.At.Equal(frame.At.Truncate(time.Second)) || len(frame.Books)+len(frame.Derivatives) > 8192 {
		return nil, fmt.Errorf("invalid preview frame")
	}
	for _, b := range frame.Books {
		if b.InstrumentID == 0 || (b.StoredDepth != 5 && b.StoredDepth != 10) {
			return nil, fmt.Errorf("invalid preview book")
		}
		if b.Valid {
			if b.Snapshot.InstrumentID != b.InstrumentID {
				return nil, fmt.Errorf("preview identity mismatch")
			}
			if e := b.Snapshot.Validate(int(b.StoredDepth)); e != nil {
				return nil, e
			}
		}
	}
	return encodeOption(frame)
}
func DecodePreview(raw []byte) (contracts.PreviewFrame, error) {
	var f contracts.PreviewFrame
	e := decodeOption(raw, &f)
	if e != nil {
		return f, e
	}
	_, e = EncodePreview(f)
	return f, e
}
