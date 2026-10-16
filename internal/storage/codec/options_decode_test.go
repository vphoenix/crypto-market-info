package codec

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func TestDirectOptionsDecoderMatchesExistingWireAndBounds(t *testing.T) {
	type nested struct {
		ID      uuid.UUID
		Value   *uint64
		At      time.Time
		Times   [3]*time.Time
		Numbers []int64
		Amount  decimal.Decimal
		Text    string
	}
	type envelope struct {
		Members []nested
		Fixed   [2]uint8
	}
	n := uint64(1<<63 + 3)
	at := time.Date(2026, 10, 14, 11, 2, 3, 456789000, time.UTC)
	source := envelope{Members: []nested{{uuid.New(), &n, at, [3]*time.Time{nil, &at, nil}, []int64{-3, 0, 8}, decimal.RequireFromString("123456789.000000001"), "\x00public"}, {ID: uuid.New(), Amount: decimal.Zero}}, Fixed: [2]uint8{0, 255}}
	raw, e := encodeOption(source)
	if e != nil {
		t.Fatal(e)
	}
	var direct, previous envelope
	if e = decodeOption(raw, &direct); e != nil {
		t.Fatal(e)
	}
	typ, _ := optionSchema(reflect.TypeOf(source))
	generic, e := decode(typ, raw, 1<<21)
	if e != nil {
		t.Fatal(e)
	}
	if e = assignOption(reflect.ValueOf(&previous).Elem(), generic); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(direct, previous) {
		t.Fatal("decoder changed a typed value")
	}
	for _, bad := range [][]byte{raw[:len(raw)-1], append(append([]byte(nil), raw...), 0), append([]byte{2}, raw[1:]...)} {
		var out envelope
		if e = decodeOption(bad, &out); e == nil {
			t.Fatal("malformed envelope accepted")
		}
	}
}

func TestWideOptionsArrayRejectedBeforeAllocation(t *testing.T) {
	// encodeOption intentionally bypasses domain Validate for this corrupt
	// fixture. The decoder must reject the count before allocating Books.
	raw, err := encodeOption(options.LiveEnvelope{Books: make([]model.DerivativeMinuteBatch, 33)})
	if err != nil {
		t.Fatal(err)
	}
	var decoded options.LiveEnvelope
	if err := decodeOption(raw, &decoded); err == nil {
		t.Fatal("oversized live array accepted")
	}
	if decoded.Books != nil {
		t.Fatal("wide invalid array allocated before validation")
	}
	minimum := minimumOptionSize(reflect.TypeOf(model.DerivativeMinuteBatch{}))
	if minimum < 60*minimumOptionSize(reflect.TypeOf(model.DerivativeQuality{})) {
		t.Fatal("fixed 60-second array omitted from minimum wire length")
	}
}
