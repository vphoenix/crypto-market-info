package runtimecheckpoint

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestTypedCodecPreservesBinaryNullZeroAndExactValues(t *testing.T) {
	type row struct {
		Identity string
		Known    *bool
		Height   *uint64
		Amount   *big.Int
		Price    decimal.Decimal
		At       *time.Time
		Missing  *uint8
		Nested   map[uint64]*bool
	}
	no := false
	zero := uint64(0)
	at := time.Date(2026, 10, 15, 0, 0, 0, 123456000, time.UTC)
	in := map[string]row{"\xff\x00": {Identity: "\x00\xff", Known: &no, Height: &zero, Amount: new(big.Int).Lsh(big.NewInt(1), 255), Price: decimal.RequireFromString("12345678901234567890.123456789012345678"), At: &at, Nested: map[uint64]*bool{0: &no, 1: nil}}}
	raw, err := MarshalTyped(in)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]row
	if err = UnmarshalTyped(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("changed exact typed state: %#v", got)
	}
	old := row{Known: &no}
	raw, err = MarshalTyped(row{})
	if err != nil {
		t.Fatal(err)
	}
	if err = UnmarshalTyped(raw, &old); err != nil || old.Known != nil {
		t.Fatal("retained stale destination pointer", old, err)
	}
}

func TestTypedCodecRejectsUnexpectedPresence(t *testing.T) {
	raw, err := MarshalTyped(struct{ Value uint64 }{1})
	if err != nil {
		t.Fatal(err)
	}
	var env typedEnvelope
	if err = decode(raw, &env); err != nil {
		t.Fatal(err)
	}
	env.Present.Children = map[string]presence{"99": {Pointer: true}}
	raw, err = encode(env)
	if err != nil {
		t.Fatal(err)
	}
	var got struct{ Value uint64 }
	if err = UnmarshalTyped(raw, &got); err == nil {
		t.Fatal("accepted wrong pointer field")
	}
}
