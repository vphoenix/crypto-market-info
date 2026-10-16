package mysql

import (
	"bytes"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

func TestTradeBarFinalFlagUsesUnsignedStorageWithoutChangingHash(t *testing.T) {
	typeOf, err := codec.Parse("UInt8")
	if err != nil {
		t.Fatal(err)
	}
	for _, final := range []bool{false, true} {
		bar := model.TradeBar{ID: "bar", InstrumentID: 1, Final: final, BaseVolume: decimal.RequireFromString("15.123456789012345678")}
		body := bar.Body()
		want := uint8(0)
		if final {
			want = 1
		}
		encoded, err := codec.Encode(typeOf, body["is_final"])
		expected, expectedErr := codec.Encode(typeOf, want)
		if err != nil || expectedErr != nil || !bytes.Equal(encoded, expected) {
			t.Fatal("bar rejected by UInt8 storage", final, encoded, err)
		}
		if value, err := codec.SQLValue(typeOf, body["is_final"]); err != nil || value != uint64(want) {
			t.Fatal("bar rejected by native SQL type", value, err)
		}
		before := model.DiscoveryHash(body)
		body["is_final"] = int(want) // Previous canonical JSON used a Go int.
		if model.DiscoveryHash(body) != before {
			t.Fatal("public identity/content hash changed")
		}
	}
}
