package replay

import (
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func legacyAnchor(t *testing.T) model.MinuteBook {
	t.Helper()
	at := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	id, err := model.MinuteID(1, at)
	if err != nil {
		t.Fatal(err)
	}
	minute := model.MinuteBook{ID: id, InstrumentID: 1, MinuteTime: at, ValidBitmap: 3, StoredDepth: 50}
	for level := range 50 {
		minute.Bids[level] = model.Level{PriceTick: int64(1000 - level), QtyLot: uint64(level + 1)}
		minute.Asks[level] = model.Level{PriceTick: int64(1001 + level), QtyLot: uint64(level + 2)}
	}
	return minute
}

func TestLegacyReplayPreservesUntouchedLevelEleven(t *testing.T) {
	minute := legacyAnchor(t)
	deltas := []model.BookDelta{{MinuteID: minute.ID, SecondOffset: 1,
		BidChangePrice: []int64{1000, 950}, BidChangeQty: []uint64{0, 51},
		AskChangePrice: []int64{1001, 1051}, AskChangeQty: []uint64{0, 52}}}
	got, valid, err := AtSecond(minute, deltas, 1)
	if err != nil || !valid || got.StoredDepth != 50 || len(got.Bids) != 50 || len(got.Asks) != 50 {
		t.Fatalf("legacy depth lost: %+v valid=%v err=%v", got, valid, err)
	}
	if got.Bids[9] != minute.Bids[10] || got.Asks[9] != minute.Asks[10] {
		t.Fatal("untouched original level 11 was lost when it entered top ten")
	}
}

func TestReplayRejectsUnknownDepthAndHiddenLevels(t *testing.T) {
	for _, depth := range []uint8{0, 9, 11, 51} {
		minute := legacyAnchor(t)
		minute.StoredDepth = depth
		if _, _, err := AtSecond(minute, nil, 0); err == nil {
			t.Fatalf("accepted unsupported stored depth %d", depth)
		}
	}
	minute := legacyAnchor(t)
	minute.StoredDepth = 10
	if _, _, err := AtSecond(minute, nil, 0); err == nil {
		t.Fatal("accepted hidden levels beyond recorded depth")
	}
	for level := 10; level < 50; level++ {
		minute.Bids[level], minute.Asks[level] = model.Level{}, model.Level{}
	}
	deltas := []model.BookDelta{{MinuteID: minute.ID, SecondOffset: 1, BidChangePrice: []int64{990}, BidChangeQty: []uint64{1}}}
	if _, _, err := AtSecond(minute, deltas, 1); err == nil {
		t.Fatal("accepted a delta expanding a ten-level book to eleven levels")
	}
}
