package bybit

import (
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func bybitTestInstrument() model.Instrument {
	settle := "USDT"
	return model.Instrument{ID: 3, Exchange: "Bybit", MarketType: model.MarketPerpetual, ExchangeSymbol: "BTCUSDT", VenueContractVersion: "5:1585526400000", BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.001")}
}

func depthPayload(kind string, updateID, sequence, ts, cts int64, bids, asks string) []byte {
	return []byte(fmt.Sprintf(`{"topic":"orderbook.1000.BTCUSDT","type":%q,"ts":%d,"cts":%d,"data":{"s":"BTCUSDT","b":%s,"a":%s,"u":%d,"seq":%d}}`, kind, ts, cts, bids, asks, updateID, sequence))
}

func TestDepthUsesTopLevelCTSThenAppliesAbsoluteDelta(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	snapshot, err := ParseDepth(depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"],["99","2"]]`, `[["101","3"],["102","4"]]`), instrument)
	if err != nil || snapshot.SourceTime.UnixMilli() != 900 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if err = collector.Push(snapshot); err != nil {
		t.Fatal(err)
	}
	delta, err := ParseDepth(depthPayload("delta", 11, 25, 1100, 1000, `[["100","0"],["98","5"]]`, `[["101","6"]]`), instrument)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.Push(delta); err != nil {
		t.Fatal(err)
	}
	got, valid := book.Snapshot(50)
	if !valid || got.Sequence != 11 || got.Bids[0].PriceTick != 990 || got.Bids[1].PriceTick != 980 || got.Asks[0].QtyLot != 6000 {
		t.Fatalf("book=%+v valid=%v", got, valid)
	}
}

func TestCollectorFailsClosedOnGapAndRecoversOnlyWithSnapshot(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	snapshot, _ := ParseDepth(depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`), instrument)
	if err := collector.Push(snapshot); err != nil {
		t.Fatal(err)
	}
	gap, _ := ParseDepth(depthPayload("delta", 12, 22, 1100, 1000, `[]`, `[]`), instrument)
	if err := collector.Push(gap); err == nil {
		t.Fatal("u gap was accepted")
	}
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("gapped book remained valid")
	}
	followup, _ := ParseDepth(depthPayload("delta", 13, 23, 1200, 1100, `[]`, `[]`), instrument)
	if err := collector.Push(followup); err == nil {
		t.Fatal("delta restored book without snapshot")
	}
	reset, _ := ParseDepth(depthPayload("snapshot", 1, 1, 1300, 1200, `[["99","2"]]`, `[["102","3"]]`), instrument)
	if err := collector.Push(reset); err != nil {
		t.Fatal(err)
	}
	if _, valid := book.Snapshot(50); !valid {
		t.Fatal("new snapshot did not restore book")
	}
}

func TestCollectorRejectsDeltaBeforeSnapshotAndCrossSequenceRollback(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	delta, _ := ParseDepth(depthPayload("delta", 2, 2, 1000, 900, `[]`, `[]`), instrument)
	if err := collector.Push(delta); err == nil {
		t.Fatal("delta before snapshot was accepted")
	}
	snapshot, _ := ParseDepth(depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`), instrument)
	_ = collector.Push(snapshot)
	rollback, _ := ParseDepth(depthPayload("delta", 11, 19, 1100, 1000, `[]`, `[]`), instrument)
	if err := collector.Push(rollback); err == nil {
		t.Fatal("seq rollback was accepted")
	}
}

func TestParseDepthRejectsMalformedRowsAndInvalidReset(t *testing.T) {
	instrument := bybitTestInstrument()
	for name, payload := range map[string][]byte{
		"non-divisible": depthPayload("snapshot", 10, 20, 1000, 900, `[["100.01","1"]]`, `[["101","1"]]`),
		"zero snapshot": depthPayload("snapshot", 10, 20, 1000, 900, `[["100","0"]]`, `[["101","1"]]`),
		"duplicate":     depthPayload("delta", 11, 21, 1000, 900, `[["100","1"],["100","2"]]`, `[]`),
		"u1 delta":      depthPayload("delta", 1, 21, 1000, 900, `[]`, `[]`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseDepth(payload, instrument); err == nil {
				t.Fatal("malformed depth was accepted")
			}
		})
	}
}

func TestCollectorRetainsLevel51WhenTopPriceIsDeleted(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	bids := make([]model.Level, 51)
	asks := make([]model.Level, 51)
	for index := range bids {
		bids[index] = model.Level{PriceTick: int64(1000 - index), QtyLot: 1}
		asks[index] = model.Level{PriceTick: int64(1001 + index), QtyLot: 1}
	}
	if err := collector.Push(DepthUpdate{Snapshot: true, UpdateID: 10, CrossSequence: 20, SourceTime: time.UnixMilli(1000).UTC(), Bids: bids, Asks: asks}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Push(DepthUpdate{UpdateID: 11, CrossSequence: 21, SourceTime: time.UnixMilli(1100).UTC(), Bids: []model.Level{{PriceTick: 1000, QtyLot: 0}}, Asks: []model.Level{{PriceTick: 1001, QtyLot: 0}}}); err != nil {
		t.Fatal(err)
	}
	snapshot, valid := book.Snapshot(50)
	if !valid || len(snapshot.Bids) != 50 || len(snapshot.Asks) != 50 || snapshot.Bids[49].PriceTick != 950 || snapshot.Asks[49].PriceTick != 1051 {
		t.Fatalf("level 51 did not enter top 50: valid=%v bids=%+v asks=%+v", valid, snapshot.Bids, snapshot.Asks)
	}
}

func TestCollectorRejectsDuplicateBackwardAndJumpedUpdateIDs(t *testing.T) {
	for name, updateID := range map[string]int64{"duplicate": 10, "backward": 9, "jump": 12} {
		t.Run(name, func(t *testing.T) {
			instrument := bybitTestInstrument()
			book, _ := orderbook.New(instrument.ID, orderBookDepth)
			collector, _ := NewCollector(book)
			if err := collector.Push(DepthUpdate{Snapshot: true, UpdateID: 10, CrossSequence: 20, SourceTime: time.UnixMilli(1000).UTC(), Bids: []model.Level{{PriceTick: 1000, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 1010, QtyLot: 1}}}); err != nil {
				t.Fatal(err)
			}
			err := collector.Push(DepthUpdate{UpdateID: updateID, CrossSequence: 21, SourceTime: time.UnixMilli(1100).UTC()})
			if err == nil || book.View().State != orderbook.StateInvalid {
				t.Fatalf("update id %d error=%v state=%s", updateID, err, book.View().State)
			}
		})
	}
}

func TestCollectorAllowsCrossSequenceJumpAndAnySnapshotOverwrite(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	if err := collector.Push(DepthUpdate{Snapshot: true, UpdateID: 10, CrossSequence: 20, SourceTime: time.UnixMilli(1000).UTC(), Bids: []model.Level{{PriceTick: 1000, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 1010, QtyLot: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Push(DepthUpdate{UpdateID: 11, CrossSequence: 200, SourceTime: time.UnixMilli(1100).UTC()}); err != nil {
		t.Fatalf("cross-sequence jump was rejected: %v", err)
	}
	if err := collector.Push(DepthUpdate{Snapshot: true, UpdateID: 99, CrossSequence: 300, SourceTime: time.UnixMilli(1200).UTC(), Bids: []model.Level{{PriceTick: 900, QtyLot: 2}}, Asks: []model.Level{{PriceTick: 1100, QtyLot: 3}}}); err != nil {
		t.Fatalf("arbitrary snapshot overwrite failed: %v", err)
	}
	snapshot, valid := book.Snapshot(50)
	if !valid || snapshot.Sequence != 99 || snapshot.Bids[0].PriceTick != 900 || snapshot.Asks[0].PriceTick != 1100 {
		t.Fatalf("overwritten snapshot=%+v valid=%v", snapshot, valid)
	}
	if err := collector.Push(DepthUpdate{Snapshot: true, UpdateID: 1, CrossSequence: 1, SourceTime: time.UnixMilli(1300).UTC(), Bids: []model.Level{{PriceTick: 800, QtyLot: 4}}, Asks: []model.Level{{PriceTick: 1200, QtyLot: 5}}}); err != nil {
		t.Fatalf("u=1 snapshot reset failed: %v", err)
	}
	if snapshot, valid = book.Snapshot(50); !valid || snapshot.Sequence != 1 || snapshot.Bids[0].PriceTick != 800 {
		t.Fatalf("u=1 snapshot=%+v valid=%v", snapshot, valid)
	}
}
