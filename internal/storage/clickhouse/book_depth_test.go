package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestBookDepthBatchValidation(t *testing.T) {
	at := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	for _, depth := range []uint8{10, 50} {
		batch := measuredBatch(1, at, false)
		batch.Minute.StoredDepth = depth
		if err := validateBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	for _, depth := range []uint8{0, 11, 51} {
		batch := measuredBatch(1, at, false)
		batch.Minute.StoredDepth = depth
		if validateBatch(batch) == nil {
			t.Fatalf("accepted unsupported depth %d", depth)
		}
	}
	batch := measuredBatch(1, at, false)
	batch.Minute.Bids[10] = model.Level{PriceTick: 90, QtyLot: 1}
	if validateBatch(batch) == nil {
		t.Fatal("accepted nonzero level 11 in new anchor")
	}
	batch = measuredBatch(1, at, false)
	for level := range 10 {
		batch.Minute.Bids[level] = model.Level{PriceTick: int64(100 - level), QtyLot: 1}
	}
	batch.Deltas = []model.BookDelta{{MinuteID: batch.Minute.ID, SecondOffset: 1, BidChangePrice: []int64{90}, BidChangeQty: []uint64{1}}}
	if validateBatch(batch) == nil {
		t.Fatal("accepted delta growing saved book beyond ten levels")
	}
}

func TestClickHouseBookDepthMigrationAndMixedHistory(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	// Remove only the new column in this isolated test database to reproduce
	// the real legacy schema, then insert with the old collector's column list.
	if err := c.conn.Exec(ctx, "ALTER TABLE "+c.table("order_book_minute")+" DROP COLUMN stored_depth"); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	legacy := measuredBatch(1, at, false)
	legacy.Minute.StoredDepth = 50
	for level := range 50 {
		legacy.Minute.Bids[level] = model.Level{PriceTick: int64(1000 - level), QtyLot: uint64(level + 1)}
		legacy.Minute.Asks[level] = model.Level{PriceTick: int64(1001 + level), QtyLot: uint64(level + 2)}
	}
	legacy.Deltas = []model.BookDelta{{MinuteID: legacy.Minute.ID, SecondOffset: 1,
		BidChangePrice: []int64{1000, 950}, BidChangeQty: []uint64{0, 51},
		AskChangePrice: []int64{1001, 1051}, AskChangeQty: []uint64{0, 52}}}
	insertOld := func(minute model.MinuteBook) {
		t.Helper()
		columns := append([]string(nil), MinuteColumns()...)
		columns = append(columns[:4], columns[5:]...)
		values := []any{minute.ID, minute.InstrumentID, minute.MinuteTime, minute.ValidBitmap}
		for _, side := range [][model.LegacyBookDepth]model.Level{minute.Bids, minute.Asks} {
			for _, level := range side {
				values = append(values, level.PriceTick, level.QtyLot)
			}
		}
		b, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.table("order_book_minute")+" ("+strings.Join(columns, ",")+")")
		if err != nil {
			t.Fatal(err)
		}
		defer b.Abort()
		if err = b.Append(values...); err != nil {
			t.Fatal(err)
		}
		if err = b.Send(); err != nil {
			t.Fatal(err)
		}
	}
	insertOld(legacy.Minute)
	if err := c.insertDeltas(ctx, legacy.Deltas); err != nil {
		t.Fatal(err)
	}
	assertLegacy := func() {
		t.Helper()
		got, valid, err := c.ReplayBook(ctx, 1, at.Add(time.Second))
		if err != nil || !valid || got.StoredDepth != 50 || len(got.Bids) != 50 || len(got.Asks) != 50 || got.Bids[9] != legacy.Minute.Bids[10] || got.Asks[9] != legacy.Minute.Asks[10] {
			t.Fatalf("legacy replay changed: %+v valid=%v err=%v", got, valid, err)
		}
	}
	assertLegacy() // No schema mutation is required by read-only replay.
	for range 2 {
		if err := c.InitSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	assertLegacy()
	// A rolled-back old collector must still tag its new rows as depth 50.
	oldLater := legacy.Minute
	oldLater.MinuteTime = at.Add(2 * time.Minute)
	oldLater.ID, _ = model.MinuteID(1, oldLater.MinuteTime)
	insertOld(oldLater)
	loaded, err := c.LoadMinute(ctx, 1, oldLater.MinuteTime)
	if err != nil || loaded.StoredDepth != 50 {
		t.Fatalf("legacy writer default changed: depth=%d err=%v", loaded.StoredDepth, err)
	}
	current := measuredBatch(1, at.Add(time.Minute), true)
	for range 2 {
		if err := c.WriteMinute(ctx, current); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err = c.LoadMinute(ctx, 1, current.Minute.MinuteTime)
	if err != nil || !reflect.DeepEqual(loaded, current.Minute) {
		t.Fatalf("ten-level minute changed during write/retry: %+v err=%v", loaded, err)
	}
	got, valid, err := c.ReplayBook(ctx, 1, current.Minute.MinuteTime.Add(59*time.Second))
	if err != nil || !valid || got.StoredDepth != 10 || got.Bids[0].QtyLot != 60 {
		t.Fatalf("ten-level replay failed: %+v %v", got, err)
	}
	assertLegacy()
	var count uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("order_book_minute")+" FINAL").Scan(&count); err != nil || count != 3 {
		t.Fatalf("retry changed logical minute identity: count=%d err=%v", count, err)
	}
}
