package clickhouse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestClickHouseDDLWriteReplayAndDayMeasurement(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 to run ClickHouse integration and day measurement")
	}
	database := fmt.Sprintf("crypto_market_info_it_%d", time.Now().UnixNano())
	client, err := Open(context.Background(), Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Database: database, Username: "default", WriteTimeout: 15 * time.Second, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		admin, openErr := ch.Open(&ch.Options{Addr: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Auth: ch.Auth{Database: "default", Username: "default"}})
		if openErr == nil {
			_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+database+"` SYNC")
			_ = admin.Close()
		}
	}()
	if err = client.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	definitions := []model.Instrument{spotDefinition("ACTIVE"), spotDefinition("INACTIVE"), perpDefinition("PERP")}
	instruments, err := client.RegisterInstruments(context.Background(), definitions)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	fundingTime := day.Add(123 * time.Millisecond)
	estimate := model.FundingRate{InstrumentID: instruments[2].ID, HourTime: day, FundingTime: fundingTime, Rate: decimal.RequireFromString("0.0001")}
	actual := model.FundingRate{InstrumentID: instruments[2].ID, HourTime: day, FundingTime: fundingTime, Rate: decimal.RequireFromString("0.0002"), IsActual: true}
	lateEstimate := estimate
	lateEstimate.FundingTime = day.Add(8*time.Hour + 456*time.Millisecond)
	lateEstimate.Rate = decimal.RequireFromString("0.0003")
	for _, rate := range []model.FundingRate{estimate, actual, lateEstimate} {
		if err = client.UpsertFundingRate(context.Background(), rate); err != nil {
			t.Fatal(err)
		}
	}
	var storedRate decimal.Decimal
	var isActual bool
	var storedFundingTime time.Time
	if err = client.conn.QueryRow(context.Background(), `SELECT rate,is_actual,funding_time FROM `+client.table("funding_rate_hourly")+` FINAL WHERE instrument_id=? AND hour_time=?`, instruments[2].ID, day).Scan(&storedRate, &isActual, &storedFundingTime); err != nil {
		t.Fatal(err)
	}
	if !isActual || !storedRate.Equal(actual.Rate) || storedFundingTime.UnixMilli() != fundingTime.UnixMilli() {
		t.Fatalf("late estimate replaced actual funding or milliseconds were lost: rate=%s actual=%v funding_time=%s", storedRate, isActual, storedFundingTime)
	}
	pendingTime := day.Add(16*time.Hour + 789*time.Millisecond)
	for _, rate := range []model.FundingRate{
		{InstrumentID: instruments[2].ID, HourTime: day.Add(8 * time.Hour), FundingTime: pendingTime, Rate: decimal.RequireFromString("0.0004")},
		{InstrumentID: instruments[2].ID, HourTime: day.Add(9 * time.Hour), FundingTime: pendingTime, Rate: decimal.RequireFromString("0.0005")},
	} {
		if err = client.UpsertFundingRate(context.Background(), rate); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := client.LoadPendingFundingConfirmations(context.Background(), day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].InstrumentID != instruments[2].ID || pending[0].FundingTime.UnixMilli() != pendingTime.UnixMilli() {
		t.Fatalf("pending funding confirmations=%+v", pending)
	}
	for minute := 0; minute < 24*60; minute++ {
		at := day.Add(time.Duration(minute) * time.Minute)
		if err = client.WriteMinute(context.Background(), measuredBatch(instruments[0].ID, at, true)); err != nil {
			t.Fatalf("active minute %d: %v", minute, err)
		}
		if err = client.WriteMinute(context.Background(), measuredBatch(instruments[1].ID, at, false)); err != nil {
			t.Fatalf("inactive minute %d: %v", minute, err)
		}
	}
	for _, table := range []string{"order_book_second_delta", "order_book_minute"} {
		if err = client.conn.Exec(context.Background(), "OPTIMIZE TABLE "+client.table(table)+" FINAL"); err != nil {
			t.Fatal(err)
		}
	}
	var compressed uint64
	if err = client.conn.QueryRow(context.Background(), `SELECT sum(data_compressed_bytes) FROM system.parts WHERE active AND database=? AND table IN ('order_book_minute','order_book_second_delta')`, database).Scan(&compressed); err != nil {
		t.Fatal(err)
	}
	if compressed == 0 {
		t.Fatal("ClickHouse reported zero compressed bytes")
	}
	queryAt := day.Add(12*time.Hour + 34*time.Minute + 59*time.Second)
	started := time.Now()
	for iteration := 0; iteration < 100; iteration++ {
		snapshot, valid, replayErr := client.ReplayBook(context.Background(), instruments[0].ID, queryAt)
		if replayErr != nil || !valid || snapshot.Bids[0].QtyLot != 60 {
			t.Fatalf("replay=%+v valid=%v err=%v", snapshot, valid, replayErr)
		}
	}
	elapsed := time.Since(started)
	if elapsed > 10*time.Second {
		t.Fatalf("100 one-minute replays took %s", elapsed)
	}
	t.Logf("representative one-day compressed bytes=%d; 100 minute replays=%s", compressed, elapsed)
}

func measuredBatch(instrumentID uint32, minute time.Time, active bool) model.MinuteBatch {
	id, _ := model.MinuteID(instrumentID, minute)
	book := model.MinuteBook{ID: id, InstrumentID: instrumentID, MinuteTime: minute, ValidBitmap: (uint64(1) << 60) - 1}
	book.Bids[0] = model.Level{PriceTick: 100, QtyLot: 1}
	book.Bids[1] = model.Level{PriceTick: 99, QtyLot: 2}
	book.Asks[0] = model.Level{PriceTick: 101, QtyLot: 3}
	book.Asks[1] = model.Level{PriceTick: 102, QtyLot: 4}
	batch := model.MinuteBatch{Minute: book}
	if active {
		for second := uint8(1); second < 60; second++ {
			batch.Deltas = append(batch.Deltas, model.BookDelta{MinuteID: id, SecondOffset: second, BidChangePrice: []int64{100}, BidChangeQty: []uint64{uint64(second) + 1}})
		}
	}
	return batch
}

func spotDefinition(symbol string) model.Instrument {
	return model.Instrument{Exchange: "Test", MarketType: model.MarketSpot, ExchangeSymbol: symbol, BaseAsset: symbol, QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.001")}
}
func perpDefinition(symbol string) model.Instrument {
	settle := "USDT"
	return model.Instrument{Exchange: "Test", MarketType: model.MarketPerpetual, ExchangeSymbol: symbol, BaseAsset: symbol, QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.001")}
}
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
