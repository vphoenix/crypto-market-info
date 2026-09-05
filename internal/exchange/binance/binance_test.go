package binance

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestParseExchangeInfoStrict(t *testing.T) {
	payload := []byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","onboardDate":1585526400000,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}`)
	got, err := ParseExchangeInfo(payload, model.MarketPerpetual)
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if !got[0].PriceTickSize.Equal(decimal.RequireFromString("0.10")) || got[0].VenueContractVersion != "1585526400000" {
		t.Fatal("tick size was not parsed exactly")
	}
	bad := []byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","onboardDate":1585526400000,"filters":[{"filterType":"PRICE_FILTER","tickSize":"bad"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}`)
	if _, err := ParseExchangeInfo(bad, model.MarketPerpetual); err == nil {
		t.Fatal("invalid decimal was silently accepted")
	}
	missingVersion := []byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","filters":[]}]}`)
	if _, err := ParseExchangeInfo(missingVersion, model.MarketPerpetual); err == nil {
		t.Fatal("missing onboardDate was accepted")
	}
}

func TestBinanceCollectorGapIsFailClosed(t *testing.T) {
	instrument := testInstrument()
	book, _ := orderbook.New(1, 1000)
	collector, _ := NewCollector(book, true)
	now := time.Unix(100, 0).UTC()
	if err := collector.ApplySnapshot(model.BookSnapshot{InstrumentID: 1, SourceTime: now, Sequence: 100, Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Push(DepthUpdate{FirstUpdateID: 100, FinalUpdateID: 101, PreviousUpdateID: 99, SourceTime: now.Add(time.Second), Bids: []model.Level{{PriceTick: 100, QtyLot: 2}}}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Push(DepthUpdate{FirstUpdateID: 103, FinalUpdateID: 103, PreviousUpdateID: 102, SourceTime: now.Add(2 * time.Second)}); err == nil {
		t.Fatal("gap was accepted")
	}
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("gap left book sampleable")
	}
	_ = instrument
}

func TestParseDepthRejectsNonDivisible(t *testing.T) {
	instrument := testInstrument()
	_, err := ParseDepthSnapshot([]byte(`{"lastUpdateId":1,"bids":[["100.01","1"]],"asks":[["101","1"]]}`), instrument, time.Now())
	if err == nil {
		t.Fatal("non-divisible price was accepted")
	}
}

func testInstrument() model.Instrument {
	settle := "USDT"
	return model.Instrument{ID: 1, Exchange: "Binance", MarketType: model.MarketPerpetual, ExchangeSymbol: "BTCUSDT", VenueContractVersion: "1585526400000", BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.001")}
}
