package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

func TestColdPairedStartKeepsFullUniverseAndRejectsInvalidBooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	books := map[uint32]*orderbook.Book{}
	defs := []model.Instrument{}
	g := &pairedGeneration{ctx: ctx}
	for n, name := range []string{"BTC", "ETH"} {
		id := uint32(n + 1)
		b, e := orderbook.New(id, 400)
		if e != nil {
			t.Fatal(e)
		}
		books[id] = b
		defs = append(defs, model.Instrument{ID: id, Exchange: "OKX", MarketType: model.MarketSpot, ExchangeSymbol: name + "-USDT", BaseAsset: name, QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.New(1, -1), QuantityStepSize: decimal.New(1, -4)})
		g.sources = append(g.sources, sampler.Source{InstrumentID: id, Book: b, StoredDepth: 5})
	}
	m, e := okx.NewBookManager(okx.NewClient(), defs, books, 20, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.manager = m
	if allowInitialPairedSampling(false, errPairedWarmTimeout, g) {
		t.Fatal("no valid snapshot accepted")
	}
	at := time.Now().UTC().Truncate(time.Minute)
	if e = books[1].ApplySnapshot(model.BookSnapshot{InstrumentID: 1, SourceTime: at, Sequence: 10, Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 2}}}); e != nil {
		t.Fatal(e)
	}
	if !allowInitialPairedSampling(false, errPairedWarmTimeout, g) {
		t.Fatal("healthy independent stream discarded at cold start")
	}
	if allowInitialPairedSampling(true, errPairedWarmTimeout, g) {
		t.Fatal("replacement bypassed all-ready barrier")
	}
	if allowInitialPairedSampling(false, errors.New("protocol failure"), g) {
		t.Fatal("fatal protocol failure treated as warm timeout")
	}
	if len(g.sources) != 2 || m.HealthSnapshot().ExpectedInstruments != 2 {
		t.Fatal("failed source removed from scope")
	}
	for _, source := range g.sources {
		buf, e := sampler.NewMinuteBufferWithDepth(source.InstrumentID, at, 5)
		if e != nil {
			t.Fatal(e)
		}
		for sec := 0; sec < 60; sec++ {
			snapshot, valid := source.Book.Snapshot(5)
			if e = buf.Sample(at.Add(time.Duration(sec)*time.Second), snapshot, valid); e != nil {
				t.Fatal(e)
			}
		}
		batch, ok := buf.Batch()
		if source.InstrumentID == 1 {
			if !ok || batch.Minute.ValidBitmap != model.MinuteMask {
				t.Fatal("valid independent stream not preserved")
			}
		} else if ok {
			t.Fatal("missing snapshot became a valid minute")
		}
	}
	books[1].MarkInvalid("sequence gap")
	if _, valid := books[1].Snapshot(5); valid {
		t.Fatal("sequence gap generated valid book")
	}
	if allowInitialPairedSampling(false, errPairedWarmTimeout, g) {
		t.Fatal("invalidated last snapshot accepted")
	}
	cancel()
	if allowInitialPairedSampling(false, errPairedWarmTimeout, g) {
		t.Fatal("canceled generation accepted")
	}
}
