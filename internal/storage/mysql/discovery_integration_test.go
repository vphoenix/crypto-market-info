package mysql

import (
	"context"
	"database/sql"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"sync"
	"testing"
	"time"
)

func discoveryFixture(t *testing.T, c *Client) model.FundingPrediction {
	t.Helper()
	settle := "USDT"
	i := model.Instrument{BaseAsset: "TEST", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), MarketType: model.MarketPerpetual, VenueContractVersion: "test-v1", PriceTickSize: decimal.RequireFromString(".001"), QuantityStepSize: decimal.RequireFromString(".001")}
	i.Exchange = "OKX"
	i.ExchangeSymbol = "TEST-USDT-SWAP"
	registered, err := c.RegisterInstruments(context.Background(), []model.Instrument{i})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Second)
	sources := []model.PublicSource{}
	for _, endpoint := range []string{"/public/funding-rate?instId=TEST-USDT-SWAP", "/public/mark-price?instId=TEST-USDT-SWAP&instType=SWAP", "/market/index-tickers?instId=TEST-USD", "/market/index-tickers?instId=USDT-USD"} {
		sources = append(sources, model.PublicSource{URL: "https://www.okx.com/api/v5" + endpoint, PayloadHash: model.ReferenceHash(endpoint), TimeBasis: "exchange_ts", SourceMS: now.UnixMilli(), ObservedMS: now.UnixMilli()})
	}
	return model.FundingPrediction{ID: "test-independent-forecast", InstrumentID: registered[0].ID, ObservedMS: now.UnixMilli(), SourceMS: now.UnixMilli(), FundingMS: now.Add(time.Hour).UnixMilli(), Rate: decimal.RequireFromString("-.00000001"), Mark: decimal.RequireFromString("2.001"), MarkMS: now.UnixMilli(), BaseIndex: decimal.RequireFromString("2.000"), BaseIndexMS: now.UnixMilli(), BaseUSDIndex: decimal.RequireFromString("2.000"), QuoteUSDIndex: decimal.NewFromInt(1), QuoteIndexMS: now.UnixMilli(), Sources: sources}
}

func TestMySQLDiscoveryFirstKnowledgeAndAtomicPublication(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if e := c.InitOKXPairSchema(ctx); e != nil {
		t.Fatal(e)
	}
	f := discoveryFixture(t, c)
	p := publicationRecord{Kind: f.Kind(), ID: f.ID, Hash: model.DiscoveryHash(f.Body()), Codec: model.DiscoveryCodec, SourceMS: f.SourceMS, ObservedMS: f.ObservedMS, KnownMS: model.CeilMilliseconds(time.Now().UTC()), InstrumentID: f.InstrumentID, SourceCount: uint16(len(f.Sources)), ProvenanceHash: model.DiscoverySourcesHash(f.Sources)}
	known, e := c.beginDiscoveryIntent(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	body := f.Body()
	body["row_hash"] = p.Hash
	if e = c.transaction(ctx, func(tx *sql.Tx) error {
		tab, e := c.mapping(f.Kind())
		if e != nil {
			return e
		}
		return putImmutable(ctx, tx, c.table(tab.Name), tab, record(body))
	}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now()); e == nil {
		t.Fatal("orphan fact visible")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := c.WriteDiscoveryFact(ctx, f); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	at := time.Now().UTC().Add(time.Millisecond)
	got, proof, e := c.LoadDiscoveryForecast(ctx, f.InstrumentID, at)
	if e != nil {
		t.Fatal(e)
	}
	if proof.KnownMS != known || model.DiscoveryHash(got.Body()) != p.Hash {
		t.Fatal("first knowledge or content changed")
	}
	if _, _, e = c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.UnixMilli(proof.PublishedMS-1)); e == nil {
		t.Fatal("future publication visible")
	}
	if e = c.WriteDiscoveryFact(ctx, f); e != nil {
		t.Fatal(e)
	}
	_, again, e := c.LoadDiscoveryForecast(ctx, f.InstrumentID, at)
	if e != nil || again.PublishedMS != proof.PublishedMS {
		t.Fatal("retry changes publication", e)
	}
	f.Rate = decimal.Zero
	if e = c.WriteDiscoveryFact(ctx, f); e == nil {
		t.Fatal("identity content changed")
	}
	if _, _, e = c.LoadDiscoveryForecast(ctx, f.InstrumentID, at.Add(91*time.Second)); e == nil {
		t.Fatal("stale fact visible")
	}
}
