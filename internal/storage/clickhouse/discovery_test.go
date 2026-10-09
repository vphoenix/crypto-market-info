package clickhouse

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"sync"
	"testing"
	"time"
)

func discoveryFixture(t *testing.T, c *Client) model.FundingPrediction {
	t.Helper()
	i := perpDefinition("TEST")
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
func TestDiscoveryDurablePublicationAndAmbiguousAcknowledgement(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	f := discoveryFixture(t, c)
	failed := false
	c.derivativeAfterInsert = func(table string) error {
		if table == f.Kind() && !failed {
			failed = true
			return fmt.Errorf("lost acknowledgement after durable fact")
		}
		return nil
	}
	if err := c.WriteDiscoveryFact(ctx, f); err != nil {
		t.Fatal(err)
	}
	if !failed {
		t.Fatal("ambiguous acknowledgement fault not exercised")
	}
	c.derivativeAfterInsert = nil
	asOf := time.Now().UTC().Add(time.Millisecond)
	got, proof, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if model.DiscoveryHash(got.Body()) != model.DiscoveryHash(f.Body()) || !got.Rate.IsNegative() {
		t.Fatal("typed observation changed")
	}
	if proof.PublishedMS < proof.KnownMS || proof.KnownMS < f.ObservedMS {
		t.Fatal("publication preceded durability")
	}
	if err = c.WriteDiscoveryFact(ctx, f); err != nil {
		t.Fatal(err)
	}
	_, again, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, asOf)
	if err != nil || again.PublishedMS != proof.PublishedMS {
		t.Fatal("retry renewed first publication", err)
	}
	if _, _, err = c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.UnixMilli(proof.PublishedMS-1)); err == nil {
		t.Fatal("future publication visible")
	}
	changed := f
	changed.Rate = decimal.Zero
	if err = c.WriteDiscoveryFact(ctx, changed); err == nil {
		t.Fatal("observation identity overwritten")
	}
	if _, _, err = c.LoadDiscoveryForecast(ctx, f.InstrumentID, asOf.Add(91*time.Second)); err == nil {
		t.Fatal("stale source accepted")
	}
}
func TestDiscoveryUnpublishedFactNeverVisibleAndConcurrentRetryUnique(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	f := discoveryFixture(t, c)
	body := f.Body()
	body["row_hash"] = model.DiscoveryHash(f.Body())
	if err := c.insertDiscoveryBody(ctx, f.Kind(), body); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now()); err == nil {
		t.Fatal("partial publication appeared complete")
	}
	var wait sync.WaitGroup
	errs := make(chan error, 8)
	for n := 0; n < 8; n++ {
		wait.Add(1)
		go func() { defer wait.Done(); errs <- c.WriteDiscoveryFact(ctx, f) }()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now().Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
}
