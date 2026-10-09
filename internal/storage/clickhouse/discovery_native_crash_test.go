package clickhouse

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"testing"
	"time"
)

func nativeDiscoveryFixture(t *testing.T, c *Client) (model.OKXLoanPolicy, model.OKXPairObservation) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Second)
	p := model.OKXLoanPolicy{ID: uuid.New(), RequestedAt: at, ObservedAt: at.Add(time.Millisecond), SourceURL: "https://www.okx.com/api/v5/public/interest-rate-loan-quota", PayloadHash: model.ReferenceHash("native-source"), Basic: []model.OKXBasicLoan{{Currency: "BTC", DailyRate: decimal.RequireFromString(".00001"), PublicQuota: decimal.NewFromInt(175)}}}
	spot := model.Instrument{Exchange: "OKX", MarketType: model.MarketSpot, ExchangeSymbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString(".1"), QuantityStepSize: decimal.RequireFromString(".0001")}
	perp := spot
	perp.MarketType = model.MarketPerpetual
	perp.ExchangeSymbol = "BTC-USDT-SWAP"
	perp.VenueContractVersion = "1597026383085"
	usd := "USDT"
	perp.SettleAsset = &usd
	perp.ContractMultiplier = decimal.RequireFromString(".01")
	perp.QuantityStepSize = decimal.NewFromInt(1)
	rows, err := c.RegisterInstruments(context.Background(), []model.Instrument{spot, perp})
	if err != nil {
		t.Fatal(err)
	}
	o := model.OKXPairObservation{ID: uuid.New(), ObservedAt: at.Add(2 * time.Millisecond), EffectiveMinute: at.Truncate(time.Minute).Add(time.Minute), LoanPolicyID: p.ID, Pairs: []model.OKXPairMember{{Base: "BTC", Spot: rows[0], Perpetual: rows[1], SpotMinSize: decimal.RequireFromString(".001"), PerpetualMinSize: decimal.NewFromInt(1)}}}
	for n := 0; n < 4; n++ {
		o.SourceURLs = append(o.SourceURLs, p.SourceURL)
		o.SourceHashes = append(o.SourceHashes, p.PayloadHash)
		o.RequestedAt = append(o.RequestedAt, p.RequestedAt)
		o.ReceivedAt = append(o.ReceivedAt, p.ObservedAt)
		o.RawCounts = append(o.RawCounts, 1)
	}
	return p, o
}

func TestDiscoveryNativeFirstKnowledgePrecedesFactAndSurvivesWholeCallRetry(t *testing.T) {
	for _, kind := range []string{"okx_public_loan_policy", "okx_paired_catalog"} {
		t.Run(kind, func(t *testing.T) {
			c := commonUniverseTestClient(t)
			ctx := context.Background()
			if err := c.InitOKXPairSchema(ctx); err != nil {
				t.Fatal(err)
			}
			p, o := nativeDiscoveryFixture(t, c)
			id := p.ID.String()
			write := func() error { return c.WriteOKXLoanPolicy(ctx, p) }
			if kind == "okx_paired_catalog" {
				if err := c.WriteOKXLoanPolicy(ctx, p); err != nil {
					t.Fatal(err)
				}
				id = o.ID.String()
				write = func() error { return c.WriteOKXPairCatalog(ctx, o) }
			}
			c.maxAttempts = 1
			fault := false
			c.derivativeAfterInsert = func(table string) error {
				if table == kind {
					fault = true
					return fmt.Errorf("process stopped after native fact")
				}
				return nil
			}
			if err := write(); err == nil || !fault {
				t.Fatal("native fact crash fault not exercised", err)
			}
			var first int64
			if err := c.conn.QueryRow(ctx, "SELECT known_from_ms FROM "+c.table("source_publication_intents")+" WHERE source_kind=? AND source_id=?", kind, id).Scan(&first); err != nil {
				t.Fatal("intent must precede fact", err)
			}
			var count uint64
			if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=?", kind, id).Scan(&count); err != nil || count != 0 {
				t.Fatal("failed write published", err)
			}
			c.derivativeAfterInsert = nil
			time.Sleep(3 * time.Millisecond)
			if err := write(); err != nil {
				t.Fatal(err)
			}
			var known, published int64
			if err := c.conn.QueryRow(ctx, "SELECT known_from_ms,published_ms FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=?", kind, id).Scan(&known, &published); err != nil || known != first || published < known {
				t.Fatal("first knowledge regenerated", known, first, err)
			}
		})
	}
}

func TestDiscoveryMissingPromisedProvenanceIsRejected(t *testing.T) {
	for _, predicate := range []string{"source_order=3", "1=1"} {
		t.Run(predicate, func(t *testing.T) {
			c := commonUniverseTestClient(t)
			ctx := context.Background()
			if err := c.InitOKXPairSchema(ctx); err != nil {
				t.Fatal(err)
			}
			f := discoveryFixture(t, c)
			if err := c.WriteDiscoveryFact(ctx, f); err != nil {
				t.Fatal(err)
			}
			if _, _, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now().Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if err := c.conn.Exec(ctx, "ALTER TABLE "+c.table("public_observation_provenance")+" DELETE WHERE source_id=? AND "+predicate+" SETTINGS mutations_sync=2", f.ID); err != nil {
				t.Fatal(err)
			}
			if _, _, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now().Add(time.Millisecond)); err == nil {
				t.Fatal("missing committed source member accepted")
			}
		})
	}
}
