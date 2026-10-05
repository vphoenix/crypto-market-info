package clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

// Exercise a complete scope larger than the live BTC catalog against the same
// 30s persistence budget, including duplicates and failed batch admission.
func TestDerivativeCatalogBatchRegistrationAndReferences(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const count = 1100
	specs := make([]options.ContractSpec, count)
	for n := range specs {
		specs[n] = derivativeTestSpec(fmt.Sprintf("CATALOG-BATCH-%04d", n), uint64(38001+n))
	}
	duplicate := specs[0]
	duplicate.EvidenceHash = options.PayloadHash([]byte("later duplicate evidence"))
	started := time.Now()
	registered, err := c.RegisterDerivativeSpecs(ctx, append(specs, duplicate))
	if err != nil {
		t.Fatal(err)
	}
	if registered[count].Instrument.ID != registered[0].Instrument.ID || registered[count].EvidenceHash != specs[0].EvidenceHash {
		t.Fatal("same-batch duplicate replaced FIRST evidence")
	}
	refresh := append([]options.ContractSpec(nil), specs...)
	for n := range refresh {
		refresh[n].EvidenceHash = options.PayloadHash([]byte("refreshed evidence"))
	}
	again, err := c.RegisterDerivativeSpecs(ctx, refresh)
	if err != nil {
		t.Fatal(err)
	}
	rules := make([]options.TradingRule, count)
	at := time.Date(2026, 10, 4, 7, 0, 0, 0, time.UTC)
	for n, s := range registered[:count] {
		if again[n].Instrument.ID != s.Instrument.ID || again[n].EvidenceHash != s.EvidenceHash {
			t.Fatal("refresh changed definition identity or FIRST evidence")
		}
		rules[n] = options.TradingRule{InstrumentID: s.Instrument.ID, SourceTick: decimal.RequireFromString("0.0001"), MinAmount: decimal.NewFromInt(1), AmountStep: decimal.NewFromInt(1), ObservedAt: at, KnownFrom: at, EffectiveFrom: at, EffectiveTimeBasis: "first_observed", SourceURL: "https://www.deribit.com/api/v2/public/get_instruments", PayloadHash: options.PayloadHash([]byte("batch rule fixture"))}
	}
	rows := func(table string) uint64 {
		t.Helper()
		var n uint64
		if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table(table)+" FINAL").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	invalid := rules[1]
	invalid.MinAmount = decimal.NewFromInt(-1)
	if err := c.WriteDerivativeTradingRules(ctx, []options.TradingRule{rules[0], invalid}); err == nil || rows("derivative_trading_rule") != 0 {
		t.Fatal("invalid batch partially inserted")
	}
	missing := rules[1]
	missing.InstrumentID = 4000000000
	if err := c.WriteDerivativeTradingRules(ctx, []options.TradingRule{rules[0], missing}); err == nil || rows("derivative_trading_rule") != 0 {
		t.Fatal("missing spec batch partially inserted")
	}
	for n := 0; n < 2; n++ {
		if err := c.WriteDerivativeTradingRules(ctx, append(rules, rules[0])); err != nil {
			t.Fatal(err)
		}
	}
	if rows("derivative_contract_spec") != count || rows("derivative_trading_rule") != count {
		t.Fatal("duplicate identity after retry")
	}
	for _, n := range []int{0, count - 1} {
		stored, err := c.LoadDerivativeTradingRule(ctx, rules[n].ID())
		if err != nil || stored.ID() != rules[n].ID() || !stored.KnownFrom.Equal(at) {
			t.Fatal("captured rule identity changed", err)
		}
	}
	t.Logf("full scope batch: books=%d definitions+refresh+rules+retry elapsed=%s", count, time.Since(started))
}

func TestDerivativeBatchReferenceFailureIsolation(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	_, e := catalogDBFixture(t, c)
	if err := c.validateDerivativeReferences(ctx, e.Live.Books); err != nil {
		t.Fatal(err)
	}
	books := e.Live.Clone().Books
	saved := books[0].Quality[0]
	books[0].Quality[0].TradingRuleID = options.PayloadHash([]byte("missing rule"))
	if err := c.validateDerivativeReferences(ctx, books); err == nil {
		t.Fatal("missing bulk rule admitted")
	}
	books[0].Quality[0] = saved
	books[0].Quality[0].RulePublishedAt = saved.RulePublishedAt.Add(-time.Hour)
	if err := c.validateDerivativeReferences(ctx, books); err == nil {
		t.Fatal("rule used before known_from")
	}
	books[0].Quality[0] = saved
	books[0].Quality[0].TradingRuleID = books[1].Quality[0].TradingRuleID
	if err := c.validateDerivativeReferences(ctx, books); err == nil {
		t.Fatal("bulk rule reused across instruments")
	}
	books[0].Quality[0] = saved
	if err := c.conn.Exec(ctx, "ALTER TABLE "+c.table("derivative_contract_spec")+" DELETE WHERE instrument_id=? SETTINGS mutations_sync=2", books[0].InstrumentID); err != nil {
		t.Fatal(err)
	}
	if err := c.validateDerivativeReferences(ctx, books); err == nil {
		t.Fatal("missing bulk spec admitted")
	}
}
