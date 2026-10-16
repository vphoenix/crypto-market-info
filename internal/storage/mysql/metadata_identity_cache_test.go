package mysql

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMetadataIdentityCacheReusesOverlappingGroupsAndKeepsCopies(t *testing.T) {
	c := minimalOptionsPlanClient(t)
	ctx := context.Background()
	run, envelope := liveDBFixture(t, c)
	base, err := c.LoadDerivativeTradingRule(ctx, envelope.Books[0].Quality[0].TradingRuleID)
	if err != nil {
		t.Fatal(err)
	}
	r1 := base
	r1.Bands = []options.TickBand{{AbovePrice: decimal.Zero, TickSize: base.SourceTick}}
	r2 := r1
	r2.ObservedAt = r2.ObservedAt.Add(time.Second)
	r2.KnownFrom = r2.KnownFrom.Add(time.Second)
	r2.EffectiveFrom = r2.EffectiveFrom.Add(time.Second)
	if err = c.WriteDerivativeTradingRules(ctx, []options.TradingRule{r1, r2}); err != nil {
		t.Fatal(err)
	}
	c.SetMetadataCache(NewMetadataCache(32 << 20))
	ids := []uint32{}
	for _, m := range run.Members {
		ids = append(ids, m.InstrumentID)
	}
	if len(ids) < 2 {
		t.Fatal("fixture needs multiple contracts")
	}
	if _, err = c.LoadDerivativeSpecs(ctx, ids); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadDerivativeTradingRules(ctx, []string{r1.ID(), r2.ID()}); err != nil {
		t.Fatal(err)
	}
	admin, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	locker, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err = locker.ExecContext(ctx, "LOCK TABLES "+c.table("instrument")+" WRITE,"+c.table("derivative_contract_spec")+" WRITE,"+c.table("derivative_trading_rule")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer locker.ExecContext(context.Background(), "UNLOCK TABLES")
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	// The requested sets differ from the warm sets. Any unnecessary SQL now
	// waits on our table lock and fails the short context.
	got, err := c.LoadDerivativeSpecs(readCtx, []uint32{ids[len(ids)-1], ids[0]})
	if err != nil || len(got) != 2 || got[0].Instrument.ID != ids[0] {
		t.Fatal("overlapping spec group missed cache or changed order", got, err)
	}
	rules, err := c.LoadDerivativeTradingRules(readCtx, []string{r1.ID()})
	if err != nil {
		t.Fatal("overlapping rule group missed cache", err)
	}
	rules[r1.ID()].Bands[0].TickSize = decimal.NewFromInt(99)
	rules, err = c.LoadDerivativeTradingRules(readCtx, []string{r1.ID()})
	if err != nil || rules[r1.ID()].ID() != r1.ID() {
		t.Fatal("returned nested rule aliases cache", err)
	}
	if _, err = locker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	missing := r1
	missing.ObservedAt = missing.ObservedAt.Add(time.Minute)
	missing.KnownFrom = missing.KnownFrom.Add(time.Minute)
	missing.EffectiveFrom = missing.EffectiveFrom.Add(time.Minute)
	if _, err = c.LoadDerivativeTradingRules(ctx, []string{missing.ID()}); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("missing reference not rejected", err)
	}
	if err = c.WriteDerivativeTradingRule(ctx, missing); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadDerivativeTradingRules(ctx, []string{missing.ID()}); err != nil {
		t.Fatal("missing result was cached", err)
	}
	c.metadata.Clear()
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("derivative_trading_rule")+" SET source_tick_size=source_tick_size+1 WHERE trading_rule_id=?", []byte(r1.ID())); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadDerivativeTradingRules(ctx, []string{r1.ID()}); err == nil {
		t.Fatal("cache clear bypassed immutable digest recheck")
	}
}
