package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestTradingRuleDoesNotBlockMinuteCommit(t *testing.T) {
	c := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, envelope := liveDBFixture(t, c)
	rule, err := c.LoadDerivativeTradingRule(ctx, envelope.Books[0].Quality[0].TradingRuleID)
	if err != nil {
		t.Fatal(err)
	}
	rule.ObservedAt = rule.ObservedAt.Add(time.Hour)
	rule.KnownFrom = rule.KnownFrom.Add(time.Hour)
	rule.EffectiveFrom = rule.EffectiveFrom.Add(time.Hour)
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{testBatch(t, 1, at, 5)}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.PrepareDelivery(ctx, ds[0])
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	blocked := context.WithValue(ctx, ruleTransactionKey{}, func() error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() { done <- c.WriteDerivativeTradingRules(blocked, []options.TradingRule{rule}) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("rule write failed before transaction", err)
	case <-ctx.Done():
		t.Fatal("rules never acquired generation")
	}
	// A real minute publication must complete while the rule transaction is open.
	minuteCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	err = c.WithProducer(c.producer).CommitPrepared(minuteCtx, p)
	stop()
	if err != nil {
		t.Fatal("rule refresh blocked minute commit", err)
	}
	if ok, e := c.Committed(ctx, ds[0]); e != nil || !ok {
		t.Fatal("minute not durably published", ok, e)
	}
	// The same shared generation protection must still prevent activation.
	probe, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = probe.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	probe.Rollback()
	if err == nil {
		t.Fatal("producer activation was not fenced by open rule batch")
	}
	close(release)
	released = true
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadDerivativeTradingRule(ctx, rule.ID()); err != nil {
		t.Fatal("rule batch was not committed", err)
	}
}

func TestTradingRulePreparationWaitRaceAtomicityAndFencing(t *testing.T) {
	c := minimalOptionsPlanClient(t)
	ctx := context.Background()
	_, envelope := liveDBFixture(t, c)
	base, err := c.LoadDerivativeTradingRule(ctx, envelope.Books[0].Quality[0].TradingRuleID)
	if err != nil {
		t.Fatal(err)
	}
	makeRules := func(offset, count int) []options.TradingRule {
		out := make([]options.TradingRule, count)
		for n := range out {
			r := base
			delta := time.Duration(offset+n) * time.Second
			r.ObservedAt = r.ObservedAt.Add(delta)
			r.KnownFrom = r.KnownFrom.Add(delta)
			r.EffectiveFrom = r.EffectiveFrom.Add(delta)
			out[n] = r
		}
		return out
	}
	admin, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	blocker, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err = blocker.ExecContext(ctx, "LOCK TABLES "+c.table("derivative_trading_rule")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")
	first := makeRules(10, 12)
	writeCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- c.WriteDerivativeTradingRules(writeCtx, first) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		err = admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=? AND STATE='Waiting for table metadata lock' AND INFO LIKE '%derivative_trading_rule%'", c.database).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rule read did not reach metadata wait")
		}
		time.Sleep(20 * time.Millisecond)
	}
	probe, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = probe.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	probe.Rollback()
	if err != nil || generation != c.producer.Generation {
		t.Fatal("cold rule read pinned generation", generation, err)
	}
	if _, err = blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rules did not publish after read resumed")
	}
	second := makeRules(100, 12)
	hookCalls := 0
	race := context.WithValue(ctx, rulePreparedKey{}, func() error {
		hookCalls++
		if hookCalls == 1 {
			return c.WriteDerivativeTradingRules(ctx, second)
		}
		return nil
	})
	if err = c.WriteDerivativeTradingRules(race, second); err != nil || hookCalls < 2 {
		t.Fatal("same-content race was not rechecked", hookCalls, err)
	}
	for _, r := range second {
		got, e := c.LoadDerivativeTradingRule(ctx, r.ID())
		if e != nil || got.ID() != r.ID() {
			t.Fatal("immutable rule changed", e)
		}
	}
	var before int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("derivative_trading_rule")).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err = c.db.ExecContext(ctx, "ALTER TABLE "+c.table("derivative_trading_rule")+" ADD UNIQUE INDEX test_rule_time(instrument_id,known_from)"); err != nil {
		t.Fatal(err)
	}
	large := makeRules(1000, 512)
	maxID := ""
	for _, r := range large {
		if r.ID() > maxID {
			maxID = r.ID()
		}
	}
	conflict := base
	for n := range 100000 {
		conflict.PayloadHash = model.DiscoveryHash(fmt.Sprintf("conflicting-rule-%d", n))
		if conflict.ID() > maxID {
			break
		}
	}
	if conflict.ID() <= maxID {
		t.Fatal("could not place conflicting row after first SQL chunk")
	}
	large = append(large, conflict)
	if err = c.WriteDerivativeTradingRules(ctx, large); err == nil {
		t.Fatal("ordinary unique conflict became batch success")
	}
	var after int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("derivative_trading_rule")).Scan(&after); err != nil || after != before {
		t.Fatal("first SQL chunk escaped rollback", before, after, err)
	}
	if _, err = c.db.ExecContext(ctx, "ALTER TABLE "+c.table("derivative_trading_rule")+" DROP INDEX test_rule_time"); err != nil {
		t.Fatal(err)
	}
	stale := context.WithValue(ctx, rulePreparedKey{}, func() error {
		_, e := ActivateProducer(ctx, admin, c.producer.Service, c.producer.Generation, uuid.New())
		return e
	})
	if err = c.WriteDerivativeTradingRules(stale, makeRules(5000, 1)); !errors.Is(err, contracts.ErrFenced) {
		t.Fatal("generation changed during preparation was ignored", err)
	}
}
