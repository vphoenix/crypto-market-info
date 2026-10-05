package optionslive

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

// Advance the supervisor's clock, retaining the real catalog publication,
// engine, writer evidence validation, database commit and replay paths.
func TestCatalogSustainedRefreshAcross96MinutesClickHouse(t *testing.T) {
	if os.Getenv("OPTIONS_SUSTAINED_INTEGRATION") != "1" {
		t.Skip("opt-in isolated real ClickHouse sustained refresh regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db := fmt.Sprintf("crypto_options_sustained_it_%d", time.Now().UnixNano())
	c, err := clickhouse.Open(ctx, clickhouse.Config{Addresses: []string{"127.0.0.1:9000"}, Database: db, MaxAttempts: 1, WriteTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		admin, err := ch.Open(&ch.Options{Addr: []string{"127.0.0.1:9000"}, Auth: ch.Auth{Database: "default", Username: "default"}})
		if err == nil {
			_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"` SYNC")
			_ = admin.Close()
		}
	})
	if err = c.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.InitOptionsCatalogSchema(ctx); err != nil {
		t.Fatal(err)
	}
	s, w, entry, at := supervisorFixture(t)
	clock := at.Add(-time.Second)
	s.clock = func() time.Time { return clock }
	s.sink = c
	raw, err := os.ReadFile("../exchange/deribit/testdata/metadata-BTC-option.json")
	if err != nil {
		t.Fatal(err)
	}
	items, _, err := deribit.DecodeInstruments(raw, "BTC", "option", clock)
	if err != nil {
		t.Fatal(err)
	}
	item := items[0]
	item.Spec.Instrument.ID = entry.item.Spec.Instrument.ID
	item.Rule.InstrumentID = item.Spec.Instrument.ID
	item.Rule.SourceURL = "https://www.deribit.com/api/v2/public/get_instruments"
	entry.item = item
	name := item.Spec.Instrument.ExchangeSymbol
	s.pendingStates[name] = pendingState{s.epoch, 9}
	refresh := func(kind string) *catalogJob {
		j := &catalogJob{kind: kind, scope: entry.scope, symbol: name, epoch: s.epoch, barrierCaptured: true, barrierSequence: 9,
			result: &deribit.ScopeResult{RequestID: uuid.New(), Scope: entry.scope, URL: item.Rule.SourceURL, Status: "complete",
				RequestedAt: clock, ObservedAt: clock, PayloadHash: options.PayloadHash(raw), Raw: raw, RawCount: 1,
				Instruments: []deribit.ParsedInstrument{item}}}
		if err := s.persist(ctx, j); err != nil {
			t.Fatal("persist catalog", err)
		}
		s.published(catalogResult{job: j})
		return j
	}
	j := refresh("instrument")
	r := w.run
	r.Members[0].InstrumentID = entry.item.Spec.Instrument.ID
	r.Members[0].DefinitionHash = entry.item.Spec.DefinitionHash()
	if err = c.WriteOptionsRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	p := options.CollectionPlan{ID: uuid.New(), SessionID: s.session, Revision: 1, CreatedAt: clock, EffectiveMinute: at,
		ConfigHash: options.PayloadHash([]byte("sustained regression")), ObservationIDs: []uuid.UUID{j.observation.ID},
		InstrumentIDs: []uint32{r.Members[0].InstrumentID}, Definitions: []string{r.Members[0].DefinitionHash}, RunIDs: []uuid.UUID{r.ID}}
	if err = c.WriteOptionsPlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	e, err := newCatalogEngine(r, []options.ContractSpec{entry.item.Spec}, clock, 20000, func(uint32) {}, func() {}, func(v options.CatalogEnvelope) error { return c.WriteOptionsCatalogMinute(ctx, v) })
	if err != nil {
		t.Fatal(err)
	}
	handleTest(t, e, event{At: clock, Plan: &p})
	g := options.LifecycleObservation{ID: uuid.New(), Kind: "status", Channel: "public/status", Epoch: s.epoch, Sequence: 1,
		ReceivedAt: clock, PayloadHash: options.PayloadHash([]byte("status fixture")), LockMode: "false"}
	if err = c.WriteOptionsLifecycle(ctx, g); err != nil {
		t.Fatal(err)
	}
	gate := s.gate
	gate.BaselineID, gate.ConfirmedAt, gate.Known = g.ID, clock, true
	handleTest(t, e, event{At: clock, Gate: &gate})
	drainStates := func() {
		for len(w.q.queue) > 0 {
			v := <-w.q.queue
			w.q.consumed(v)
			if v.State != nil {
				v.At = clock
				handleTest(t, e, v)
			}
		}
	}
	drainStates()
	bookSnapshot(t, e, clock, s.epoch)
	for minute := 0; minute < 96; minute++ {
		if minute != 0 && minute%30 == 0 {
			clock = at.Add(time.Duration(minute)*time.Minute - time.Second)
			refresh("catalog")
			drainStates()
		}
		for sec := 0; sec < 60; sec++ {
			clock = at.Add(time.Duration(minute)*time.Minute + time.Duration(sec)*time.Second)
			catalogStep(t, e, clock)
		}
		minuteTime := at.Add(time.Duration(minute) * time.Minute)
		loaded, err := c.LoadOptionsCatalogMinute(ctx, r.ID, minuteTime)
		if err != nil {
			t.Fatalf("minute %d load: %v", minute, err)
		}
		for sec := 0; sec < 60; sec++ {
			result, err := replay.ReplayDerivative(loaded.Live.Books[0], uint8(sec))
			if err != nil || !result.Quality.ReplayValid {
				t.Fatalf("minute %d second %d valid replay: %v", minute, sec, err)
			}
		}
	}
	t.Logf("database=%s virtual_minutes=96 catalog_refreshes=4 replay_valid_seconds=5760", db)
}
