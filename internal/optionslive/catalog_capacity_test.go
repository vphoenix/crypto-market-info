//go:build !race

package optionslive

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestCatalogFullUniversePlusTwentyPercentSamplingCapacity(t *testing.T) {
	base, at, _, _ := catalogFixture(t)
	template := base.specs[1]
	count := 3728
	var engines []*engine
	completed := 0
	for start := 0; start < count; start += 32 {
		var specs []options.ContractSpec
		r := options.LiveRun{ID: uuid.New(), StartedAt: at.Add(-time.Second), RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2", Selection: options.CatalogSelection, Indexes: []string{"btc_usd"}}
		limit := min(count, start+32)
		for n := start; n < limit; n++ {
			s := template
			s.NativeID += uint64(n)
			s.Instrument.ID = uint32(n + 1)
			s.Instrument.ExchangeSymbol = fmt.Sprintf("BTC-23SEP26-%d-C", 80000+n)
			s.Instrument.VenueContractVersion = s.Version()
			specs = append(specs, s)
			r.Members = append(r.Members, options.LiveMember{InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, DefinitionHash: s.DefinitionHash(), IndexID: s.IndexID})
		}
		e, err := newCatalogEngine(r, specs, at.Add(-time.Second), 20000, func(uint32) {}, func() {}, func(options.CatalogEnvelope) error { completed++; return nil })
		if err != nil {
			t.Fatal(err)
		}
		p := options.CollectionPlan{ID: uuid.New(), SessionID: uuid.New(), Revision: 1, CreatedAt: at.Add(-time.Second), EffectiveMinute: at, ConfigHash: options.PayloadHash([]byte("capacity"))}
		for _, m := range r.Members {
			p.InstrumentIDs = append(p.InstrumentIDs, m.InstrumentID)
			p.Definitions = append(p.Definitions, m.DefinitionHash)
			p.RunIDs = append(p.RunIDs, r.ID)
		}
		handleTest(t, e, event{At: at.Add(-time.Second), Plan: &p})
		epoch := uuid.New()
		e.catalog.gate = catalogGate{Epoch: epoch, Known: true, LockMode: "false", BaselineID: uuid.New()}
		for _, spec := range specs {
			id := spec.Instrument.ID
			e.catalog.states[id] = catalogState{InstrumentID: id, Known: true, Open: true, StateID: uuid.New(), RuleObservationID: uuid.New(), StateKind: 2, RuleID: options.PayloadHash([]byte("rule")), StateAt: at.Add(-time.Second), StateObservedAt: at.Add(-time.Second), RuleAt: at.Add(-time.Second), ObservedAt: at.Add(-time.Second), Epoch: epoch}
			e.catalog.connections[id] = connection{epoch: epoch, ready: true}
			if err := e.books[id].Reset(epoch); err != nil {
				t.Fatal(err)
			}
			u := orderbook.DerivativeUpdate{InstrumentID: id, Snapshot: true, Epoch: epoch, ChangeID: 1, SourceTime: at.Add(-time.Second), ReceivedAt: at.Add(-time.Second)}
			for n := int64(0); n < 20; n++ {
				u.Bids = append(u.Bids, orderbook.DerivativeChangeLevel{Action: orderbook.DerivativeNew, Level: model.Level{PriceTick: 100 - n, QtyLot: 7}})
				u.Asks = append(u.Asks, orderbook.DerivativeChangeLevel{Action: orderbook.DerivativeNew, Level: model.Level{PriceTick: 101 + n, QtyLot: 9}})
			}
			if err := e.books[id].Apply(u); err != nil {
				t.Fatal(err)
			}
		}
		engines = append(engines, e)
	}
	started := time.Now()
	max := time.Duration(0)
	for sec := 0; sec < 60; sec++ {
		before := time.Now()
		now := at.Add(time.Duration(sec) * time.Second)
		for _, e := range engines {
			e.catalog.gate.ConfirmedAt = now.Add(-time.Microsecond)
			for id, c := range e.catalog.connections {
				c.confirmed = now.Add(-time.Microsecond)
				e.catalog.connections[id] = c
				if id%5 == 0 {
					u := orderbook.DerivativeUpdate{InstrumentID: id, Epoch: c.epoch, ChangeID: uint64(sec + 2), PrevChangeID: uint64(sec + 1), SourceTime: now.Add(-time.Microsecond), ReceivedAt: now.Add(-time.Microsecond), Bids: []orderbook.DerivativeChangeLevel{{Action: orderbook.DerivativeChange, Level: model.Level{PriceTick: 100, QtyLot: uint64(sec + 8)}}}}
					if err := e.books[id].Apply(u); err != nil {
						t.Fatal(err)
					}
				}
			}
			e.now = func() time.Time { return now.Add(time.Millisecond) }
			if err := e.sample(now, now.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(before); elapsed > max {
			max = elapsed
		}
	}
	if completed != len(engines) {
		t.Fatalf("completed=%d", completed)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("books=%d shards=%d elapsed=%s worst_sequential_second=%s heap_bytes=%d", count, len(engines), time.Since(started), max, memory.HeapAlloc)
	if max > 250*time.Millisecond {
		t.Fatal("sampling capacity exceeds 250ms budget")
	}
}
func TestCatalogPublicFullUniverseProbe(t *testing.T) {
	if os.Getenv("OPTIONS_PUBLIC_INTEGRATION") != "1" {
		t.Skip("opt-in public API full universe probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	cfg := Config{RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2", EvidenceDir: t.TempDir()}.catalogDefaults()
	if directory := os.Getenv("OPTIONS_PUBLIC_EVIDENCE_DIR"); directory != "" {
		cfg.EvidenceDir = directory
	}
	sink := &catalogMemorySink{signal: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- RunCatalog(ctx, cfg, sink, slog.New(slog.NewTextHandler(os.Stdout, nil))) }()
	select {
	case <-sink.signal:
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatal("no minute committed", err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	t.Logf("minute_books=%v minute_valid=%v", sink.minuteBooks, sink.minuteValid)
	t.Logf("minute_reasons=%v quality_examples=%v", sink.minuteReasons, sink.qualityExample)
	t.Logf("minute_families(total,second59,all60)=%v valid_seconds_histogram=%v", sink.minuteFamilies, sink.minuteValidSeconds)
	t.Logf("public instruments=%d maximum_plan_books=%d plans=%d committed_shards=%d", len(sink.registry), sink.maxBooks, len(sink.plans), sink.batches)
	if sink.maxBooks < 3000 || sink.batches < 1 {
		t.Fatal("incomplete public probe")
	}
	covered := false
	for key, books := range sink.minuteBooks {
		if books >= sink.maxBooks && sink.minuteValid[key]*100 >= books*95 {
			allFamilies := len(sink.minuteFamilies[key]) == 4
			for _, counts := range sink.minuteFamilies[key] {
				allFamilies = allFamilies && counts[1]*100 >= counts[0]*95
			}
			covered = allFamilies
		}
	}
	if !covered {
		t.Fatal("no complete public minute with at least 95% valid books")
	}
}
