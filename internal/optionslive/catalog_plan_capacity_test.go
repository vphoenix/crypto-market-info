package optionslive

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type delayedCatalogRunSink struct {
	catalogMemorySink
	started chan struct{}
	resume  chan struct{}
	blocked bool
}

func (s *delayedCatalogRunSink) WriteOptionsRun(ctx context.Context, r options.LiveRun) error {
	if !s.blocked {
		s.blocked = true
		close(s.started)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.catalogMemorySink.WriteOptionsRun(ctx, r)
}

func TestCatalogPlanLatePrewarmDoesNotOverflowNextRun(t *testing.T) {
	s, w, original, _ := supervisorFixture(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	s.clock = func() time.Time { return now }
	expiry := now.Add(24 * time.Hour)
	s.entries = map[string]*catalogEntry{}
	entries := make([]*catalogEntry, options.MaxLiveBooks+1)
	for n := range entries {
		e := *original
		e.item.Spec.Instrument.ID = uint32(n + 1)
		e.item.Spec.Instrument.ExchangeSymbol = fmt.Sprintf("BTC-30OCT26-%d-C", 80000+n*1000)
		e.item.Spec.Instrument.ExpiryTime = &expiry
		e.item.Spec.NativeID += uint64(n)
		e.item.Spec.Instrument.VenueContractVersion = e.item.Spec.Version()
		e.state.InstrumentID = e.item.Spec.Instrument.ID
		e.state.RuleObservationID = uuid.New()
		e.admitted = true
		entries[n] = &e
		if n < options.MaxLiveBooks {
			s.entries[e.item.Spec.Instrument.ExchangeSymbol] = &e
		}
	}
	w.specs = []options.ContractSpec{entries[0].item.Spec}
	sink := &delayedCatalogRunSink{started: make(chan struct{}), resume: make(chan struct{})}
	s.sink = sink
	// A failed shard's members are ready to fill the existing worker. Delay
	// the run write while one more newly discovered member is prewarmed there.
	s.buildPlan(now)
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("catalog publication did not reach delayed database write")
	}
	late := entries[options.MaxLiveBooks]
	s.entries[late.item.Spec.Instrument.ExchangeSymbol] = late
	s.prewarm(late)
	if s.byID[late.item.Spec.Instrument.ID] != w || len(w.specs) != 2 {
		t.Fatal("late member was not prewarmed during plan persistence")
	}
	close(sink.resume)
	first := <-s.plans
	if first.err != nil {
		t.Fatal(first.err)
	}
	s.planned(first)
	if len(w.run.Members) != options.MaxLiveBooks || len(w.specs) != options.MaxLiveBooks+1 {
		t.Fatal("late publication did not preserve active and pending members")
	}
	// Advance the planning clock, not the sampling clock: the next plan must
	// retain the first run's members and assign the extra member exactly once.
	now = first.plan.EffectiveMinute
	s.buildPlan(now)
	second := <-s.plans
	if second.err != nil {
		t.Fatalf("late prewarm prevented next publication: %v", second.err)
	}
	owners := map[uint32]uuid.UUID{}
	for _, group := range second.groups {
		if group.retire {
			continue
		}
		if err := group.run.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := second.plan.ValidateRun(group.run, second.plan.EffectiveMinute); err != nil {
			t.Fatal(err)
		}
		for _, member := range group.run.Members {
			if owners[member.InstrumentID] != uuid.Nil {
				t.Fatal("member belongs to multiple runs")
			}
			owners[member.InstrumentID] = group.run.ID
		}
	}
	if len(owners) != len(entries) || len(second.plan.InstrumentIDs) != len(entries) {
		t.Fatal("late prewarm or original members disappeared from the next plan")
	}
	for _, member := range first.groups[0].run.Members {
		if owners[member.InstrumentID] != first.groups[0].run.ID {
			t.Fatal("existing run ownership changed to make room for late prewarm")
		}
	}
	if owners[late.item.Spec.Instrument.ID] == first.groups[0].run.ID {
		t.Fatal("late member exceeded the existing run capacity")
	}
	// Completing the second publication must transfer the pending member to
	// its new worker without retaining a competing claim in the old worker.
	t.Cleanup(func() {
		for _, worker := range s.workers {
			if worker != w {
				worker.cancel()
				<-worker.done
			}
		}
	})
	s.planned(second)
	if len(s.workers) != 2 {
		t.Fatal("overflow shard was not started")
	}
	for _, worker := range s.workers {
		for _, spec := range worker.specs {
			if s.byID[spec.Instrument.ID] != worker || owners[spec.Instrument.ID] != worker.run.ID {
				t.Fatal("old prewarm retained a competing owner after reassignment")
			}
		}
	}
	if s.hub.routes[late.item.Spec.Instrument.ID].worker != s.byID[late.item.Spec.Instrument.ID] {
		t.Fatal("late member route was not transferred to its committed owner")
	}
	now = second.plan.EffectiveMinute
	s.buildPlan(now)
	if s.planning || len(s.plans) != 0 {
		t.Fatal("stable ownership caused another reassignment")
	}
}
