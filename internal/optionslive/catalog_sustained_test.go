package optionslive

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func freshCatalogJob(s *catalogSupervisor, e *catalogEntry, kind string, seq uint64) *catalogJob {
	now := time.Now().UTC().Truncate(time.Microsecond)
	item := e.item
	item.State, item.Active = "open", true
	return &catalogJob{kind: kind, scope: e.scope, epoch: s.epoch, barrierCaptured: true, barrierSequence: seq,
		result:      &deribit.ScopeResult{Instruments: []deribit.ParsedInstrument{item}},
		observation: &options.CatalogObservation{ID: uuid.New(), Status: "complete", RequestedAt: now, ObservedAt: now}}
}

func TestCatalogNewEpochRefreshReplacesOldWSState(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	old := s.epoch
	e.state.StateKind, e.state.Epoch, e.state.Known = 1, old, true
	e.state.StateID = uuid.New()
	e.latestKind = "catalog"
	s.epoch, s.activeEpoch = uuid.New(), uuid.Nil
	s.activeEpoch = s.epoch
	e.state.Known = false
	s.published(catalogResult{job: freshCatalogJob(s, e, "catalog", 0)})
	if !e.state.Known || !e.state.Open || e.state.StateKind != 2 || e.state.Epoch != s.epoch {
		t.Fatal("old WS epoch prevented fresh catalog confirmation after reconnect")
	}
}

func TestCatalogNewEpochCannotBorrowOldAppliedSequence(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	e.state.StateKind, e.state.Epoch, e.state.Known = 1, s.epoch, true
	e.appliedSequence = 9
	e.lastEvent = time.Now().UTC()
	e.latestKind = "catalog"
	newEpoch := uuid.New()
	s.activeEpoch = newEpoch
	s.pendingStates[name] = pendingState{newEpoch, 9}
	s.source(deribit.StreamEvent{Kind: "connected", Epoch: newEpoch})
	if e.appliedSequence != 0 || !e.lastEvent.IsZero() {
		t.Fatal("reconnect retained WS ordering from a different epoch")
	}
	s.published(catalogResult{job: freshCatalogJob(s, e, "catalog", 0)})
	if e.state.Known || e.state.Epoch == newEpoch {
		t.Fatal("cached bulk confirmation borrowed an applied sequence from an old epoch")
	}
}

func TestCatalogTickRecoversUnknownAdmittedMember(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	now := time.Now().UTC()
	expiry := now.Add(time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	e.admitted, e.state.Known = true, false
	e.state.StateKind, e.state.Epoch = 1, uuid.New()
	s.lifecycleReady = true
	for _, scope := range catalogScopes() {
		s.scopes[scope.String()] = now
	}
	s.tick(now)
	if len(s.jobs) != 1 || (<-s.jobs).symbol != e.item.Spec.Instrument.ExchangeSymbol {
		t.Fatal("admitted unknown member had no independent recovery request")
	}
}

func TestCatalogDurableUnknownTerminalClearsOnlyMatchingIntent(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	name := "BTC-30OCT26-80888-C"
	o := options.LifecycleObservation{ID: uuid.New(), Kind: "state", Epoch: s.epoch, Sequence: 9, Symbol: name, State: "archivized"}
	s.pendingStates[name] = pendingState{s.epoch, 9}
	s.uncertainSymbols[name] = true
	s.published(catalogResult{job: &catalogJob{life: &o}})
	if _, ok := s.pendingStates[name]; ok || s.uncertainSymbols[name] {
		t.Fatal("durable terminal state retained an unknown-symbol recovery intent")
	}
	s.pendingStates[name] = pendingState{s.epoch, 10}
	s.published(catalogResult{job: &catalogJob{life: &o}})
	if s.pendingStates[name].sequence != 10 {
		t.Fatal("old terminal evidence removed a newer source barrier")
	}
}

func TestBookDetachedSubscribeStillWaitsForACK(t *testing.T) {
	p := &bookPhysical{routes: map[string]*bookRoute{}, requested: map[string]bool{"book.removed.100ms": true}, acked: map[string]bool{}}
	if !p.controlPending() {
		t.Fatal("detached unacknowledged subscribe released the physical control barrier")
	}
}

func TestCatalogWSStateOrderingDoesNotUseHTTPLocalClock(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	now := time.Now().UTC().Truncate(time.Microsecond)
	s.pendingStates[name] = pendingState{s.epoch, 8}
	j := freshCatalogJob(s, e, "instrument", 8)
	j.result.Instruments[0].State, j.result.Instruments[0].Active = "inactive", false
	s.published(catalogResult{job: j})
	source := now.Add(-100 * time.Millisecond)
	s.pendingStates[name] = pendingState{s.epoch, 9}
	o := options.LifecycleObservation{ID: uuid.New(), Kind: "state", Epoch: s.epoch, Sequence: 9,
		Symbol: name, State: "open", SourceTime: &source, ReceivedAt: now}
	s.published(catalogResult{job: &catalogJob{life: &o}})
	if !e.state.Known || !e.state.Open || e.state.StateID != o.ID {
		t.Fatal("new WS open event was ordered against a local HTTP observation clock")
	}
}

func TestCatalogSlowStatePersistencePreservesNewerHTTPConfirmation(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	s.pendingStates[name] = pendingState{s.epoch, 9}
	source := time.Now().UTC().Add(-time.Second)
	e.lastEvent = source
	j := freshCatalogJob(s, e, "instrument", 9)
	s.published(catalogResult{job: j})
	o := options.LifecycleObservation{ID: uuid.New(), Kind: "state", Epoch: s.epoch, Sequence: 9,
		Symbol: name, State: "locked", SourceTime: &source}
	s.published(catalogResult{job: &catalogJob{life: &o}})
	if !e.state.Open || e.state.StateID != j.observation.ID || e.appliedSequence != 9 {
		t.Fatal("late durable old state undid an HTTP confirmation issued after its barrier")
	}
}

func TestCatalogDroppedUnknownOpenSchedulesIndependentConfirmation(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	now := time.Now().UTC()
	expiry := now.Add(time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	name := "BTC-30OCT26-80888-C"
	s.pendingStates[name] = pendingState{s.epoch, 9}
	o := options.LifecycleObservation{Kind: "state", Epoch: s.epoch, Sequence: 9, Symbol: name, State: "open"}
	s.dropRetry(&catalogJob{life: &o})
	s.lifecycleReady = true
	for _, scope := range catalogScopes() {
		s.scopes[scope.String()] = now
	}
	s.tick(now)
	if len(s.jobs) != 1 || (<-s.jobs).symbol != name {
		t.Fatal("unknown new open state lost its recovery intent until the next bulk directory")
	}
}

func TestCatalogSlowUnsubscribeACKBoundsContinuousRemovals(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	h := s.hub
	p := &bookPhysical{epoch: uuid.New(), ready: true, routes: map[string]*bookRoute{}, used: map[string]bool{},
		requested: map[string]bool{}, acked: map[string]bool{}, commands: make(chan deribit.SessionCommand, 64), cancel: func() {}}
	h.physical = []*bookPhysical{p}
	for n := uint32(1); n <= 2002; n++ {
		symbol := fmt.Sprintf("BTC-continuous-%d", n)
		ch := "book." + symbol + ".100ms"
		r := &bookRoute{id: n, symbol: symbol, worker: w, physical: p}
		p.routes[ch], h.routes[n], p.used[ch], p.requested[ch], p.acked[ch] = r, r, true, true, true
	}
	for n := uint32(1); n <= 1800; n++ {
		h.remove(n)
		h.flushUnsubscribes()
		if len(p.commands) > 1 || len(p.unsubscribing) > 1 {
			t.Fatal("slow ACK allowed the control queue to grow with time")
		}
	}
	cmd := <-p.commands
	if len(cmd.Channels) != 1 || len(p.pendingUnsubscribe) != 1799 {
		t.Fatal("continuous removals lost bounded intent")
	}
	if err := h.dispatch(p, deribit.StreamEvent{Kind: "unsubscribed", Epoch: p.epoch, Channel: cmd.Channels[0]}); err != nil {
		t.Fatal(err)
	}
	h.flushUnsubscribes()
	cmd = <-p.commands
	if len(cmd.Channels) != 512 || len(p.pendingUnsubscribe) != 1287 || !p.controlPending() {
		t.Fatal("ACK did not release the next merged batch")
	}
}

func TestCatalogHistoricalChannelUseRotatesAnEpochAtConnectionLimit(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	h := s.hub
	h.cfg.MaxConnections = 4
	ch := "book." + e.item.Spec.Instrument.ExchangeSymbol + ".100ms"
	rotations := 0
	for n := uint32(2); n <= 3; n++ {
		r := &bookRoute{id: n, symbol: fmt.Sprintf("healthy-%d", n), worker: w}
		p := &bookPhysical{epoch: uuid.New(), routes: map[string]*bookRoute{r.symbol: r}, used: map[string]bool{ch: true}, cancel: func() { rotations++ }}
		r.physical = p
		h.physical = append(h.physical, p)
	}
	r := &bookRoute{id: 1, symbol: e.item.Spec.Instrument.ExchangeSymbol, worker: w}
	h.assign(r)
	if rotations != 1 {
		t.Fatal("channel exhausted every old epoch and was left permanently unassigned")
	}
	h.assign(r)
	if rotations != 1 {
		t.Fatal("rotation was not throttled while awaiting an epoch slot")
	}
}

func TestCatalogExpiredMemberDoesNotRequestMissingMetadata(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	expiry := time.Now().Add(-time.Minute)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	j := freshCatalogJob(s, e, "catalog", 0)
	j.result.Instruments = nil
	s.published(catalogResult{job: j})
	if len(s.jobs) != 0 || len(s.retries) != 0 {
		t.Fatal("expired member absent from the current directory generated recovery work")
	}
	j = &catalogJob{scope: e.scope, symbol: e.item.Spec.Instrument.ExchangeSymbol, kind: "instrument"}
	s.retries = []scheduledCatalogJob{{at: time.Now().Add(-time.Second), job: j}}
	s.inflight[e.scope.String()+":"+j.symbol] = j
	s.tick(time.Now())
	if len(s.jobs) != 0 || len(s.retries) != 0 || s.inflight[e.scope.String()+":"+j.symbol] != nil {
		t.Fatal("an expired member kept retrying queued metadata work")
	}
}

func TestCatalogRemovalCoalescesUnsubscribeAcrossWorkers(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	h := s.hub
	cancelled := false
	p := &bookPhysical{epoch: uuid.New(), routes: map[string]*bookRoute{}, used: map[string]bool{}, acked: map[string]bool{},
		requested: map[string]bool{}, commands: make(chan deribit.SessionCommand, 64), cancel: func() { cancelled = true }}
	h.physical = []*bookPhysical{p}
	for n := uint32(1); n <= 67; n++ {
		symbol := fmt.Sprintf("BTC-removal-%d", n)
		ch := "book." + symbol + ".100ms"
		r := &bookRoute{id: n, symbol: symbol, worker: w, physical: p}
		p.routes[ch], h.routes[n], p.requested[ch] = r, r, true
		p.acked[ch] = true
	}
	for n := uint32(1); n <= 65; n++ {
		h.remove(n)
	}
	if cancelled || p.closing || len(p.commands) != 0 || len(p.routes) != 2 {
		t.Fatal("member removal flooded the command queue or retired healthy unrelated routes")
	}
	h.flushUnsubscribes()
	if len(p.commands) != 1 || len((<-p.commands).Channels) != 65 {
		t.Fatal("removals were not sent as one physical-connection unsubscribe")
	}
}

func TestCatalogExpiredStateEvidenceDoesNotEmitValidSeconds(t *testing.T) {
	e, at, out, _ := catalogFixture(t)
	s := e.catalog.states[1]
	s.StateObservedAt = at.Add(-36 * time.Minute)
	s.ObservedAt = at.Add(-time.Second)
	e.catalog.states[1] = s
	for sec := 0; sec < 60; sec++ {
		catalogStep(t, e, at.Add(time.Duration(sec)*time.Second))
	}
	for _, q := range (*out)[0].Live.Books[0].Quality {
		if q.MarketKnown || q.ReplayValid || q.Reason != model.DerivativeMetadataUncertain {
			t.Fatal("fresh rules masked stale state evidence until the database rejected the shard")
		}
	}
	if err := (*out)[0].Validate(); err != nil {
		t.Fatal("explicitly invalid minute is not persistable", err)
	}
}

type sustainedBatchSink struct {
	catalogMemorySink
	batchSizes []int
}

func (s *sustainedBatchSink) WriteOptionsLifecycles(_ context.Context, oo []options.LifecycleObservation) error {
	for _, o := range oo {
		if err := o.Validate(); err != nil {
			return err
		}
	}
	s.batchSizes = append(s.batchSizes, len(oo))
	return nil
}

func TestCatalogWholeUniverseLifecycleBurstIsBatched(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	s.cfg.EvidenceDir = t.TempDir()
	sink := &sustainedBatchSink{}
	s.sink = sink
	s.lifeJobs = make(chan *catalogJob, s.evidenceQueueLimit())
	s.persisted = make(chan catalogResult, s.evidenceQueueLimit())
	now := time.Now().UTC().Truncate(time.Microsecond)
	raw := []byte(`{"fixture":"whole universe close/open"}`)
	const events = 3728 * 2
	for n := 0; n < events; n++ {
		state := "locked"
		if n >= events/2 {
			state = "open"
		}
		o := options.LifecycleObservation{ID: uuid.New(), Kind: "state", Channel: "instrument.state.option.BTC", Epoch: s.epoch,
			Sequence: uint64(n + 1), Symbol: fmt.Sprintf("BTC-30OCT26-%d-C", 80000+n%(events/2)), State: state,
			SourceTime: &now, ReceivedAt: now, PayloadHash: options.PayloadHash(raw)}
		if !s.enqueue(&catalogJob{life: &o, raw: raw, epoch: s.epoch}) {
			t.Fatal("full universe burst refused")
		}
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.ctx = ctx
	done := make(chan struct{})
	go func() { s.persistLifecycleLoop(); close(done) }()
	for n := 0; n < events; n++ {
		select {
		case result := <-s.persisted:
			if result.err != nil {
				t.Fatal(result.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("batch persistence did not drain burst")
		}
	}
	cancel()
	<-done
	total := 0
	for _, size := range sink.batchSizes {
		total += size
	}
	if total != events || len(sink.batchSizes) > (events+127)/128+1 {
		t.Fatal("events lost or persisted one at a time", total, len(sink.batchSizes))
	}
	t.Logf("persisted_events=%d batches=%d", total, len(sink.batchSizes))
}

func TestCatalogCreationPreservesDurableOpenEvent(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	e.state.StateKind, e.state.StateID, e.state.Known, e.state.Open = 1, uuid.New(), true, true
	e.marketState, e.appliedSequence = "open", 9
	s.pendingStates[name] = pendingState{s.epoch, 9}
	want := e.state.StateID
	j := freshCatalogJob(s, e, "creation", 9)
	j.result.Instruments[0].State, j.result.Instruments[0].Active = "inactive", false
	s.published(catalogResult{job: j})
	if !e.state.Open || !e.state.Known || e.state.StateID != want || e.marketState != "open" {
		t.Fatal("delayed inactive creation overwrote durable open state")
	}
}

func TestCatalogRefreshRenewsConfirmedStateEvidence(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	e.state.StateKind, e.appliedSequence = 2, 9
	e.state.ObservedAt, e.state.StateAt = time.Now().Add(-36*time.Minute), time.Now().Add(-36*time.Minute)
	s.pendingStates[name] = pendingState{s.epoch, 9}
	j := freshCatalogJob(s, e, "catalog", 0)
	s.published(catalogResult{job: j})
	if e.state.StateID != j.observation.ID || e.state.RuleObservationID != j.observation.ID {
		t.Fatal("periodic refresh renewed rules but retained expired catalog state proof")
	}
}

func TestCatalogTickReconfirmsLostLatestState(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	now := time.Now().UTC()
	expiry := now.Add(24 * time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	e.state.StateKind, e.state.Known, e.appliedSequence = 2, false, 8
	s.pendingStates[e.item.Spec.Instrument.ExchangeSymbol] = pendingState{s.epoch, 9}
	s.lifecycleReady = true
	for _, scope := range catalogScopes() {
		s.scopes[scope.String()] = now
	}
	s.tick(now)
	if len(s.jobs) != 1 {
		t.Fatal("lost durable event left pending state permanently unconfirmed")
	}
	j := <-s.jobs
	if j.symbol != e.item.Spec.Instrument.ExchangeSymbol || j.kind != "instrument" || j.barrierSequence != 9 {
		t.Fatal("repair did not capture latest symbol barrier")
	}
}

func TestCatalogTickConfirmsInactiveCreationWithoutOpenPush(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	now := time.Now().UTC()
	expiry := now.Add(24 * time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	e.admitted, e.state.Open, e.marketState = false, false, "inactive"
	s.lifecycleReady = true
	for _, scope := range catalogScopes() {
		s.scopes[scope.String()] = now
	}
	s.tick(now)
	if len(s.jobs) != 1 || (<-s.jobs).symbol != e.item.Spec.Instrument.ExchangeSymbol {
		t.Fatal("inactive creation waited forever for an open notification")
	}
}

func TestBookRecoveryCooldownRetainsRecoveryIntent(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	r := &bookRoute{id: 1, symbol: e.item.Spec.Instrument.ExchangeSymbol, worker: w, lastRecovery: time.Now()}
	s.hub.routes[1] = r
	s.hub.recover(1)
	if !s.hub.recovery[1] {
		t.Fatal("recovery request during cooldown was discarded")
	}
	s.hub.recoverPending()
	if !s.hub.recovery[1] {
		t.Fatal("maintenance discarded throttled recovery request")
	}
}
