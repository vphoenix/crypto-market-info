package optionslive

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

var fakeNotFound = errors.New("missing")

type catalogMemorySink struct {
	minuteBooks        map[string]int
	minuteValid        map[string]int
	minuteReasons      map[string]map[string]int
	qualityExample     map[string]string
	minuteFamilies     map[string]map[string][3]int // total, valid at second 59, all 60 seconds valid
	minuteValidSeconds map[string]map[int]int
	writeSlots         chan struct{}
	registry           map[string]options.ContractSpec
	batches            int
	maxBooks           int
	signal             chan struct{}
	mu                 sync.Mutex
	plans              []options.CollectionPlan
	runs               []options.LiveRun
}

func (*catalogMemorySink) InitOptionsLiveSchema(context.Context) error    { return nil }
func (*catalogMemorySink) InitOptionsCatalogSchema(context.Context) error { return nil }
func (s *catalogMemorySink) RegisterDerivativeSpecs(_ context.Context, specs []options.ContractSpec) ([]options.ContractSpec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registry == nil {
		s.registry = map[string]options.ContractSpec{}
	}
	for n := range specs {
		key := specs[n].Version()
		if old, ok := s.registry[key]; ok {
			specs[n] = old
		} else {
			specs[n].Instrument.ID = uint32(len(s.registry) + 1)
			s.registry[key] = specs[n]
		}
	}
	return specs, nil
}

func (*catalogMemorySink) WriteDerivativeTradingRule(_ context.Context, r options.TradingRule) error {
	return r.Validate()
}
func (*catalogMemorySink) WriteDerivativeTradingRules(_ context.Context, rules []options.TradingRule) error {
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (*catalogMemorySink) WriteOptionsMetadata(context.Context, options.MetadataObservation) error {
	return nil
}
func (s *catalogMemorySink) WriteOptionsRun(_ context.Context, r options.LiveRun) error {
	if e := r.Validate(); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs = append(s.runs, r)
	return nil
}
func (*catalogMemorySink) WriteOptionsMinute(context.Context, options.LiveEnvelope) error { return nil }
func (s *catalogMemorySink) WriteOptionsCatalogMinute(ctx context.Context, e options.CatalogEnvelope) error {
	s.mu.Lock()
	if s.writeSlots == nil {
		s.writeSlots = make(chan struct{}, 3)
	}
	slots := s.writeSlots
	s.mu.Unlock()
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := e.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.batches++
	if s.minuteBooks == nil {
		s.minuteBooks = map[string]int{}
		s.minuteValid = map[string]int{}
		s.minuteReasons = map[string]map[string]int{}
		s.qualityExample = map[string]string{}
		s.minuteFamilies = map[string]map[string][3]int{}
		s.minuteValidSeconds = map[string]map[int]int{}
	}
	key := e.PlanID.String() + e.Live.MinuteTime.String()
	s.minuteBooks[key] += len(e.Live.Books)
	if s.minuteReasons[key] == nil {
		s.minuteReasons[key] = map[string]int{}
		s.minuteFamilies[key] = map[string][3]int{}
		s.minuteValidSeconds[key] = map[int]int{}
	}
	symbols := map[uint32]string{}
	for _, r := range s.runs {
		if r.ID == e.Live.RunID {
			for _, m := range r.Members {
				symbols[m.InstrumentID] = m.Symbol
			}
			break
		}
	}
	for _, b := range e.Live.Books {
		validSeconds := 0
		for _, q := range b.Quality {
			if q.ReplayValid {
				validSeconds++
			}
		}
		s.minuteValidSeconds[key][validSeconds]++
		family := symbols[b.InstrumentID]
		if n := strings.IndexByte(family, '-'); n >= 0 {
			family = family[:n]
		}
		counts := s.minuteFamilies[key][family]
		counts[0]++
		if b.Quality[59].ReplayValid {
			counts[1]++
		}
		if validSeconds == 60 {
			counts[2]++
		}
		s.minuteFamilies[key][family] = counts
		q := b.Quality[59]
		reason := fmt.Sprintf("reason=%d known=%t open=%t stream=%t sampled=%t", q.Reason, q.MarketKnown, q.MarketOpen, q.StreamValid, q.Sampled)
		s.minuteReasons[key][reason]++
		if _, ok := s.qualityExample[reason]; !ok {
			s.qualityExample[reason] = fmt.Sprintf("%+v", q)
		}
		if b.Quality[59].ReplayValid {
			s.minuteValid[key]++
		}
	}
	if s.signal != nil && s.maxBooks >= 3000 && s.minuteBooks[key] >= s.maxBooks {
		select {
		case s.signal <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
	return nil
}

func (*catalogMemorySink) WriteOptionsCatalog(_ context.Context, o options.CatalogObservation) error {
	return o.Validate()
}
func (*catalogMemorySink) WriteOptionsLifecycle(_ context.Context, o options.LifecycleObservation) error {
	return o.Validate()
}
func (s *catalogMemorySink) WriteOptionsPlan(_ context.Context, p options.CollectionPlan) error {
	if e := p.Validate(); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans = append(s.plans, p)
	if len(p.InstrumentIDs) > s.maxBooks {
		s.maxBooks = len(p.InstrumentIDs)
	}
	return nil
}
func (s *catalogMemorySink) LatestOptionsPlan(context.Context) (options.CollectionPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.plans) == 0 {
		return options.CollectionPlan{}, fakeNotFound
	}
	return s.plans[len(s.plans)-1], nil
}
func (*catalogMemorySink) OptionsStoreIdentity() string { return "test" }
func (*catalogMemorySink) OptionsNotFound() error       { return fakeNotFound }
func supervisorFixture(t *testing.T) (*catalogSupervisor, *catalogWorker, *catalogEntry, time.Time) {
	t.Helper()
	e, at, _, epoch := catalogFixture(t)
	spec := e.specs[1]
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &catalogWorker{id: uuid.New(), q: newIngress(time.Now()), run: e.run, specs: []options.ContractSpec{spec}, done: make(chan struct{}), cancel: func() {}}
	entry := &catalogEntry{item: deribit.ParsedInstrument{Spec: spec}, scope: deribit.Scope{Currency: "BTC", Kind: "option"}, state: e.catalog.states[1], admitted: true}
	cfg := Config{RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2"}.catalogDefaults()
	s := &catalogSupervisor{ctx: ctx, cfg: cfg, sink: &catalogMemorySink{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), session: uuid.New(), activeEpoch: epoch, epoch: epoch, gate: e.catalog.gate, workers: []*catalogWorker{w}, byID: map[uint32]*catalogWorker{1: w}, entries: map[string]*catalogEntry{spec.Instrument.ExchangeSymbol: entry}, pendingStates: map[string]pendingState{}, orphanStates: map[string]options.LifecycleObservation{}, scopeGenerations: map[string]uint64{}, uncertainScopes: map[string]bool{}, uncertainSymbols: map[string]bool{}, protocolScopes: map[string]bool{}, attempted: map[string]time.Time{}, scopes: map[string]time.Time{}, inflight: map[string]*catalogJob{}, jobs: make(chan *catalogJob, 64), plans: make(chan catalogPlanResult, 1), commands: make(chan deribit.SessionCommand, 64), jobsBudget: &exchange.BufferBudget{Limit: 32 << 20}, budget: &exchange.BufferBudget{Limit: 64 << 20}, levels: &exchange.BufferBudget{Limit: 2_000_000}}
	s.hub = &bookHub{ctx: ctx, cfg: Config{MaxConnections: 2, ChannelsPerConnection: 256}, routes: map[uint32]*bookRoute{}, workers: map[uuid.UUID]*catalogWorker{w.id: w}, indexes: map[string]options.IndexSample{}, resetIndex: make(chan struct{}, 1), recovery: map[uint32]bool{}}
	return s, w, entry, at
}
func TestPlatformBarrierCannotBeReleasedByHeartbeat(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	s.generation.Add(1)
	s.broadcastGate(s.gate)
	v := <-w.q.queue
	w.q.consumed(v)
	if v.Gate.Known {
		t.Fatal("old known gate crossed pending platform barrier")
	}
	s.gate.Generation = s.generation.Load()
	s.broadcastGate(s.gate)
	v = <-w.q.queue
	w.q.consumed(v)
	if !v.Gate.Known {
		t.Fatal("durable same generation cannot resume")
	}
	s.gate.Locks = map[string]bool{"btc_usd": true}
	s.broadcastGate(s.gate)
	v = <-w.q.queue
	w.q.consumed(v)
	s.gate.Locks["btc_usd"] = false
	if !v.Gate.Locks["btc_usd"] {
		t.Fatal("published gate aliases mutable map")
	}
}
func TestStateBarrierBlocksOldCatalogPublication(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	s.pendingStates[e.item.Spec.Instrument.ExchangeSymbol] = pendingState{s.epoch, 9}
	s.publishEntry(e)
	v := <-w.q.queue
	w.q.consumed(v)
	if v.State.Known {
		t.Fatal("old catalog reopened pending halted state")
	}
	e.appliedSequence = 9
	s.publishEntry(e)
	v = <-w.q.queue
	w.q.consumed(v)
	if !v.State.Known {
		t.Fatal("durable same source sequence rejected")
	}
}
func TestFailedWorkerReplacedBeforeDrainAndFuturePlansBounded(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	at := time.Now().UTC().Truncate(time.Microsecond)
	expiry := at.Add(24 * time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	e.item.Spec.Instrument.VenueContractVersion = e.item.Spec.Version()
	w.specs[0] = e.item.Spec
	s.workerFailed(w.id)
	if !w.failed {
		t.Fatal("failed draining worker still eligible")
	}
	s.dirty = true
	s.buildPlan(at)
	select {
	case p := <-s.plans:
		if p.err != nil {
			t.Fatal(p.err)
		}
		if len(p.groups) != 1 || p.groups[0].worker != nil || p.groups[0].run.ID == w.run.ID {
			t.Fatal("failed run reused")
		}
		s.tail = &p.plan
	case <-time.After(time.Second):
		t.Fatal("replacement waited for drain")
	}
	if s.byID[e.item.Spec.Instrument.ID] != nil {
		t.Fatal("failed worker route retained")
	}
	s.planning = false
	for n := 0; n < 100; n++ {
		s.dirty = true
		s.buildPlan(at.Add(time.Duration(n) * time.Millisecond))
		if s.planning {
			t.Fatal("unbounded future plans")
		}
	}
}
func TestRetryKeepsIdentityAndByteReservation(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	j := &catalogJob{kind: "catalog", scope: deribit.Scope{Currency: "BTC", Kind: "option"}, raw: make([]byte, 1024)}
	if !s.enqueue(j) {
		t.Fatal("enqueue")
	}
	<-s.jobs
	before := s.jobsBudget.Used()
	s.published(catalogResult{j, errors.New("ambiguous write")})
	if s.inflight[j.scope.String()] == nil || len(s.retries) != 1 || s.retries[0].job != j || s.jobsBudget.Used() != before {
		t.Fatal("retry lost identity, dedupe or budget")
	}
	s.requestScope(j.scope)
	if len(s.jobs) != 0 {
		t.Fatal("duplicate scope started during retry")
	}
}
func TestCatalogProcessLockExclusive(t *testing.T) {
	id := uuid.NewString()
	release, e := acquireCatalogLock(id)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if second, e := acquireCatalogLock(id); e == nil {
		second()
		t.Fatal("second writer admitted")
	}
}

func TestCatalogStatusBurstIsCoalescedUntilDurable(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	s.generation.Store(1)
	s.requestStatus()
	for n := uint64(2); n <= 156; n++ {
		s.generation.Store(n)
		s.requestStatus()
	}
	if len(s.commands) != 1 {
		t.Fatalf("status burst queued %d requests", len(s.commands))
	}
	old := <-s.commands
	s.source(deribit.StreamEvent{Kind: "status", Epoch: s.epoch, Generation: old.Generation, ReceivedAt: time.Now().UTC()})
	if len(s.commands) != 1 {
		t.Fatal("stale ACK did not request the latest generation")
	}
	latest := <-s.commands
	if latest.Generation != 156 {
		t.Fatal("status request did not converge")
	}
	falseLock := options.LifecycleObservation{ID: uuid.New(), Kind: "platform", IndexID: "zec_usdc", Locked: new(bool), Epoch: s.epoch}
	s.published(catalogResult{job: &catalogJob{life: &falseLock, sourceGeneration: 156}})
	if !s.statusPending || len(s.commands) != 0 {
		t.Fatal("unrelated asset changed the single flight")
	}
	baseline := options.LifecycleObservation{ID: uuid.New(), Kind: "status", Epoch: s.epoch, LockMode: "false"}
	s.published(catalogResult{job: &catalogJob{life: &baseline, sourceGeneration: 156}})
	if s.statusPending || !s.gate.Known {
		t.Fatal("durable latest baseline did not release the single flight")
	}
}

func TestCatalogInvalidStatusAndDroppedStatusCanRetry(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	s.lifecycleReady = true
	s.requestStatus()
	cmd := <-s.commands
	s.source(deribit.StreamEvent{Kind: "status", Channel: "public/status", Epoch: s.epoch, Generation: cmd.Generation, Sequence: 7, ReceivedAt: time.Now().UTC(), Raw: []byte(`{"jsonrpc":"2.0","id":3,"result":{"locked":false}}`)})
	if s.statusPending || s.statusRetryAfter.IsZero() || !e.state.Known {
		t.Fatal("malformed status retained flight or erased unrelated instrument evidence")
	}
	s.statusRetryAfter = time.Now().Add(-time.Second)
	s.tick(time.Now())
	if !s.statusPending || len(s.commands) != 1 {
		t.Fatal("malformed status never retried")
	}
	cmd = <-s.commands
	j := &catalogJob{life: &options.LifecycleObservation{Kind: "status", Epoch: s.epoch}, sourceGeneration: cmd.Generation}
	s.dropRetry(j)
	if s.statusPending || s.statusRetryAfter.IsZero() {
		t.Fatal("discarded status retained its flight")
	}
	s.statusRetryAfter = time.Now().Add(-time.Second)
	s.tick(time.Now())
	if !s.statusPending || len(s.commands) != 1 {
		t.Fatal("discarded status never retried")
	}
}

func TestCatalogPlatformEvidenceDoesNotWaitForRESTQueue(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	s.lifeJobs = make(chan *catalogJob, 256)
	for len(s.jobs) < cap(s.jobs) {
		s.jobs <- &catalogJob{}
	}
	for n := 0; n < 156; n++ {
		j := &catalogJob{life: &options.LifecycleObservation{Kind: "platform"}, raw: []byte("public platform observation")}
		if !s.enqueue(j) {
			t.Fatal("initial public platform burst blocked by REST")
		}
	}
	if len(s.lifeJobs) != 156 || len(s.retries) != 0 {
		t.Fatal("control observations leaked into metadata retries")
	}
}

func TestCatalogSingleInstrumentFailuresUseBoundedBackoff(t *testing.T) {
	for attempt, delay := range []time.Duration{5 * time.Second, 15 * time.Second, time.Minute, time.Minute} {
		s, _, e, _ := supervisorFixture(t)
		before := time.Now()
		j := &catalogJob{scope: e.scope, kind: "instrument", symbol: e.item.Spec.Instrument.ExchangeSymbol, retryCount: attempt, observation: &options.CatalogObservation{Status: "request_error"}}
		s.published(catalogResult{job: j})
		if len(s.retries) != 1 || s.retries[0].job.retryCount != attempt+1 || s.retries[0].at.Before(before.Add(delay)) || s.retries[0].at.After(time.Now().Add(delay)) {
			t.Fatalf("attempt %d did not retain bounded delay %s", attempt, delay)
		}
	}
}

func TestCatalogFailedNewInstrumentKeepsHealthyScopeMembers(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	s.published(catalogResult{job: &catalogJob{scope: e.scope, kind: "instrument", symbol: "BTC-30OCT26-90000-C", observation: &options.CatalogObservation{Status: "request_error"}}})
	if !e.state.Known || len(s.retries) != 1 {
		t.Fatal("failed new instrument invalidated a healthy existing member")
	}
}

func TestCatalogSingleSuccessCannotReleaseFailedDirectoryScope(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	s.markScopeUnknown(e.scope)
	for len(w.q.queue) > 0 {
		v := <-w.q.queue
		w.q.consumed(v)
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	item := e.item
	item.State = "open"
	item.Active = true
	j := &catalogJob{scope: e.scope, symbol: item.Spec.Instrument.ExchangeSymbol, kind: "instrument", epoch: s.epoch, scopeGeneration: s.scopeGenerations[e.scope.String()], barrierCaptured: true, result: &deribit.ScopeResult{Instruments: []deribit.ParsedInstrument{item}}, observation: &options.CatalogObservation{ID: uuid.New(), Status: "complete", RequestedAt: observed, ObservedAt: observed}}
	s.published(catalogResult{job: j})
	if !s.uncertainScopes[e.scope.String()] || e.confirmedScopeGeneration != j.scopeGeneration {
		t.Fatal("one instrument released the entire failed directory scope")
	}
	other := *e
	other.item.Spec.Instrument.ID = 2
	other.item.Spec.Instrument.ExchangeSymbol = "BTC-30OCT26-90000-C"
	other.state.InstrumentID = 2
	other.confirmedScopeGeneration = 0
	s.byID[2] = w
	s.publishEntry(&other)
	unknown := false
	for len(w.q.queue) > 0 {
		v := <-w.q.queue
		w.q.consumed(v)
		if v.State != nil && v.State.InstrumentID == 2 {
			unknown = !v.State.Known
		}
	}
	if !unknown {
		t.Fatal("unconfirmed scope member became known")
	}
}

func TestNewUnpairedCreationPrewarmsWhenOldContractExpired(t *testing.T) {
	s, w, old, _ := supervisorFixture(t)
	past := time.Now().UTC().Add(-time.Hour)
	old.item.Spec.Instrument.ExpiryTime = &past
	old.admitted = false
	item := old.item
	expiry := time.Now().UTC().AddDate(1, 0, 0).Truncate(24 * time.Hour).Add(8 * time.Hour)
	item.Spec.Instrument.ID = 2
	item.Spec.NativeID++
	item.Spec.Instrument.ExpiryTime = &expiry
	item.Spec.Instrument.ExchangeSymbol = "BTC-" + strings.ToUpper(expiry.Format("02Jan06")) + "-" + item.Spec.Strike.String() + "-C"
	item.Spec.Instrument.VenueContractVersion = item.Spec.Version()
	item.Rule.InstrumentID = 2
	item.State = "open"
	item.Active = true
	observed := time.Now().UTC().Truncate(time.Microsecond)
	s.published(catalogResult{job: &catalogJob{kind: "creation", scope: old.scope, epoch: s.epoch, result: &deribit.ScopeResult{Instruments: []deribit.ParsedInstrument{item}}, observation: &options.CatalogObservation{ID: uuid.New(), Status: "complete", RequestedAt: observed, ObservedAt: observed}}})
	entry := s.entries[item.Spec.Instrument.ExchangeSymbol]
	if entry == nil || !entry.admitted || !entry.state.Known || !entry.state.Open || s.byID[2] != w || s.hub.routes[2] == nil {
		t.Fatal("new independent option did not start prewarming after old expiry")
	}
	if len(s.plans) != 0 || w.persisted {
		t.Fatal("prewarming incorrectly required a committed future plan")
	}
}

func TestReviewOldMaintenanceRetryCannotReopenCurrentMaintenance(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	s.maintenancePending = 6
	s.maintenancePublished = 6
	s.generation.Store(2)
	s.gate.Generation = 2
	s.gate.Maintenance = true
	s.gate.MaintenanceID = uuid.New()
	s.gate.Known = true
	oldFalse := false
	old := options.LifecycleObservation{ID: uuid.New(), Kind: "platform", Epoch: s.epoch, Sequence: 5, Maintenance: &oldFalse}
	s.published(catalogResult{job: &catalogJob{life: &old, sourceGeneration: 1}})
	baseline := options.LifecycleObservation{ID: uuid.New(), Kind: "status", Epoch: s.epoch, Sequence: 8, LockMode: "false"}
	s.published(catalogResult{job: &catalogJob{life: &baseline, sourceGeneration: 2}})
	if !s.gate.Maintenance {
		t.Fatal("older maintenance=false retry overwrote newer durable maintenance=true")
	}
}

func TestUnknownSymbolCannotResumeFromPlatformOrCachedCatalog(t *testing.T) {
	s, w, e, _ := supervisorFixture(t)
	name := e.item.Spec.Instrument.ExchangeSymbol
	s.uncertainSymbols[name] = true
	e.state.StateKind = 1
	e.state.Known = false
	s.pendingStates[name] = pendingState{s.epoch, 5}
	e.appliedSequence = 5
	item := e.item
	item.State = "open"
	item.Active = true
	scope := e.scope
	observed := time.Now().UTC().Truncate(time.Microsecond)
	publish := func(kind string, seq uint64) {
		o := options.CatalogObservation{ID: uuid.New(), Status: "complete", ObservedAt: observed, RequestedAt: observed}
		r := deribit.ScopeResult{Instruments: []deribit.ParsedInstrument{item}}
		s.published(catalogResult{job: &catalogJob{kind: kind, scope: scope, epoch: s.epoch, result: &r, observation: &o, barrierSequence: seq}})
	}
	publish("catalog", 0)
	s.publishEntry(e)
	known := false
	for len(w.q.queue) > 0 {
		v := <-w.q.queue
		w.q.consumed(v)
		if v.State != nil {
			known = v.State.Known
		}
	}
	if known {
		t.Fatal("cached catalog cleared symbol protocol uncertainty")
	}
	publish("instrument", 5)
	s.publishEntry(e)
	for len(w.q.queue) > 0 {
		v := <-w.q.queue
		w.q.consumed(v)
		if v.State != nil {
			known = v.State.Known
		}
	}
	if !known || s.uncertainSymbols[name] {
		t.Fatal("fresh matching single confirmation failed to recover")
	}
}
func TestOldMetadataCannotReplaceNewDefinition(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	e.state.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	e.latestKind = "creation"
	old := e.item
	old.Spec.NativeID++
	old.Spec.Instrument.VenueContractVersion = old.Spec.Version()
	r := deribit.ScopeResult{Instruments: []deribit.ParsedInstrument{old}}
	o := options.CatalogObservation{ID: uuid.New(), Status: "complete", ObservedAt: e.state.ObservedAt.Add(-time.Second)}
	wanted := e.item.Spec.DefinitionHash()
	s.published(catalogResult{job: &catalogJob{kind: "catalog", scope: e.scope, epoch: s.epoch, result: &r, observation: &o}})
	if e.item.Spec.DefinitionHash() != wanted {
		t.Fatal("older retry reverted definition")
	}
}
func TestRetryCapacityReleasesSingleFlight(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	expiry := time.Now().Add(time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	for n := 0; n < s.evidenceQueueLimit(); n++ {
		s.retries = append(s.retries, scheduledCatalogJob{})
	}
	j := &catalogJob{kind: "instrument", scope: e.scope, symbol: e.item.Spec.Instrument.ExchangeSymbol}
	s.inflight[j.scope.String()+":"+j.symbol] = j
	s.scheduleRetry(j, time.Second)
	if s.inflight[j.scope.String()+":"+j.symbol] != nil {
		t.Fatal("dropped retry leaves permanent occupancy")
	}
	s.requestInstrument(j.scope, j.symbol)
	if len(s.jobs) != 1 {
		t.Fatal("source cannot retry after capacity refusal")
	}
}
func TestPartialPhysicalRecoveryUsesOneUnsubscribe(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	h := s.hub
	// Keep one spare book connection for partial migration. Exhausted epochs
	// are covered separately by the bounded rotation regression.
	h.cfg.MaxConnections = 4
	p := &bookPhysical{epoch: uuid.New(), routes: map[string]*bookRoute{}, used: map[string]bool{}, requested: map[string]bool{}, acked: map[string]bool{}, commands: make(chan deribit.SessionCommand, 64), cancel: func() {}}
	h.physical = []*bookPhysical{p}
	for n := uint32(1); n <= 128; n++ {
		symbol := fmt.Sprintf("BTC-short-%d", n)
		ch := "book." + symbol + ".100ms"
		r := &bookRoute{id: n, symbol: symbol, worker: w, physical: p}
		p.routes[ch] = r
		p.used[ch] = true
		p.requested[ch] = true
		p.acked[ch] = true
		h.routes[n] = r
		if n <= 64 {
			h.recovery[n] = true
		}
	}
	h.recoverPending()
	if len(p.commands) != 1 {
		t.Fatalf("requests=%d", len(p.commands))
	}
	cmd := <-p.commands
	if len(cmd.Channels) != 64 || cmd.Method != "public/unsubscribe" {
		t.Fatal("not batched")
	}
}

type ambiguousCatalogSink struct {
	catalogMemorySink
	failedRead bool
	writes     int
}

func (s *ambiguousCatalogSink) WriteOptionsPlan(ctx context.Context, p options.CollectionPlan) error {
	s.writes++
	if s.writes == 1 {
		if err := s.catalogMemorySink.WriteOptionsPlan(ctx, p); err != nil {
			return err
		}
		s.failedRead = true
		return errors.New("committed, reply lost")
	}
	return nil
}
func (s *ambiguousCatalogSink) LatestOptionsPlan(ctx context.Context) (options.CollectionPlan, error) {
	if s.failedRead {
		s.failedRead = false
		return options.CollectionPlan{}, errors.New("temporarily unavailable")
	}
	return s.catalogMemorySink.LatestOptionsPlan(ctx)
}
func TestAmbiguousPlanKeepsSameIdentityUntilResolved(t *testing.T) {
	s, _, _, _ := supervisorFixture(t)
	sink := &ambiguousCatalogSink{}
	s.sink = sink
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := options.CollectionPlan{ID: uuid.New(), SessionID: s.session, Revision: 1, CreatedAt: now, EffectiveMinute: now.Truncate(time.Minute).Add(2 * time.Minute), ConfigHash: options.PayloadHash([]byte("config"))}
	candidate := catalogPlanResult{plan: p}
	s.submitPlan(candidate)
	result := <-s.plans
	s.planned(result)
	if s.unresolved == nil || s.unresolved.plan.ID != p.ID {
		t.Fatal("unresolved identity discarded")
	}
	s.submitPlan(*s.unresolved)
	result = <-s.plans
	s.planned(result)
	if s.unresolved != nil || s.tail == nil || s.tail.ID != p.ID || len(sink.plans) != 1 {
		t.Fatal("ambiguous plan did not converge")
	}
}

func TestMalformedPlatformCannotResumeFromStatus(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	s.platformUncertain = true
	s.gate.Known = true
	s.publishGate(w, s.gate)
	v := <-w.q.queue
	w.q.consumed(v)
	if v.Gate.Known {
		t.Fatal("status cannot prove malformed maintenance field")
	}
}
func TestScopeRecoveryCapacityDoesNotInvalidateIssuedConfirmations(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	s.markScopeUnknown(e.scope)
	s.protocolScopes[e.scope.String()] = true
	generation := s.scopeGenerations[e.scope.String()]
	for n := 0; n < 256; n++ {
		s.requestInstrument(e.scope, fmt.Sprintf("BTC-30OCT26-%d-C", 80000+n))
	}
	if s.scopeGenerations[e.scope.String()] != generation {
		t.Fatal("capacity refusal invalidates already issued confirmations")
	}
	if len(s.jobs) > cap(s.jobs) || len(s.retries) > s.evidenceQueueLimit() {
		t.Fatal("unbounded confirmation work")
	}
}

func TestOverlayReviewQueuedMaintenanceCannotClearLaterMalformedPlatform(t *testing.T) {
	s, w, _, _ := supervisorFixture(t)
	s.maintenancePending = 5
	s.generation.Store(2)
	f := deribit.StreamEvent{Kind: "message", Channel: "platform_state", Epoch: s.epoch, Sequence: 6, Generation: 2, ReceivedAt: time.Now().UTC().Truncate(time.Microsecond), Raw: []byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"platform_state","data":{"maintenance":"true"}}}`)}
	s.source(f)
	oldFalse := false
	old := options.LifecycleObservation{ID: uuid.New(), Kind: "platform", Epoch: s.epoch, Sequence: 5, Maintenance: &oldFalse}
	s.published(catalogResult{job: &catalogJob{life: &old, sourceGeneration: 1}})
	baseline := options.LifecycleObservation{ID: uuid.New(), Kind: "status", Epoch: s.epoch, Sequence: 8, LockMode: "false"}
	s.published(catalogResult{job: &catalogJob{life: &baseline, sourceGeneration: s.generation.Load()}})
	known := false
	for len(w.q.queue) > 0 {
		v := <-w.q.queue
		w.q.consumed(v)
		if v.Gate != nil {
			known = v.Gate.Known
		}
	}
	if known {
		t.Fatal("older queued maintenance=false cleared later malformed platform barrier")
	}
}
