package optionslive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type CatalogSink interface {
	Sink
	WriteDerivativeTradingRules(context.Context, []options.TradingRule) error
	InitOptionsCatalogSchema(context.Context) error
	WriteOptionsCatalog(context.Context, options.CatalogObservation) error
	WriteOptionsLifecycle(context.Context, options.LifecycleObservation) error
	WriteOptionsPlan(context.Context, options.CollectionPlan) error
	LatestOptionsPlan(context.Context) (options.CollectionPlan, error)
	WriteOptionsCatalogMinute(context.Context, options.CatalogEnvelope) error
	OptionsStoreIdentity() string
	OptionsNotFound() error
}
type catalogEntry struct {
	latestKind               string
	confirmedScopeGeneration uint64
	appliedSequence          uint64
	item                     deribit.ParsedInstrument
	scope                    deribit.Scope
	state                    catalogState
	marketState              string
	lastEvent                time.Time
	admitted                 bool
}
type catalogJob struct {
	barrierCaptured                  bool
	retryCount                       int
	barrierSequence, scopeGeneration uint64
	reserved                         int64
	scope                            deribit.Scope
	symbol, kind                     string
	epoch                            uuid.UUID
	result                           *deribit.ScopeResult
	observation                      *options.CatalogObservation
	life                             *options.LifecycleObservation
	raw                              []byte
	sourceGeneration                 uint64
	retry                            bool
}
type catalogResult struct {
	job *catalogJob
	err error
}
type catalogPlanResult struct {
	absent bool
	plan   options.CollectionPlan
	groups []catalogGroup
	err    error
}
type catalogGroup struct {
	worker *catalogWorker
	run    options.LiveRun
	specs  []options.ContractSpec
	retire bool
}
type pendingState struct {
	epoch    uuid.UUID
	sequence uint64
}
type catalogSupervisor struct {
	clock                                    func() time.Time
	statusPending                            bool
	statusGeneration                         uint64
	statusRetryAfter                         time.Time
	platformUncertain                        bool
	maintenancePending, maintenancePublished uint64
	unresolved                               *catalogPlanResult
	resolveAfter                             time.Time
	uncertainSymbols                         map[string]bool
	protocolScopes                           map[string]bool
	scopeGenerations                         map[string]uint64
	uncertainScopes                          map[string]bool
	orphanStates                             map[string]options.LifecycleObservation
	lifecycleReady                           bool
	attempted                                map[string]time.Time
	activeEpoch                              uuid.UUID
	pendingStates                            map[string]pendingState
	ctx                                      context.Context
	cfg                                      Config
	c                                        *deribit.Client
	sink                                     CatalogSink
	logger                                   *slog.Logger
	hub                                      *bookHub
	budget, levels, jobsBudget               *exchange.BufferBudget
	minuteCache                              *catalogMinuteCache
	mu                                       sync.Mutex // protects immediate invalidation routes, never held over database I/O
	workers                                  []*catalogWorker
	byID                                     map[uint32]*catalogWorker
	entries                                  map[string]*catalogEntry
	gate                                     catalogGate
	epoch                                    uuid.UUID
	generation                               atomic.Uint64
	incoming                                 chan deribit.StreamEvent
	jobs                                     chan *catalogJob
	scopeJobs                                chan *catalogJob
	lifeJobs                                 chan *catalogJob
	persisted                                chan catalogResult
	commands                                 chan deribit.SessionCommand
	failed                                   chan uuid.UUID
	plans                                    chan catalogPlanResult
	session                                  uuid.UUID
	tail                                     *options.CollectionPlan
	planning, dirty                          bool
	scopes                                   map[string]time.Time
	inflight                                 map[string]*catalogJob
	retries                                  []scheduledCatalogJob
	lastHealth                               time.Time
}

func (s *catalogSupervisor) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

type scheduledCatalogJob struct {
	at      time.Time
	job     *catalogJob
	attempt int
}

func catalogScopes() []deribit.Scope {
	var ss []deribit.Scope
	for _, currency := range []string{"BTC", "ETH", "USDC"} {
		for _, kind := range []string{"option", "future"} {
			ss = append(ss, deribit.Scope{Currency: currency, Kind: kind})
		}
	}
	return ss
}
func RunCatalog(ctx context.Context, cfg Config, sink CatalogSink, logger *slog.Logger) error {
	cfg = cfg.catalogDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}
	unlock, err := acquireCatalogLock(sink.OptionsStoreIdentity())
	if err != nil {
		return err
	}
	defer unlock()
	if err = sink.InitOptionsCatalogSchema(ctx); err != nil {
		return err
	}
	c := deribit.NewClient(cfg.RESTURL, cfg.WSURL)
	c.ControlGate = exchange.NewRequestGate(100 * time.Millisecond)
	c.SubscriptionGate = exchange.NewRequestGate(time.Second)
	c.FrameBudget = &exchange.BufferBudget{Limit: cfg.MaxIngressBytes}
	return runCatalogClient(ctx, cfg, c, sink, logger)
}
func runCatalogClient(parent context.Context, cfg Config, c *deribit.Client, sink CatalogSink, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s := &catalogSupervisor{ctx: ctx, cfg: cfg, c: c, sink: sink, logger: logger, budget: &exchange.BufferBudget{Limit: cfg.MaxIngressBytes}, levels: &exchange.BufferBudget{Limit: cfg.MaxTotalLevels}, jobsBudget: &exchange.BufferBudget{Limit: 32 << 20}, byID: map[uint32]*catalogWorker{}, pendingStates: map[string]pendingState{}, orphanStates: map[string]options.LifecycleObservation{}, scopeGenerations: map[string]uint64{}, uncertainScopes: map[string]bool{}, uncertainSymbols: map[string]bool{}, protocolScopes: map[string]bool{}, attempted: map[string]time.Time{}, entries: map[string]*catalogEntry{}, incoming: make(chan deribit.StreamEvent, 128), jobs: make(chan *catalogJob, 64), persisted: make(chan catalogResult, 128), commands: make(chan deribit.SessionCommand, 64), failed: make(chan uuid.UUID, 256), plans: make(chan catalogPlanResult, 1), session: uuid.New(), scopes: map[string]time.Time{}, inflight: map[string]*catalogJob{}}
	// Six periodic scope refreshes must not wait behind thousands of per-symbol
	// repairs. Both queues still share one REST worker and the same rate gate.
	s.scopeJobs = make(chan *catalogJob, 16)
	tail, err := sink.LatestOptionsPlan(ctx)
	if err == nil {
		s.tail = &tail
	} else if !errors.Is(err, sink.OptionsNotFound()) {
		return err
	}
	// Reading and queued book events share one task-wide byte allowance.
	// A decoded frame transfers its reservation to the destination ingress.
	c.FrameBudget = s.budget
	s.minuteCache = newCatalogMinuteCache(2 * cfg.MaxIngressBytes)
	s.hub = newBookHub(ctx, c, cfg, logger)
	var wg sync.WaitGroup
	s.lifeJobs = make(chan *catalogJob, s.evidenceQueueLimit())
	wg.Add(3)
	go func() { defer wg.Done(); s.persistLoop(s.jobs) }()
	go func() { defer wg.Done(); s.persistLifecycleLoop() }()
	go func() { defer wg.Done(); s.lifecycleLoop() }()
	defer func() {
		cancel()
		wg.Wait()
		s.hub.wait()
		s.mu.Lock()
		ws := slices.Clone(s.workers)
		s.mu.Unlock()
		for _, w := range ws {
			w.cancel()
		}
		waitCatalogWorkers(ws)
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case f := <-s.incoming:
			s.jobsBudget.Release(int64(len(f.Raw) + 512))
			s.source(f)
		case r := <-s.persisted:
			s.published(r)
		case p := <-s.plans:
			s.planned(p)
		case id := <-s.failed:
			s.workerFailed(id)
		case <-tick.C:
			s.tick(s.now().UTC())
		}
	}
}

func (s *catalogSupervisor) broadcastGate(g catalogGate) {
	g = cloneCatalogGate(g)
	s.mu.Lock()
	if g.Epoch != s.activeEpoch || g.Generation != s.generation.Load() || s.maintenancePending != s.maintenancePublished || s.platformUncertain {
		g.Known = false
	}
	defer s.mu.Unlock()
	for _, w := range s.workers {
		_ = w.q.offer(event{Gate: &g})
	}
}
func (s *catalogSupervisor) invalidateAll() { s.broadcastGate(catalogGate{Known: false}) }
func (s *catalogSupervisor) invalidateSymbol(symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		for _, spec := range w.specs {
			if s.byID[spec.Instrument.ID] == w && spec.Instrument.ExchangeSymbol == symbol {
				x := catalogState{InstrumentID: spec.Instrument.ID}
				_ = w.q.offer(event{State: &x})
				return
			}
		}
	}
}
func (s *catalogSupervisor) lifecycleLoop() {
	channels := []string{"platform_state"}
	for _, scope := range catalogScopes() {
		channels = append(channels, "instrument.creation."+scope.Kind+"."+scope.Currency, "instrument.state."+scope.Kind+"."+scope.Currency)
	}
	for s.ctx.Err() == nil {
		err := s.c.Session(s.ctx, channels, s.commands, func(f deribit.StreamEvent) error {
			if f.ReceivedAt.IsZero() {
				f.ReceivedAt = s.now().UTC().Truncate(time.Microsecond)
			}
			if f.Kind == "connected" || f.Kind == "disconnected" {
				s.mu.Lock()
				if f.Kind == "connected" {
					s.activeEpoch = f.Epoch
					clear(s.pendingStates)
					s.maintenancePending = 0
					s.maintenancePublished = 0
				} else {
					s.activeEpoch = uuid.Nil
				}
				s.mu.Unlock()
				f.Generation = s.generation.Add(1)
				s.invalidateAll()
			}
			if f.Kind == "message" && f.Channel == "platform_state" {
				o, e := deribit.DecodeLifecycle(f, f.ReceivedAt)
				f.Generation = s.generation.Load()
				if e != nil || affectsCatalogPlatform(o) {
					f.Generation = s.generation.Add(1)
					s.invalidateAll()
				}
				if e != nil {
					s.mu.Lock()
					s.platformUncertain = true
					if f.Sequence > s.maintenancePending {
						s.maintenancePending = f.Sequence
					}
					s.mu.Unlock()
				}
				if e == nil && o.Maintenance != nil {
					s.mu.Lock()
					if f.Sequence > s.maintenancePending {
						s.maintenancePending = f.Sequence
					}
					s.mu.Unlock()
				}
			}
			if f.Kind == "message" && strings.HasPrefix(f.Channel, "instrument.state.") {
				o, e := deribit.DecodeLifecycle(f, f.ReceivedAt)
				if e != nil {
					symbol := extractLifecycleSymbol(f.Raw)
					if symbol != "" {
						if err := s.acceptPendingState(f, symbol); err != nil {
							s.invalidateAll()
							return err
						}
					}
					s.markProtocolUnknown(channelScope(f.Channel), symbol)
					s.invalidateAll()
				} else {
					if err := s.acceptPendingState(f, o.Symbol); err != nil {
						s.invalidateAll()
						return err
					}
					s.invalidateSymbol(o.Symbol)
				}
			}
			if !s.jobsBudget.Reserve(int64(len(f.Raw) + 512)) {
				s.invalidateAll()
				return fmt.Errorf("lifecycle ingress byte budget exceeded")
			}
			select {
			case s.incoming <- f:
				return nil
			case <-s.ctx.Done():
				s.jobsBudget.Release(int64(len(f.Raw) + 512))
				return s.ctx.Err()
			default:
				s.jobsBudget.Release(int64(len(f.Raw) + 512))
				s.invalidateAll()
				return fmt.Errorf("lifecycle ingress full")
			}
		})
		if s.ctx.Err() != nil {
			return
		}
		s.logger.Warn("options lifecycle reconnect", "error", err)
		if !exchange.Wait(s.ctx, time.Second) {
			return
		}
	}
}

func (s *catalogSupervisor) acceptPendingState(f deribit.StreamEvent, symbol string) error {
	if _, err := symbolScope(symbol); err != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pendingStates[symbol]; !exists && len(s.pendingStates) >= s.evidenceQueueLimit() {
		return fmt.Errorf("lifecycle symbol recovery budget exceeded")
	}
	s.pendingStates[symbol] = pendingState{f.Epoch, f.Sequence}
	return nil
}
func (s *catalogSupervisor) enqueue(j *catalogJob) bool {
	queue := s.jobs
	if j.life != nil && s.lifeJobs != nil {
		queue = s.lifeJobs
	} else if j.life == nil && j.kind == "catalog" && s.scopeJobs != nil {
		queue = s.scopeJobs
	}
	if !j.barrierCaptured && j.life == nil {
		j.barrierCaptured = true
		s.mu.Lock()
		j.barrierSequence = s.pendingStates[j.symbol].sequence
		j.scopeGeneration = s.scopeGenerations[j.scope.String()]
		s.mu.Unlock()
	}
	if j.reserved > 0 {
		select {
		case queue <- j:
			return true
		default:
			return false
		}
	}
	cost := int64(len(j.raw) + 512)
	if j.result != nil {
		cost += int64(len(j.result.Raw))
	}
	if !s.jobsBudget.Reserve(cost) {
		return false
	}
	j.reserved = cost
	select {
	case queue <- j:
		return true
	default:
		s.jobsBudget.Release(cost)
		j.reserved = 0
		return false
	}
}
func (s *catalogSupervisor) persistLoop(queue <-chan *catalogJob) {
	for {
		j, ok := nextCatalogJob(s.ctx, s.scopeJobs, queue)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		err := s.persist(ctx, j)
		cancel()
		select {
		case s.persisted <- catalogResult{j, err}:
		case <-s.ctx.Done():
			return
		}
	}
}

func nextCatalogJob(ctx context.Context, scopes, ordinary <-chan *catalogJob) (*catalogJob, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	select {
	case j := <-scopes:
		return j, true
	default:
	}
	select {
	case <-ctx.Done():
		return nil, false
	case j := <-scopes:
		return j, true
	case j := <-ordinary:
		return j, true
	}
}
func (s *catalogSupervisor) persist(ctx context.Context, j *catalogJob) error {
	if j.life != nil {
		if err := validateCatalogPayload(j.raw, j.life.PayloadHash); err != nil {
			return err
		}
		return s.sink.WriteOptionsLifecycle(ctx, *j.life)
	}
	if j.result == nil {
		// Capture the barrier when the HTTP request actually starts, rather
		// than when it waits behind other requests in the metadata queue.
		s.mu.Lock()
		j.barrierSequence = s.pendingStates[j.symbol].sequence
		j.scopeGeneration = s.scopeGenerations[j.scope.String()]
		j.barrierCaptured = true
		s.mu.Unlock()
		var r deribit.ScopeResult
		var err error
		if j.symbol != "" {
			r, err = s.c.FetchInstrument(ctx, j.scope, j.symbol)
		} else {
			r, err = s.c.FetchScope(ctx, j.scope)
		}
		if r.RequestedAt.IsZero() {
			return err
		}
		if !s.jobsBudget.Reserve(int64(len(r.Raw))) {
			return fmt.Errorf("catalog evidence byte budget exceeded")
		}
		j.reserved += int64(len(r.Raw))
		j.result = &r
		if r.Status != "complete" {
			if j.symbol != "" {
				s.markProtocolUnknown(j.scope, j.symbol)
			} else {
				s.markScopeUnknown(j.scope)
			}
		}
	}
	r := j.result
	if err := validateCatalogPayload(r.Raw, r.PayloadHash); err != nil {
		return err
	}
	if j.observation == nil {
		o := options.CatalogObservation{ID: r.RequestID, Scope: r.Scope.String(), Kind: j.kind, URL: r.URL, RequestedAt: r.RequestedAt, ObservedAt: r.ObservedAt, PayloadHash: r.PayloadHash, Status: r.Status}
		if r.Status == "complete" {
			specs := make([]options.ContractSpec, len(r.Instruments))
			for n, p := range r.Instruments {
				specs[n] = p.Spec
			}
			if len(specs) > 0 {
				registered, err := s.sink.RegisterDerivativeSpecs(ctx, specs)
				if err != nil {
					return err
				}
				for n, spec := range registered {
					r.Instruments[n].Spec = spec
					r.Instruments[n].Rule.InstrumentID = spec.Instrument.ID
				}
			}
			rules := make([]options.TradingRule, len(r.Instruments))
			for n, p := range r.Instruments {
				rules[n] = p.Rule
			}
			if err := s.sink.WriteDerivativeTradingRules(ctx, rules); err != nil {
				return err
			}
			for _, p := range r.Instruments {
				o.NativeIDs = append(o.NativeIDs, p.Spec.NativeID)
				o.Symbols = append(o.Symbols, p.Spec.Instrument.ExchangeSymbol)
				o.InstrumentIDs = append(o.InstrumentIDs, p.Spec.Instrument.ID)
				o.Definitions = append(o.Definitions, p.Spec.DefinitionHash())
				o.Rules = append(o.Rules, p.Rule.ID())
				o.States = append(o.States, p.State)
				o.Active = append(o.Active, p.Active)
			}
			o.RawCount = r.RawCount
			for _, x := range r.Excluded {
				o.ExcludedSymbols = append(o.ExcludedSymbols, x.Symbol)
				o.ExcludedReasons = append(o.ExcludedReasons, x.Reason)
			}
		}
		j.observation = &o
	}
	return s.sink.WriteOptionsCatalog(ctx, *j.observation)
}
func (s *catalogSupervisor) source(f deribit.StreamEvent) {
	if f.Kind == "connected" {
		s.statusPending = false
		s.statusRetryAfter = time.Time{}
		s.epoch = f.Epoch
		s.lifecycleReady = false
		maintenance := s.gate.Maintenance
		s.gate = catalogGate{Maintenance: maintenance, Epoch: f.Epoch, LockIDs: map[string]uuid.UUID{}, Locks: map[string]bool{}}

		for _, e := range s.entries {
			e.state.Known = false
			e.appliedSequence = 0
			e.lastEvent = time.Time{}
		}
		return
	}
	if f.Epoch != s.epoch {
		return
	}
	if f.Kind == "disconnected" {
		s.statusPending = false
		s.gate.Known = false
		s.broadcastGate(s.gate)
		return
	}
	s.gate.ConfirmedAt = f.ReceivedAt
	if f.Kind == "confirm" || f.Kind == "ready" {
		s.broadcastGate(cloneCatalogGate(s.gate))
	}
	if f.Kind == "ready" {
		s.lifecycleReady = true
		s.requestStatus()
		for _, scope := range catalogScopes() {
			s.requestScope(scope)
		}
		return
	}
	if f.Kind != "message" && f.Kind != "status" {
		return
	}
	if strings.HasPrefix(f.Channel, "instrument.creation.") {
		r, err := deribit.DecodeCreation(f, f.ReceivedAt)
		r.URL = s.cfg.WSURL + "#" + f.Channel
		for n := range r.Instruments {
			r.Instruments[n].Rule.SourceURL = r.URL
		}
		if err != nil {
			scope := r.Scope
			var frame struct {
				Params struct {
					Data struct {
						Symbol string `json:"instrument_name"`
					} `json:"data"`
				} `json:"params"`
			}
			_ = json.Unmarshal(f.Raw, &frame)
			if frame.Params.Data.Symbol != "" {
				s.requestInstrument(scope, frame.Params.Data.Symbol)
			} else {
				s.requestScope(scope)
			}
			return
		}
		for _, item := range r.Instruments {
			name := item.Spec.Instrument.ExchangeSymbol
			if e := s.entries[name]; e != nil && e.item.Spec.DefinitionHash() != item.Spec.DefinitionHash() {
				s.markProtocolUnknown(r.Scope, name)
			}
		}
		// Creation supplies definitions, not confirmation of a later state
		// frame already accepted by the reader. Never borrow its pending ID.
		j := &catalogJob{scope: r.Scope, kind: "creation", epoch: f.Epoch, result: &r, barrierCaptured: true}
		s.mu.Lock()
		j.scopeGeneration = s.scopeGenerations[r.Scope.String()]
		s.mu.Unlock()
		if !s.enqueue(j) {
			s.scheduleRetry(j, time.Second)
		}
		return
	}
	if f.Kind == "status" && f.Generation != s.generation.Load() {
		s.finishStatus(f.Generation)
		s.requestStatus()
		return
	}
	o, err := deribit.DecodeLifecycle(f, f.ReceivedAt)
	if err != nil {
		if f.Kind == "status" {
			s.finishStatus(f.Generation)
			s.statusRetryAfter = s.now().Add(time.Second)
		}
		scope := channelScope(f.Channel)
		if f.Channel == "platform_state" {
			s.mu.Lock()
			s.platformUncertain = true
			if f.Sequence > s.maintenancePending {
				s.maintenancePending = f.Sequence
			}
			s.mu.Unlock()
		}
		if scope.Currency != "" {
			s.markProtocolUnknown(scope, extractLifecycleSymbol(f.Raw))
		}
		failure := options.LifecycleObservation{ID: uuid.New(), Kind: "error", Channel: f.Channel, Epoch: f.Epoch, Sequence: f.Sequence, ReceivedAt: f.ReceivedAt, PayloadHash: options.PayloadHash(f.Raw)}
		job := &catalogJob{life: &failure, raw: f.Raw, epoch: f.Epoch}
		if !s.enqueue(job) {
			s.scheduleRetry(job, time.Second)
		}
		s.logger.Error("invalid options lifecycle frame", "channel", f.Channel, "error", err)
		if scope.Currency != "" {
			if symbol := extractLifecycleSymbol(f.Raw); symbol != "" {
				s.requestInstrument(scope, symbol)
			} else {
				s.requestScope(scope)
			}
		} else {
			s.generation.Add(1)
			s.gate.Known = false
			s.invalidateAll()
			s.requestStatus()
		}
		return
	}
	j := &catalogJob{life: &o, raw: f.Raw, epoch: f.Epoch, sourceGeneration: f.Generation}
	if !s.enqueue(j) {
		s.scheduleRetry(j, time.Second)
	}
}
func (s *catalogSupervisor) requestStatus() {
	if s.statusPending || s.now().Before(s.statusRetryAfter) {
		return
	}
	generation := s.generation.Load()
	select {
	case s.commands <- deribit.SessionCommand{Method: "public/status", Generation: generation}:
		s.statusPending = true
		s.statusGeneration = generation
		s.statusRetryAfter = time.Time{}
	default:
		s.statusRetryAfter = s.now().Add(time.Second)
		s.gate.Known = false
		s.broadcastGate(s.gate)
	}
}
func (s *catalogSupervisor) finishStatus(generation uint64) {
	if s.statusPending && s.statusGeneration == generation {
		s.statusPending = false
	}
}
func (s *catalogSupervisor) requestScope(scope deribit.Scope) {
	key := scope.String()
	if s.inflight[key] != nil {
		return
	}
	j := &catalogJob{scope: scope, kind: "catalog", epoch: s.epoch}
	if s.enqueue(j) {
		s.attempted[key] = s.now()
		s.inflight[key] = j
	}
}
func (s *catalogSupervisor) requestInstrument(scope deribit.Scope, symbol string) {
	if _, err := symbolScope(symbol); err != nil {
		return
	}
	key := scope.String() + ":" + symbol
	if s.inflight[key] != nil {
		return
	}
	now := s.now()
	if e := s.entries[symbol]; e != nil && (isTerminal(e.marketState) || !e.item.Spec.Instrument.ExpiryTime.After(now)) {
		return
	}
	if now.Sub(s.attempted[key]) < 5*time.Second {
		return
	}
	s.attempted[key] = now
	j := &catalogJob{scope: scope, symbol: symbol, kind: "instrument", epoch: s.epoch}
	if s.enqueue(j) {
		s.inflight[key] = j
	} else {
		s.scheduleRetry(j, 5*time.Second)
	}
}
func cloneCatalogGate(g catalogGate) catalogGate {
	out := g
	out.LockedIndexes = slices.Clone(g.LockedIndexes)
	out.Locks = map[string]bool{}
	out.LockIDs = map[string]uuid.UUID{}
	for k, v := range g.Locks {
		out.Locks[k] = v
	}
	for k, v := range g.LockIDs {
		out.LockIDs[k] = v
	}
	return out
}
func (s *catalogSupervisor) publishEntry(e *catalogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.byID[e.item.Spec.Instrument.ID]
	if w == nil {
		return
	}
	x := e.state
	pending := s.pendingStates[e.item.Spec.Instrument.ExchangeSymbol]
	if x.Epoch != s.activeEpoch || pending.epoch == x.Epoch && pending.sequence != e.appliedSequence || (s.uncertainScopes[e.scope.String()] && e.confirmedScopeGeneration != s.scopeGenerations[e.scope.String()]) || s.uncertainSymbols[e.item.Spec.Instrument.ExchangeSymbol] {
		x.Known = false
	}
	_ = w.q.offer(event{State: &x})
}
func (s *catalogSupervisor) publishGate(w *catalogWorker, g catalogGate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g = cloneCatalogGate(g)
	if g.Epoch != s.activeEpoch || g.Generation != s.generation.Load() || s.maintenancePending != s.maintenancePublished || s.platformUncertain {
		g.Known = false
	}
	_ = w.q.offer(event{Gate: &g})
}

func (s *catalogSupervisor) published(r catalogResult) {
	j := r.job
	if r.err == nil {
		s.jobsBudget.Release(j.reserved)
		j.reserved = 0
	}
	key := j.scope.String()
	if j.symbol != "" {
		key += ":" + j.symbol
	}
	if j.life == nil && r.err == nil && (j.observation == nil || j.observation.Status == "complete") {
		if s.inflight[key] == j {
			delete(s.inflight, key)
		}
	}
	if r.err != nil {
		s.logger.Error("options evidence persistence failed", "scope", key, "error", r.err)
		// Reuse the exact prepared evidence identity after ambiguous DB results.
		s.scheduleRetry(j, 5*time.Second)
		return
	}
	if j.life != nil {
		o := j.life
		s.mu.Lock()
		active := s.activeEpoch
		pending := s.pendingStates[o.Symbol]
		s.mu.Unlock()
		if o.Epoch != s.epoch || o.Epoch != active {
			return
		}
		if o.Kind == "status" {
			s.finishStatus(j.sourceGeneration)
			if j.sourceGeneration != s.generation.Load() {
				s.requestStatus()
				return
			}
			s.gate.Generation = j.sourceGeneration
			s.gate.BaselineID = o.ID
			s.gate.LockMode = o.LockMode
			s.gate.LockedIndexes = slices.Clone(o.LockedIndexes)
			clear(s.gate.Locks)
			clear(s.gate.LockIDs)
			s.gate.Known = !s.gate.Maintenance || s.gate.MaintenanceID != uuid.Nil
		} else if o.Kind == "platform" {
			// Other public assets are recorded but do not change these four
			// collection families or force their books to resnapshot.
			if !affectsCatalogPlatform(*o) {
				return
			}
			s.mu.Lock()
			latestMaintenance := s.maintenancePending
			s.mu.Unlock()
			if o.Maintenance != nil && o.Sequence == latestMaintenance {
				s.mu.Lock()
				s.maintenancePublished = o.Sequence
				s.platformUncertain = false
				s.mu.Unlock()
				s.gate.Maintenance = *o.Maintenance
				s.gate.MaintenanceID = o.ID
			}
			if o.Locked != nil && j.sourceGeneration == s.generation.Load() {
				s.gate.Locks[o.IndexID] = *o.Locked
				s.gate.LockIDs[o.IndexID] = o.ID
			}
			// A platform update requires a fresh same-generation lock baseline.
			s.gate.Known = false
			s.requestStatus()
		} else if o.Kind == "state" {
			if pending.epoch != o.Epoch || pending.sequence != o.Sequence {
				return
			}
			e := s.entries[o.Symbol]
			if e == nil {
				if isTerminal(o.State) {
					s.mu.Lock()
					if latest := s.pendingStates[o.Symbol]; latest.epoch == o.Epoch && latest.sequence == o.Sequence {
						delete(s.pendingStates, o.Symbol)
						delete(s.uncertainSymbols, o.Symbol)
						delete(s.orphanStates, o.Symbol)
					}
					s.mu.Unlock()
					return
				}
				if _, e := symbolScope(o.Symbol); e != nil {
					return
				}
				if len(s.orphanStates) >= s.cfg.MaxBooks*2 {
					return
				}
				s.orphanStates[o.Symbol] = *o
				scope, err := symbolScope(o.Symbol)
				if err == nil {
					s.requestInstrument(scope, o.Symbol)
				}
				return
			}
			// A fresh HTTP confirmation issued after this frame has already
			// resolved its barrier. A slow durable copy cannot undo that result,
			// including equal source timestamps previously resolved as conflicts.
			if e.state.StateKind == 2 && e.appliedSequence == o.Sequence {
				if e.lastEvent.IsZero() || o.SourceTime.After(e.lastEvent) {
					e.lastEvent = *o.SourceTime
				}
				s.publishEntry(e)
				return
			}
			if !e.lastEvent.IsZero() && !o.SourceTime.After(e.lastEvent) {
				e.appliedSequence = o.Sequence
				if o.SourceTime.Equal(e.lastEvent) && e.marketState != o.State {
					e.state.Known = false
					s.mu.Lock()
					s.uncertainSymbols[o.Symbol] = true
					s.mu.Unlock()
					s.publishEntry(e)
					s.requestInstrument(e.scope, o.Symbol)
				} else {
					s.publishEntry(e)
				}
				return
			}
			e.appliedSequence = o.Sequence
			e.lastEvent = *o.SourceTime
			e.marketState = o.State
			e.state.Known = true
			e.state.Open = o.State == "open"
			e.state.StateID = o.ID
			e.state.StateKind = 1
			e.state.StateAt = s.now().UTC().Truncate(time.Microsecond)
			e.state.Epoch = o.Epoch
			if e.state.Open {
				e.admitted = true
			}
			if e.admitted {
				s.prewarm(e)
			}
			s.publishEntry(e)
			if isTerminal(o.State) {
				s.hub.remove(e.item.Spec.Instrument.ID)
			}
			s.dirty = true
		}
		s.broadcastGate(cloneCatalogGate(s.gate))
		return
	}
	o := j.observation
	cat := j.result
	if o.Status != "complete" {
		for _, e := range s.entries {
			if e.scope == j.scope && (j.symbol == "" || e.item.Spec.Instrument.ExchangeSymbol == j.symbol) {
				e.state.Known = false
				s.publishEntry(e)
			}
		}
		delay := time.Minute
		if j.kind == "instrument" {
			delay = singleInstrumentRetryDelay(j.retryCount)
		}
		s.scheduleRetry(&catalogJob{scope: j.scope, symbol: j.symbol, kind: j.kind, epoch: s.epoch, retryCount: j.retryCount + 1}, delay)
		return
	}
	s.mu.Lock()
	scopeCurrent := s.scopeGenerations[j.scope.String()] == j.scopeGeneration
	if j.kind == "catalog" && scopeCurrent && j.epoch == s.activeEpoch && !s.protocolScopes[j.scope.String()] {
		delete(s.uncertainScopes, j.scope.String())
	}
	s.mu.Unlock()
	if j.kind == "catalog" && j.epoch == s.epoch && scopeCurrent {
		// A delayed durable retry cannot give old source evidence a new lifetime.
		s.scopes[j.scope.String()] = o.ObservedAt
	}
	for _, item := range cat.Instruments {
		name := item.Spec.Instrument.ExchangeSymbol
		e := s.entries[name]
		if e != nil {
			if o.ObservedAt.Before(e.state.ObservedAt) {
				continue
			}
			if j.kind == "catalog" && e.item.Spec.DefinitionHash() != item.Spec.DefinitionHash() {
				s.markProtocolUnknown(j.scope, name)
				s.requestInstrument(j.scope, name)
				continue
			}
			if j.kind == "catalog" && (e.latestKind == "creation" || e.latestKind == "instrument") && o.RequestedAt.Before(e.state.ObservedAt.Add(time.Minute)) {
				continue
			}
		}
		if e == nil || e.item.Spec.Instrument.ID != item.Spec.Instrument.ID {
			if e != nil {
				s.invalidateSymbol(name)
				s.hub.remove(e.item.Spec.Instrument.ID)
			}
			e = &catalogEntry{scope: j.scope}
			s.entries[name] = e
		}
		e.item = item
		e.latestKind = j.kind
		if orphan, ok := s.orphanStates[name]; ok && orphan.Epoch == s.epoch {
			e.appliedSequence = orphan.Sequence
			e.lastEvent = *orphan.SourceTime
			e.marketState = orphan.State
			e.state.StateID = orphan.ID
			e.state.StateKind = 1
			e.state.StateAt = s.now().UTC().Truncate(time.Microsecond)
			e.state.Open = orphan.State == "open"
			e.state.Known = true
			e.state.Epoch = orphan.Epoch
			delete(s.orphanStates, name)
		}
		e.state.InstrumentID = item.Spec.Instrument.ID
		e.state.RuleID = item.Rule.ID()
		e.state.RuleObservationID = o.ID
		e.state.RuleAt = s.now().UTC().Truncate(time.Microsecond)
		e.state.ObservedAt = o.ObservedAt
		s.mu.Lock()
		pending := s.pendingStates[name]
		active := s.activeEpoch
		s.mu.Unlock()
		if j.epoch == s.epoch && j.epoch == active {
			// Cached bulk metadata cannot overwrite a state event in this epoch.
			confirmedBarrier := pending.epoch != s.epoch || e.state.Epoch == s.epoch && e.appliedSequence == pending.sequence
			currentWSState := e.state.StateKind == 1 && e.state.Epoch == s.epoch
			canUseCatalog := j.kind == "catalog" && !currentWSState && confirmedBarrier
			canUseCreation := j.kind == "creation" && !currentWSState && pending.epoch != s.epoch
			canUseSingle := j.kind == "instrument" && j.barrierSequence == pending.sequence
			if scopeCurrent && (canUseCatalog || canUseCreation || canUseSingle) {
				if j.kind == "instrument" {
					e.appliedSequence = j.barrierSequence
					e.confirmedScopeGeneration = j.scopeGeneration
					s.mu.Lock()
					delete(s.uncertainSymbols, name)
					s.mu.Unlock()
				}
				e.marketState = item.State
				e.state.StateID = o.ID
				e.state.StateKind = 2
				e.state.StateAt = e.state.RuleAt
				e.state.StateObservedAt = o.ObservedAt
				e.state.Open = item.State == "open" && item.Active
				e.state.Known = true
				e.state.Epoch = s.epoch
			}
			if e.state.Open {
				e.admitted = true
			}
			if e.admitted {
				s.prewarm(e)
			}
		}
		if e.admitted {
			s.prewarm(e)
		}
		s.mu.Lock()
		symbolUncertain := s.uncertainSymbols[name]
		protocolUncertain := s.protocolScopes[j.scope.String()]
		s.mu.Unlock()
		if ((protocolUncertain && e.confirmedScopeGeneration != j.scopeGeneration) || symbolUncertain) && (j.kind != "instrument" || j.barrierSequence != pending.sequence) {
			s.requestInstrument(e.scope, name)
		}
		if scopeCurrent && j.epoch == s.epoch && e.state.Epoch == s.epoch && e.state.StateKind == 1 && !symbolUncertain {
			e.state.Known = true
		}
		if pending.epoch == s.epoch && e.state.StateKind != 1 && e.appliedSequence != pending.sequence {
			e.state.Known = false
		}
		s.publishEntry(e)
	}
	// Missing members are uncertain, never deleted from a cached directory.
	if j.kind == "catalog" && j.epoch == s.epoch {
		seen := map[string]bool{}
		for _, p := range cat.Instruments {
			seen[p.Spec.Instrument.ExchangeSymbol] = true
		}
		for name, e := range s.entries {
			if e.scope == j.scope && !seen[name] {
				if isTerminal(e.marketState) || !e.item.Spec.Instrument.ExpiryTime.After(s.now()) {
					continue
				}
				e.state.Known = false
				s.publishEntry(e)
				s.requestInstrument(e.scope, name)
			}
		}
	}
	if j.epoch != s.epoch || !scopeCurrent {
		if j.symbol != "" {
			s.requestInstrument(j.scope, j.symbol)
		} else {
			s.requestScope(j.scope)
		}
	}
	if j.kind == "instrument" && scopeCurrent {
		s.mu.Lock()
		if s.protocolScopes[j.scope.String()] {
			all := true
			for _, entry := range s.entries {
				if entry.scope == j.scope && entry.item.Spec.Instrument.ExpiryTime.After(s.now()) && entry.confirmedScopeGeneration != j.scopeGeneration {
					all = false
					break
				}
			}
			if all {
				delete(s.protocolScopes, j.scope.String())
				delete(s.uncertainScopes, j.scope.String())
			}
		}
		s.mu.Unlock()
	}
	s.dirty = true
}
func affectsCatalogPlatform(o options.LifecycleObservation) bool {
	return o.Maintenance != nil || o.Locked != nil && containsString([]string{"btc_usd", "eth_usd", "btc_usdc", "eth_usdc"}, o.IndexID)
}
func singleInstrumentRetryDelay(attempt int) time.Duration {
	if attempt == 0 {
		return 5 * time.Second
	}
	if attempt == 1 {
		return 15 * time.Second
	}
	return time.Minute
}
func isTerminal(state string) bool { return state == "delivered" || state == "archivized" }
func (s *catalogSupervisor) tick(now time.Time) {
	if s.lifecycleReady && !s.statusPending && !s.statusRetryAfter.IsZero() && !now.Before(s.statusRetryAfter) {
		s.requestStatus()
	}
	s.mu.Lock()
	kept := s.workers[:0]
	var removed []*catalogWorker
	for _, w := range s.workers {
		select {
		case <-w.done:
			removed = append(removed, w)
			for id, owner := range s.byID {
				if owner == w {
					delete(s.byID, id)
				}
			}
		default:
			kept = append(kept, w)
		}
	}
	s.workers = kept
	s.mu.Unlock()
	for _, w := range removed {
		s.hub.removeWorker(w)
	}
	for _, scope := range catalogScopes() {
		last := s.scopes[scope.String()]
		// A later, partial refresh or creation may have updated the scope clock
		// without renewing an individual member's referenced state/rule evidence.
		for _, e := range s.entries {
			if e.scope != scope || !e.item.Spec.Instrument.ExpiryTime.After(now) {
				continue
			}
			observations := []time.Time{e.state.ObservedAt}
			if e.state.StateKind == 2 {
				observations = append(observations, e.state.StateObservedAt)
			}
			for _, observed := range observations {
				if !observed.IsZero() && (last.IsZero() || observed.Before(last)) {
					last = observed
				}
			}
		}
		s.mu.Lock()
		uncertain := s.uncertainScopes[scope.String()]
		s.mu.Unlock()
		if s.lifecycleReady && (uncertain || last.IsZero() || now.Sub(last) >= 30*time.Minute) && now.Sub(s.attempted[scope.String()]) >= time.Minute {
			s.requestScope(scope)
		}
	}
	remaining := s.retries[:0]
	for _, r := range s.retries {
		if s.confirmationSatisfied(r.job, now) {
			s.jobsBudget.Release(r.job.reserved)
			r.job.reserved = 0
			key := r.job.scope.String() + ":" + r.job.symbol
			if s.inflight[key] == r.job {
				delete(s.inflight, key)
			}
			continue
		}
		if r.job.life == nil && r.job.symbol != "" {
			if e := s.entries[r.job.symbol]; e != nil && (isTerminal(e.marketState) || !e.item.Spec.Instrument.ExpiryTime.After(now)) {
				s.jobsBudget.Release(r.job.reserved)
				r.job.reserved = 0
				key := r.job.scope.String() + ":" + r.job.symbol
				if s.inflight[key] == r.job {
					delete(s.inflight, key)
				}
				continue
			}
		}
		if now.Before(r.at) {
			remaining = append(remaining, r)
			continue
		}
		if !s.enqueue(r.job) {
			r.at = now.Add(time.Second)
			remaining = append(remaining, r)
		}
	}
	s.retries = remaining
	for name, e := range s.entries {
		if !e.item.Spec.Instrument.ExpiryTime.After(now) {
			s.hub.remove(e.item.Spec.Instrument.ID)
			s.mu.Lock()
			delete(s.byID, e.item.Spec.Instrument.ID)
			s.mu.Unlock()
			if e.admitted {
				e.admitted = false
				s.dirty = true
			}
		}
		if !e.item.Spec.Instrument.ExpiryTime.After(now) {
			if !e.admitted && now.Sub(*e.item.Spec.Instrument.ExpiryTime) > time.Hour {
				delete(s.entries, name)
				s.mu.Lock()
				delete(s.pendingStates, name)
				delete(s.uncertainSymbols, name)
				s.mu.Unlock()
			}
			continue
		}
		s.mu.Lock()
		pending := s.pendingStates[name]
		uncertain := s.uncertainSymbols[name]
		s.mu.Unlock()
		needsConfirmation := pending.epoch == s.epoch && pending.sequence != e.appliedSequence || uncertain || !e.state.Known || e.state.Epoch != s.epoch || !e.admitted && !isTerminal(e.marketState)
		key := e.scope.String() + ":" + name
		// A fresh same-epoch bulk observation can repair ordinary uncertainty.
		// Protocol errors or unresolved source-state barriers still require the
		// independent per-symbol confirmation used by the existing strict path.
		bulk := s.inflight[e.scope.String()]
		s.mu.Lock()
		protocolUnknown := s.protocolScopes[e.scope.String()]
		s.mu.Unlock()
		barrierPending := pending.epoch == s.epoch && pending.sequence != e.appliedSequence
		if bulk != nil && bulk.epoch == s.epoch && e.admitted && !uncertain && !protocolUnknown && !barrierPending {
			continue
		}
		if s.lifecycleReady && needsConfirmation && now.Sub(s.attempted[key]) >= time.Minute {
			s.requestInstrument(e.scope, name)
		}
	}
	// A dropped creation or first open event may have no entry yet. The
	// reader's symbol barrier still owns the recovery intent in that case.
	s.mu.Lock()
	var unseen []string
	for name, pending := range s.pendingStates {
		if pending.epoch == s.epoch && s.entries[name] == nil {
			unseen = append(unseen, name)
		}
	}
	s.mu.Unlock()
	if s.lifecycleReady {
		for _, name := range unseen {
			if scope, err := symbolScope(name); err == nil && now.Sub(s.attempted[scope.String()+":"+name]) >= time.Minute {
				s.requestInstrument(scope, name)
			}
		}
	}
	if s.unresolved != nil && !s.planning && !now.Before(s.resolveAfter) {
		s.planning = true
		s.submitPlan(*s.unresolved)
		return
	}
	if s.dirty && !s.planning && s.unresolved == nil {
		s.buildPlan(now)
	}
	s.logHealth(now)
}

// Only cancel work that has not captured any source response. Prepared evidence
// keeps its identity and still persists after an ambiguous database result.
func (s *catalogSupervisor) confirmationSatisfied(j *catalogJob, now time.Time) bool {
	if j.life != nil || j.symbol == "" || j.result != nil || j.observation != nil {
		return false
	}
	e := s.entries[j.symbol]
	if e == nil || !e.state.Known || e.state.Epoch != s.epoch || e.state.ObservedAt.IsZero() || now.Sub(e.state.ObservedAt) > 30*time.Minute {
		return false
	}
	if e.state.StateKind == 2 && (e.state.StateObservedAt.IsZero() || now.Sub(e.state.StateObservedAt) > 30*time.Minute) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.pendingStates[j.symbol]
	return !s.uncertainSymbols[j.symbol] && !s.protocolScopes[j.scope.String()] &&
		(pending.epoch != s.epoch || pending.sequence == e.appliedSequence)
}

func (s *catalogSupervisor) logHealth(now time.Time) {
	if s.logger == nil {
		return
	}
	if !s.lastHealth.IsZero() && now.Sub(s.lastHealth) < time.Minute {
		return
	}
	s.lastHealth = now
	unknown, staleRules, staleStates := 0, 0, 0
	for _, e := range s.entries {
		if !e.item.Spec.Instrument.ExpiryTime.After(now) {
			continue
		}
		if !e.state.Known || e.state.Epoch != s.epoch {
			unknown++
		}
		if e.state.ObservedAt.IsZero() || now.Sub(e.state.ObservedAt) > options.CatalogEvidenceMaxAge {
			staleRules++
		}
		if e.state.StateKind == 2 && (e.state.StateObservedAt.IsZero() || now.Sub(e.state.StateObservedAt) > options.CatalogEvidenceMaxAge) {
			staleStates++
		}
	}
	s.mu.Lock()
	uncertainScopes, protocolScopes := len(s.uncertainScopes), len(s.protocolScopes)
	s.mu.Unlock()
	s.logger.Info("options catalog runtime health", "entries", len(s.entries), "unknown_entries", unknown,
		"stale_rule_entries", staleRules, "stale_state_entries", staleStates, "uncertain_scopes", uncertainScopes, "protocol_scopes", protocolScopes,
		"scope_jobs", len(s.scopeJobs), "symbol_jobs", len(s.jobs), "retry_jobs", len(s.retries),
		"ingress_bytes", s.budget.Used(), "pending_minute_bytes", s.minuteCacheBytes())
}

func (s *catalogSupervisor) minuteCacheBytes() int64 {
	if s.minuteCache == nil {
		return 0
	}
	return s.minuteCache.budget.Used()
}
func (s *catalogSupervisor) workerFailed(id uuid.UUID) {
	s.mu.Lock()
	var target *catalogWorker
	for _, w := range s.workers {
		if w.id == id {
			target = w
		}
	}
	if target != nil {
		target.failed = true
		for iid, w := range s.byID {
			if w == target {
				delete(s.byID, iid)
			}
		}
	}
	s.mu.Unlock()
	if target != nil {
		s.hub.removeWorker(target)
		target.cancel()
		s.dirty = true
	}
}
func (s *catalogSupervisor) buildPlan(now time.Time) {
	if s.tail != nil && s.tail.EffectiveMinute.After(now.Truncate(time.Minute).Add(time.Minute)) {
		return
	}
	var desired []*catalogEntry
	for _, e := range s.entries {
		if e.admitted && !isTerminal(e.marketState) && e.item.Spec.Instrument.ExpiryTime.After(now) {
			desired = append(desired, e)
		}
	}
	sort.Slice(desired, func(i, j int) bool { return desired[i].item.Spec.Instrument.ID < desired[j].item.Spec.Instrument.ID })
	p := options.CollectionPlan{ID: uuid.New(), SessionID: s.session, Revision: 1, ConfigHash: options.PayloadHash([]byte(fmt.Sprintf("catalog_v2:%+v", s.cfg))), CreatedAt: now.Truncate(time.Microsecond), EffectiveMinute: now.Truncate(time.Minute).Add(2 * time.Minute)}
	if s.tail != nil {
		p.PreviousID = s.tail.ID
		p.PreviousHash = s.tail.Hash()
		if s.tail.SessionID == s.session {
			p.Revision = s.tail.Revision + 1
		}
		if !p.EffectiveMinute.After(s.tail.EffectiveMinute) {
			p.EffectiveMinute = s.tail.EffectiveMinute.Add(time.Minute)
		}
	}
	if len(desired) > s.cfg.MaxBooks {
		for _, e := range desired[s.cfg.MaxBooks:] {
			p.PendingSymbols = append(p.PendingSymbols, e.item.Spec.Instrument.ExchangeSymbol)
			p.PendingReasons = append(p.PendingReasons, "book_capacity")
		}
		desired = desired[:s.cfg.MaxBooks]
	}
	wanted := map[uint32]*catalogEntry{}
	for _, e := range desired {
		wanted[e.item.Spec.Instrument.ID] = e
	}
	var groups []catalogGroup
	s.mu.Lock()
	ws := slices.Clone(s.workers)
	s.mu.Unlock()
	for _, w := range ws {
		if w.failed {
			continue
		}
		select {
		case <-w.done:
			continue
		default:
		}
		g := catalogGroup{worker: w}
		for _, spec := range w.specs {
			// A late prewarm can be retained alongside a full run while an
			// earlier plan is being persisted. Keep excess members in wanted
			// so they receive another shard instead of creating an invalid run.
			if len(g.specs) == options.MaxLiveBooks {
				break
			}
			if e := wanted[spec.Instrument.ID]; e != nil {
				g.specs = append(g.specs, e.item.Spec)
				delete(wanted, spec.Instrument.ID)
			}
		}
		groups = append(groups, g)
	}
	var extra []options.ContractSpec
	for _, e := range wanted {
		extra = append(extra, e.item.Spec)
	}
	sort.Slice(extra, func(i, j int) bool { return extra[i].Instrument.ID < extra[j].Instrument.ID })
	for _, spec := range extra {
		placed := false
		for n := range groups {
			if len(groups[n].specs) < options.MaxLiveBooks {
				groups[n].specs = append(groups[n].specs, spec)
				placed = true
				break
			}
		}
		if !placed {
			groups = append(groups, catalogGroup{specs: []options.ContractSpec{spec}})
		}
	}
	obs := map[uuid.UUID]bool{}
	changed := s.tail == nil || s.tail.SessionID != s.session
	for n := range groups {
		g := &groups[n]
		sort.Slice(g.specs, func(i, j int) bool { return g.specs[i].Instrument.ID < g.specs[j].Instrument.ID })
		if len(g.specs) == 0 {
			g.retire = true
			if g.worker != nil {
				changed = true
			}
			continue
		}
		same := g.worker != nil && len(g.specs) == len(g.worker.run.Members)
		if same {
			for k, spec := range g.specs {
				if g.worker.run.Members[k].InstrumentID != spec.Instrument.ID {
					same = false
					break
				}
			}
		}
		if same {
			g.run = g.worker.run
		} else {
			changed = true
			g.run = options.LiveRun{ID: uuid.New(), StartedAt: p.CreatedAt, RESTURL: s.cfg.RESTURL, WSURL: s.cfg.WSURL, Selection: options.CatalogSelection}
			idx := map[string]bool{}
			for _, spec := range g.specs {
				g.run.Members = append(g.run.Members, options.LiveMember{InstrumentID: spec.Instrument.ID, Symbol: spec.Instrument.ExchangeSymbol, DefinitionHash: spec.DefinitionHash(), IndexID: spec.IndexID})
				idx[spec.IndexID] = true
			}
			for id := range idx {
				g.run.Indexes = append(g.run.Indexes, id)
			}
			slices.Sort(g.run.Indexes)
		}
		for _, spec := range g.specs {
			p.InstrumentIDs = append(p.InstrumentIDs, spec.Instrument.ID)
			p.Definitions = append(p.Definitions, spec.DefinitionHash())
			p.RunIDs = append(p.RunIDs, g.run.ID)
			e := s.entries[spec.Instrument.ExchangeSymbol]
			obs[e.state.RuleObservationID] = true
		}
	}
	// Sort the ownership arrays together, independently of stable shard assignment.
	order := make([]int, len(p.InstrumentIDs))
	for n := range order {
		order[n] = n
	}
	sort.Slice(order, func(i, j int) bool { return p.InstrumentIDs[order[i]] < p.InstrumentIDs[order[j]] })
	ids := slices.Clone(p.InstrumentIDs)
	defs := slices.Clone(p.Definitions)
	runs := slices.Clone(p.RunIDs)
	for n, k := range order {
		p.InstrumentIDs[n] = ids[k]
		p.Definitions[n] = defs[k]
		p.RunIDs[n] = runs[k]
	}
	for id := range obs {
		p.ObservationIDs = append(p.ObservationIDs, id)
	}
	slices.SortFunc(p.ObservationIDs, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	for name, e := range s.entries {
		if !e.admitted && e.item.Spec.Instrument.ExpiryTime.After(now) {
			p.PendingSymbols = append(p.PendingSymbols, name)
			p.PendingReasons = append(p.PendingReasons, "waiting_open")
		}
	}
	sortPendingPlan(&p)
	if s.tail != nil && !slices.Equal(s.tail.PendingSymbols, p.PendingSymbols) {
		changed = true
	}
	s.dirty = false
	if !changed {
		return
	}
	s.planning = true
	s.submitPlan(catalogPlanResult{plan: p, groups: groups})
}
func (s *catalogSupervisor) submitPlan(candidate catalogPlanResult) {
	go func() {
		p := candidate.plan
		groups := candidate.groups
		var err error
		if s.now().Before(p.EffectiveMinute.Add(-time.Second)) {
			ctx, cancel := context.WithDeadline(s.ctx, p.EffectiveMinute.Add(-time.Second))
			for _, g := range groups {
				if g.retire || g.worker != nil && g.worker.persisted && g.run.ID == g.worker.run.ID {
					continue
				}
				if err = s.sink.WriteOptionsRun(ctx, g.run); err != nil {
					break
				}
			}
			if err == nil {
				err = s.sink.WriteOptionsPlan(ctx, p)
			}
			cancel()
		} else {
			err = fmt.Errorf("plan publication boundary passed")
		}
		if err != nil && s.ctx.Err() == nil {
			check, stop := context.WithTimeout(s.ctx, 10*time.Second)
			tail, e := s.sink.LatestOptionsPlan(check)
			stop()
			if e == nil && tail.ID == p.ID && tail.Hash() == p.Hash() {
				err = nil
			} else if !s.now().Before(p.EffectiveMinute) && (e == nil && tail.ID == p.PreviousID || errors.Is(e, s.sink.OptionsNotFound()) && p.PreviousID == uuid.Nil) {
				candidate.absent = true
			}
		}
		candidate.err = err
		select {
		case s.plans <- candidate:
		case <-s.ctx.Done():
		}
	}()
}

func (s *catalogSupervisor) planned(r catalogPlanResult) {
	s.planning = false
	if r.err != nil {
		s.logger.Error("options plan publication failed", "error", r.err)
		if r.absent {
			s.unresolved = nil
			s.dirty = true
		} else {
			s.unresolved = &r
			s.resolveAfter = s.now().Add(5 * time.Second)
		}
		return
	}
	s.unresolved = nil
	p := r.plan
	p.RowHash = p.Hash()
	s.tail = &p
	assigned := make(map[uint32]bool, len(p.InstrumentIDs))
	for _, id := range p.InstrumentIDs {
		assigned[id] = true
	}
	// Plan publication can finish after its sampling boundary on an ambiguous DB
	// response. That minute remains missing; new workers never backfill validity.
	for n := range r.groups {
		g := &r.groups[n]
		if g.retire {
			if g.worker != nil {
				_ = g.worker.q.offer(event{Plan: &p})
				_ = g.worker.q.offer(event{Swap: &catalogSwap{At: p.EffectiveMinute, Retire: true}})
			}
			continue
		}
		w := g.worker
		if w != nil {
			select {
			case <-w.done:
				w = nil
			default:
				if w.failed {
					w = nil
				}
			}
		}
		var err error
		if w == nil {
			w, err = startCatalogWorker(s.ctx, s.cfg, g.run, g.specs, s.sink, s.budget, s.levels, s.minuteCache, s.hub.recover, func() {
				select {
				case s.hub.resetIndex <- struct{}{}:
				default:
				}
			}, s.logger, s.failed)
			if err != nil {
				s.logger.Error("options worker start failed", "error", err)
				s.dirty = true
				continue
			}
			s.mu.Lock()
			s.workers = append(s.workers, w)
			s.mu.Unlock()
			s.hub.register(w)
		}
		_ = w.q.offer(event{Plan: &p})
		if w.run.ID != g.run.ID {
			_ = w.q.offer(event{Swap: &catalogSwap{At: p.EffectiveMinute, Run: g.run, Specs: g.specs}})
		}
		s.mu.Lock()
		w.persisted = true
		oldSpecs := w.specs
		w.run = g.run
		w.specs = slices.Clone(g.specs)
		selected := map[uint32]bool{}
		for _, spec := range g.specs {
			selected[spec.Instrument.ID] = true
		}
		for _, spec := range oldSpecs {
			if selected[spec.Instrument.ID] {
				continue
			}
			entry := s.entries[spec.Instrument.ExchangeSymbol]
			// Only members that arrived after this candidate was built remain
			// pending here. Members assigned to another run must not be added
			// back to this worker's warm list and reclaimed by the next plan.
			if !assigned[spec.Instrument.ID] && entry != nil && entry.admitted && entry.item.Spec.Instrument.ID == spec.Instrument.ID && !isTerminal(entry.marketState) && spec.Instrument.ExpiryTime.After(s.now()) {
				w.specs = append(w.specs, spec)
			} else {
				if s.byID[spec.Instrument.ID] == w {
					delete(s.byID, spec.Instrument.ID)
				}
			}
		}
		for _, spec := range g.specs {
			s.byID[spec.Instrument.ID] = w
		}
		s.mu.Unlock()
		s.publishGate(w, s.gate)
		for _, spec := range g.specs {
			if e := s.entries[spec.Instrument.ExchangeSymbol]; e != nil {
				s.publishEntry(e)
				s.hub.add(spec.Instrument.ID, spec.Instrument.ExchangeSymbol, w)
			}
		}
	}
	s.logger.Info("options catalog plan published", "plan", p.ID, "effective", p.EffectiveMinute, "books", len(p.InstrumentIDs), "pending", len(p.PendingSymbols))
}
func gatePointer(g catalogGate) *catalogGate { return &g }

func sortPendingPlan(p *options.CollectionPlan) {
	order := make([]int, len(p.PendingSymbols))
	for n := range order {
		order[n] = n
	}
	sort.Slice(order, func(i, j int) bool { return p.PendingSymbols[order[i]] < p.PendingSymbols[order[j]] })
	names := slices.Clone(p.PendingSymbols)
	reasons := slices.Clone(p.PendingReasons)
	for n, k := range order {
		p.PendingSymbols[n] = names[k]
		p.PendingReasons[n] = reasons[k]
	}
}

func (s *catalogSupervisor) scheduleRetry(j *catalogJob, delay time.Duration) {
	if !j.barrierCaptured && j.life == nil {
		s.mu.Lock()
		j.barrierSequence = s.pendingStates[j.symbol].sequence
		j.scopeGeneration = s.scopeGenerations[j.scope.String()]
		j.barrierCaptured = true
		s.mu.Unlock()
	}
	if j.reserved == 0 {
		cost := int64(len(j.raw) + 512)
		if j.result != nil {
			cost += int64(len(j.result.Raw))
		}
		if !s.jobsBudget.Reserve(cost) {
			s.dropRetry(j)
			return
		}
		j.reserved = cost
	}
	if len(s.retries) >= s.evidenceQueueLimit() {
		s.jobsBudget.Release(j.reserved)
		j.reserved = 0
		s.dropRetry(j)
		s.logger.Error("options retry capacity exceeded")
		return
	}
	if j.life == nil {
		key := j.scope.String()
		if j.symbol != "" {
			key += ":" + j.symbol
		}
		s.inflight[key] = j
	}
	s.retries = append(s.retries, scheduledCatalogJob{s.now().Add(delay), j, 0})
}

func (s *catalogSupervisor) prewarm(e *catalogEntry) {
	id := e.item.Spec.Instrument.ID
	if !e.admitted || !e.state.Known || !e.item.Spec.Instrument.ExpiryTime.After(s.now()) {
		return
	}
	s.mu.Lock()
	if s.byID[id] != nil {
		s.mu.Unlock()
		return
	}
	if len(s.byID) >= s.cfg.MaxBooks {
		s.mu.Unlock()
		return
	}
	var w *catalogWorker
	for _, candidate := range s.workers {
		if !candidate.failed && len(candidate.specs) < options.MaxLiveBooks {
			select {
			case <-candidate.done:
				continue
			default:
			}
			w = candidate
			break
		}
	}
	s.mu.Unlock()
	spec := e.item.Spec
	if w == nil {
		r := options.LiveRun{ID: uuid.New(), StartedAt: s.now().UTC().Truncate(time.Microsecond), RESTURL: s.cfg.RESTURL, WSURL: s.cfg.WSURL, Selection: options.CatalogSelection, Indexes: []string{spec.IndexID}, Members: []options.LiveMember{{InstrumentID: id, Symbol: spec.Instrument.ExchangeSymbol, DefinitionHash: spec.DefinitionHash(), IndexID: spec.IndexID}}}
		var err error
		w, err = startCatalogWorker(s.ctx, s.cfg, r, []options.ContractSpec{spec}, s.sink, s.budget, s.levels, s.minuteCache, s.hub.recover, func() {
			select {
			case s.hub.resetIndex <- struct{}{}:
			default:
			}
		}, s.logger, s.failed)
		if err != nil {
			s.logger.Error("options prewarm failed", "error", err)
			return
		}
		s.mu.Lock()
		s.workers = append(s.workers, w)
		s.mu.Unlock()
		s.hub.register(w)
	} else {
		_ = w.q.offer(event{Spec: &spec})
		s.mu.Lock()
		w.specs = append(w.specs, spec)
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.byID[id] = w
	s.mu.Unlock()
	s.publishGate(w, s.gate)
	s.publishEntry(e)
	s.hub.add(id, spec.Instrument.ExchangeSymbol, w)
}

func channelScope(channel string) deribit.Scope {
	p := strings.Split(channel, ".")
	if len(p) == 4 && (p[1] == "state" || p[1] == "creation") {
		return deribit.Scope{Currency: p[3], Kind: p[2]}
	}
	return deribit.Scope{}
}
func (s *catalogSupervisor) markScopeUnknown(scope deribit.Scope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertainScopes == nil {
		s.uncertainScopes = map[string]bool{}
	}
	if s.scopeGenerations == nil {
		s.scopeGenerations = map[string]uint64{}
	}
	s.scopeGenerations[scope.String()]++
	s.uncertainScopes[scope.String()] = true
	for id, w := range s.byID {
		for _, spec := range w.specs {
			own, _ := symbolScope(spec.Instrument.ExchangeSymbol)
			if scope.Currency == "" || own == scope {
				state := catalogState{InstrumentID: id}
				_ = w.q.offer(event{State: &state})
			}
		}
	}
}

func extractLifecycleSymbol(raw []byte) string {
	var f struct {
		Params struct {
			Data struct {
				Symbol string `json:"instrument_name"`
			} `json:"data"`
		} `json:"params"`
	}
	if json.Unmarshal(raw, &f) != nil {
		return ""
	}
	if _, err := symbolScope(f.Params.Data.Symbol); err != nil {
		return ""
	}
	return f.Params.Data.Symbol
}
func (s *catalogSupervisor) markProtocolUnknown(scope deribit.Scope, symbol string) {
	if symbol == "" {
		s.markScopeUnknown(scope)
		s.mu.Lock()
		s.protocolScopes[scope.String()] = true
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.uncertainSymbols[symbol] = true
	s.mu.Unlock()
	s.invalidateSymbol(symbol)
}
func (s *catalogSupervisor) dropRetry(j *catalogJob) {
	if j.life != nil && j.life.Kind == "status" && j.life.Epoch == s.epoch {
		s.finishStatus(j.sourceGeneration)
		s.gate.Known = false
		s.statusRetryAfter = s.now().Add(time.Second)
	}
	key := j.scope.String()
	if j.symbol != "" {
		key += ":" + j.symbol
	}
	if s.inflight[key] == j {
		delete(s.inflight, key)
	}
	if j.life == nil {
		if j.symbol != "" {
			s.markProtocolUnknown(j.scope, j.symbol)
		} else {
			s.ensureScopeUnknown(j.scope)
		}
	} else {
		if j.life.Kind == "state" {
			scope, err := symbolScope(j.life.Symbol)
			if err == nil {
				// The reader's pending barrier survives bounded queue overflow.
				// tick actively obtains a fresh confirmation for this symbol.
				s.markProtocolUnknown(scope, j.life.Symbol)
			}
		} else {
			s.invalidateAll()
		}
	}
}

func (s *catalogSupervisor) ensureScopeUnknown(scope deribit.Scope) {
	s.mu.Lock()
	uncertain := s.uncertainScopes[scope.String()]
	s.mu.Unlock()
	if !uncertain {
		s.markScopeUnknown(scope)
	}
}
