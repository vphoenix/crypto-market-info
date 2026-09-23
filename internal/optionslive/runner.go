package optionslive

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type Sink interface {
	InitOptionsLiveSchema(context.Context) error
	RegisterDerivativeSpecs(context.Context, []options.ContractSpec) ([]options.ContractSpec, error)
	WriteDerivativeTradingRule(context.Context, options.TradingRule) error
	WriteOptionsMetadata(context.Context, options.MetadataObservation) error
	WriteOptionsRun(context.Context, options.LiveRun) error
	WriteOptionsMinute(context.Context, options.LiveEnvelope) error
}
type Prepared struct {
	Run      options.LiveRun
	Specs    []options.ContractSpec
	Scopes   []deribit.Scope
	Metadata []options.MetadataObservation
}

func Prepare(ctx context.Context, c *deribit.Client, cfg Config, sink Sink) (Prepared, error) {
	var p Prepared
	plan, err := Discover(ctx, c, cfg)
	if err != nil {
		return p, err
	}
	if err = sink.InitOptionsLiveSchema(ctx); err != nil {
		return p, err
	}
	var specs []options.ContractSpec
	for _, item := range plan.Selected {
		specs = append(specs, item.Spec)
	}
	registered, err := sink.RegisterDerivativeSpecs(ctx, specs)
	if err != nil {
		return p, err
	}
	p.Specs = registered
	p.Run = options.LiveRun{ID: uuid.New(), RESTURL: c.RESTURL, WSURL: c.WSURL, Selection: plan.Selection, References: plan.References}
	seenIndexes, seenScopes := map[string]bool{}, map[string]bool{}
	for n, s := range registered {
		p.Run.Members = append(p.Run.Members, options.LiveMember{InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, DefinitionHash: s.DefinitionHash(), IndexID: s.IndexID})
		seenIndexes[s.IndexID] = true
		scope, _ := symbolScope(s.Instrument.ExchangeSymbol)
		if !seenScopes[scope.String()] {
			seenScopes[scope.String()] = true
			p.Scopes = append(p.Scopes, scope)
		}
		var source deribit.ScopeResult
		for _, r := range plan.Scopes {
			if r.Scope == scope {
				source = r
				break
			}
		}
		rule := plan.Selected[n].Rule
		rule.InstrumentID = s.Instrument.ID
		if err = sink.WriteDerivativeTradingRule(ctx, rule); err != nil {
			return p, err
		}
		o := options.MetadataObservation{AttemptID: source.RequestID, RunID: p.Run.ID, InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, Scope: scope.String(), SourceURL: source.URL, RequestedAt: source.RequestedAt, ObservedAt: source.ObservedAt, PayloadHash: source.PayloadHash, Status: "complete", DefinitionHash: s.DefinitionHash(), TradingRuleID: rule.ID(), State: plan.Selected[n].State, Active: plan.Selected[n].Active, ScopeComplete: true, ScopeRawCount: source.RawCount, ScopeAcceptedCount: source.AcceptedCount, ScopeExcludedCount: source.ExcludedCount}
		if err = sink.WriteOptionsMetadata(ctx, o); err != nil {
			return p, err
		}
		p.Metadata = append(p.Metadata, o)
	}
	for id := range seenIndexes {
		p.Run.Indexes = append(p.Run.Indexes, id)
	}
	slices.Sort(p.Run.Indexes)
	sort.Slice(p.Run.Members, func(i, j int) bool { return p.Run.Members[i].InstrumentID < p.Run.Members[j].InstrumentID })
	sort.Slice(p.Scopes, func(i, j int) bool { return p.Scopes[i].String() < p.Scopes[j].String() })
	p.Run.StartedAt = time.Now().UTC().Truncate(time.Microsecond)
	if err = sink.WriteOptionsRun(ctx, p.Run); err != nil {
		return p, err
	}
	return p, nil
}

// Run is the application component. Recoverable failures remain inside this
// task; other venues are not canceled. The client/gates survive run retries.
func Run(ctx context.Context, cfg Config, sink Sink, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := deribit.NewClient(cfg.RESTURL, cfg.WSURL)
	for ctx.Err() == nil {
		p, err := Prepare(ctx, c, cfg, sink)
		if err == nil {
			logger.Info("options run started", "run_id", p.Run.ID, "books", len(p.Run.Members), "indexes", p.Run.Indexes)
			err = Collect(ctx, c, p, sink, logger)
		}
		if ctx.Err() != nil {
			return nil
		}
		logger.Error("options task will retry", "error", err)
		if !exchange.Wait(ctx, exchange.AddJitter(30*time.Second)) {
			return nil
		}
	}
	return nil
}

// Collect serves one fixed run and drains only complete queued minutes on exit.
func Collect(ctx context.Context, c *deribit.Client, p Prepared, sink Sink, logger *slog.Logger) (result error) {
	if logger == nil {
		logger = slog.Default()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	started := time.Now()
	q := newIngress(started)
	reset := map[string]chan struct{}{"book": make(chan struct{}, 1), "index": make(chan struct{}, 1)}
	refresh := make(chan struct{}, 1)
	minutes := make(chan options.LiveEnvelope, 2)
	failures := make(chan error, 4)
	writeCtx, writeCancel := context.WithCancel(context.WithoutCancel(ctx))
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for e := range minutes {
			if time.Since(e.PreparedAt) > 45*time.Second {
				failures <- fmt.Errorf("options oldest write exceeded 45s")
				return
			}
			attempt, stop := context.WithTimeout(writeCtx, 40*time.Second)
			err := sink.WriteOptionsMinute(attempt, e)
			stop()
			if err != nil {
				failures <- err
				return
			}
			logger.Info("options minute committed", "run_id", e.RunID, "minute", e.MinuteTime, "books", len(e.Books))
		}
	}()
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
		close(minutes)
		timer := time.NewTimer(45 * time.Second)
		defer timer.Stop()
		select {
		case <-writerDone:
		case <-timer.C:
			writeCancel()
			<-writerDone
			if result == nil {
				result = fmt.Errorf("options drain timed out")
			}
		}
		writeCancel()
		select {
		case err := <-failures:
			if result == nil {
				result = err
			}
		default:
		}
	}()
	requestRefresh := func() {
		select {
		case refresh <- struct{}{}:
		default:
		}
	}
	engine, err := newEngine(p.Run, p.Specs, started, func(group string) {
		select {
		case reset[group] <- struct{}{}:
		default:
		}
	}, requestRefresh, func(e options.LiveEnvelope) error {
		select {
		case minutes <- e:
			return nil
		default:
			return fmt.Errorf("options minute queue full")
		}
	})
	if err != nil {
		return err
	}
	// Startup evidence is published through the same sequencer as later updates.
	for _, o := range p.Metadata {
		o := o
		if err = q.offer(event{Metadata: &o}); err != nil {
			return err
		}
	}
	bookChannels := make([]string, 0, len(p.Run.Members))
	for _, m := range p.Run.Members {
		bookChannels = append(bookChannels, "book."+m.Symbol+".100ms")
	}
	indexChannels := make([]string, 0, len(p.Run.Indexes))
	for _, id := range p.Run.Indexes {
		indexChannels = append(indexChannels, "deribit_price_index."+id)
	}
	for group, channels := range map[string][]string{"book": bookChannels, "index": indexChannels} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			backoff := time.Second
			for workerCtx.Err() == nil {
				// A stale reset from the retired generation must not close its successor.
				select {
				case <-reset[group]:
				default:
				}
				started := time.Now()
				err := c.Stream(workerCtx, channels, reset[group], func(s deribit.StreamEvent) error { return q.offer(event{Group: group, Stream: s}) })
				if workerCtx.Err() != nil {
					return
				}
				logger.Warn("options connection restarting", "group", group, "error", err)
				if time.Since(started) > time.Minute {
					backoff = time.Second
				}
				if !exchange.Wait(workerCtx, exchange.AddJitter(backoff)) {
					return
				}
				backoff = min(30*time.Second, backoff*2)
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		timer := time.NewTimer(30 * time.Minute)
		defer timer.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-timer.C:
			case <-refresh:
			}
			retry, err := refreshMetadata(workerCtx, c, p, sink, q)
			if err != nil {
				select {
				case failures <- err:
				default:
				}
				return
			}
			interval := 30 * time.Minute
			if retry {
				interval = time.Minute
			}
			timer.Reset(interval)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-failures:
			return err
		case err := <-q.failure:
			return err
		case <-ticker.C:
			if err = q.pulse(); err != nil {
				return err
			}
		case v := <-q.queue:
			q.consumed(v)
			if err = engine.handle(v, time.Now()); err != nil {
				return err
			}
		}
	}
}

func refreshMetadata(ctx context.Context, c *deribit.Client, p Prepared, sink Sink, q *ingress) (bool, error) {
	retry := false
	for _, scope := range p.Scopes {
		r, fetchErr := c.FetchScope(ctx, scope)
		if ctx.Err() != nil {
			return false, nil
		}
		if r.RequestedAt.IsZero() {
			return true, fetchErr
		}
		bySymbol := map[string]deribit.ParsedInstrument{}
		if fetchErr != nil {
			retry = true
		}
		for _, item := range r.Instruments {
			bySymbol[item.Spec.Instrument.ExchangeSymbol] = item
		}
		for _, s := range p.Specs {
			own, _ := symbolScope(s.Instrument.ExchangeSymbol)
			if own != scope {
				continue
			}
			o := options.MetadataObservation{AttemptID: r.RequestID, RunID: p.Run.ID, InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, Scope: scope.String(), SourceURL: r.URL, RequestedAt: r.RequestedAt, ObservedAt: r.ObservedAt, PayloadHash: r.PayloadHash, Status: r.Status, ScopeComplete: r.Status == "complete", ScopeRawCount: r.RawCount, ScopeAcceptedCount: r.AcceptedCount, ScopeExcludedCount: r.ExcludedCount}
			if fetchErr == nil {
				item, ok := bySymbol[o.Symbol]
				if !ok {
					o.Status = "missing"
					retry = true
				} else if item.Spec.DefinitionHash() != s.DefinitionHash() {
					o.Status = "definition_changed"
				} else {
					rule := item.Rule
					rule.InstrumentID = s.Instrument.ID
					if err := sink.WriteDerivativeTradingRule(ctx, rule); err != nil {
						return true, err
					}
					o.DefinitionHash = s.DefinitionHash()
					o.TradingRuleID = rule.ID()
					o.State = item.State
					o.Active = item.Active
				}
			}
			if err := sink.WriteOptionsMetadata(ctx, o); err != nil {
				return true, err
			}
			if err := q.offer(event{Metadata: &o}); err != nil {
				return true, err
			}
		}
	}
	return retry, nil
}
