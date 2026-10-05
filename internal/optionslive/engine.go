package optionslive

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

type connection struct {
	epoch              uuid.UUID
	ready              bool
	confirmed, started time.Time
}
type metaState struct {
	observation options.MetadataObservation
	published   time.Time
	known       bool
}
type engine struct {
	run          options.LiveRun
	specs        map[uint32]options.ContractSpec
	books        map[uint32]*orderbook.DerivativeBook
	byChannel    map[string]uint32
	metadata     map[uint32]metaState
	connections  map[string]connection
	indexes      map[string]options.IndexSample
	buffers      map[uint32]*sampler.DerivativeMinuteBuffer
	indexMinutes []options.IndexMinute
	minute, next time.Time
	reset        func(string)
	refresh      func()
	complete     func(options.LiveEnvelope) error
	now          func() time.Time
	catalog      *catalogEngine
}

func newEngine(run options.LiveRun, specs []options.ContractSpec, started time.Time, reset func(string), refresh func(), complete func(options.LiveEnvelope) error) (*engine, error) {
	if err := run.Validate(); err != nil {
		return nil, err
	}
	e := &engine{run: run, specs: map[uint32]options.ContractSpec{}, books: map[uint32]*orderbook.DerivativeBook{}, byChannel: map[string]uint32{}, metadata: map[uint32]metaState{}, connections: map[string]connection{}, indexes: map[string]options.IndexSample{}, reset: reset, refresh: refresh, complete: complete, next: run.StartedAt.Truncate(time.Minute).Add(time.Minute)}
	for _, s := range specs {
		b, err := orderbook.NewDerivative(s.Instrument.ID, false, 20000)
		if err != nil {
			return nil, err
		}
		e.books[s.Instrument.ID] = b
		e.specs[s.Instrument.ID] = s
		e.byChannel["book."+s.Instrument.ExchangeSymbol+".100ms"] = s.Instrument.ID
	}
	if len(e.books) != len(run.Members) {
		return nil, fmt.Errorf("engine membership mismatch")
	}
	e.now = time.Now
	if first := started.UTC().Truncate(time.Minute).Add(time.Minute); first.After(e.next) {
		e.next = first
	}
	return e, nil
}
func (e *engine) handle(v event, now time.Time) error {
	if v.Boundary {
		return e.sample(v.At, now)
	}
	if e.catalog != nil {
		handled, err := e.handleCatalog(v)
		if handled {
			return err
		}
	}
	if v.Metadata != nil {
		o := *v.Metadata
		s, ok := e.specs[o.InstrumentID]
		if !ok || o.RunID != e.run.ID || o.ObservedAt.After(v.At) {
			return fmt.Errorf("unexpected metadata publication")
		}
		if o.Status == "definition_changed" {
			return fmt.Errorf("economic definition changed: %s; restart fixed run", o.Symbol)
		}
		known := o.Status == "complete" && o.DefinitionHash == s.DefinitionHash() && !o.ObservedAt.Before(e.connections["book"].started)
		e.metadata[o.InstrumentID] = metaState{observation: o, published: v.At, known: known}
		return nil
	}
	f := v.Stream
	c := e.connections[v.Group]
	if f.Kind == "connected" {
		c = connection{epoch: f.Epoch, confirmed: v.At, started: v.At}
		e.connections[v.Group] = c
		if v.Group == "book" {
			for id, b := range e.books {
				if err := b.Reset(f.Epoch); err != nil {
					return err
				}
				m := e.metadata[id]
				m.known = false
				e.metadata[id] = m
			}
			e.refresh()
		} else {
			for _, id := range e.run.Indexes {
				e.indexes[id] = options.IndexSample{Epoch: f.Epoch, State: options.IndexMissing}
			}
		}
		return nil
	}
	if f.Epoch != c.epoch {
		return nil
	}
	if f.Kind == "disconnected" {
		c.ready = false
		e.connections[v.Group] = c
		if v.Group == "book" {
			for id, b := range e.books {
				b.Invalidate(model.DerivativeDisconnected)
				m := e.metadata[id]
				m.known = false
				e.metadata[id] = m
			}
		} else {
			for _, id := range e.run.Indexes {
				e.indexes[id] = options.IndexSample{Epoch: c.epoch, State: options.IndexDisconnected}
			}
		}
		return nil
	}
	c.confirmed = v.At
	if f.Kind == "ready" {
		c.ready = true
	}
	e.connections[v.Group] = c
	if f.Kind != "message" {
		return nil
	}
	if v.Group == "book" {
		id, ok := e.byChannel[f.Channel]
		if !ok {
			return fmt.Errorf("unknown book channel")
		}
		u, err := deribit.DecodeBook(f.Raw, e.specs[id].Instrument, f.Epoch, v.At)
		if err == nil {
			err = e.books[id].Apply(u)
		} else {
			e.books[id].Invalidate(model.DerivativeParseError)
		}
		if err != nil {
			e.reset("book")
		}
	} else {
		id := strings.TrimPrefix(f.Channel, "deribit_price_index.")
		if !options.ValidIndex(id) {
			return fmt.Errorf("unknown index channel")
		}
		s, err := deribit.DecodeIndex(f.Raw, id, f.Epoch, v.At)
		if err != nil {
			e.indexes[id] = options.IndexSample{Epoch: c.epoch, State: options.IndexInvalid}
			e.reset("index")
		} else {
			e.indexes[id] = s
		}
	}
	return nil
}
func (e *engine) sample(at, now time.Time) error {
	if at.Before(e.next) {
		return nil
	}
	if !at.Equal(e.next) {
		return fmt.Errorf("missing options second boundary")
	}
	e.next = e.next.Add(time.Second)
	if e.catalog != nil {
		if err := e.catalogBoundary(at); err != nil {
			return err
		}
	}
	if at.Second() == 0 {
		e.minute = at
		e.buffers = map[uint32]*sampler.DerivativeMinuteBuffer{}
		e.indexMinutes = nil
		for _, m := range e.run.Members {
			b, err := sampler.NewDerivativeMinuteBuffer(m.InstrumentID, at, false)
			if err != nil {
				return err
			}
			e.buffers[m.InstrumentID] = b
		}
		for _, id := range e.run.Indexes {
			e.indexMinutes = append(e.indexMinutes, options.IndexMinute{IndexID: id})
		}
	}
	if e.buffers == nil {
		return fmt.Errorf("sampling lacks minute start")
	}
	// A reader may be waiting on control-request cooldown; enforce liveness
	// here as well, independently of its next ReadMessage call.
	for group, c := range e.connections {
		if c.ready && at.Sub(c.confirmed) > 30*time.Second {
			c.ready = false
			e.connections[group] = c
			e.reset(group)
			if group == "book" {
				for id, b := range e.books {
					b.Invalidate(model.DerivativeDisconnected)
					m := e.metadata[id]
					m.known = false
					e.metadata[id] = m
				}
			} else {
				for _, id := range e.run.Indexes {
					e.indexes[id] = options.IndexSample{Epoch: c.epoch, State: options.IndexDisconnected}
				}
			}
		}
	}
	late := now.Before(at) || now.After(at.Add(250*time.Millisecond))
	for _, member := range e.run.Members {
		id := member.InstrumentID
		var s model.DerivativeSnapshot
		var q model.DerivativeQuality
		if late {
			q = model.DerivativeQuality{CapturedAt: now.UTC().Truncate(time.Microsecond), Reason: model.DerivativeSamplingLag}
		} else {
			s, q = e.books[id].Current()
			q.Sampled = true
			q.CapturedAt = now.UTC().Truncate(time.Microsecond)
			if e.catalog != nil {
				q = e.catalogQuality(id, at, q)
			} else {
				c := e.connections["book"]
				q.ConnectionConfirmedAt = c.confirmed.UTC().Truncate(time.Microsecond)
				m := e.metadata[id]
				if m.known {
					q.MarketKnown = true
					q.MarketOpen = m.observation.Active && m.observation.State == "open"
					q.MarketStateAt = m.published.UTC().Truncate(time.Microsecond)
					q.MarketStateBasis = 2
					q.TradingRuleID = m.observation.TradingRuleID
					q.RulePublishedAt = q.MarketStateAt
				}
				if !c.ready {
					q.StreamValid = false
					q.Reason = model.DerivativeNotReady
				}
				if !m.known {
					q.StreamValid = false
					q.Reason = model.DerivativeMetadataUncertain
				}
				if m.known && (!q.MarketOpen || !at.Before(*e.specs[id].Instrument.ExpiryTime)) {
					q.MarketOpen = false
					q.StreamValid = false
					q.Reason = model.DerivativeMarketClosed
				}
				if !at.Before(*e.specs[id].Instrument.ExpiryTime) {
					q.StreamValid = false
					q.MarketOpen = false
					q.Reason = model.DerivativeMarketClosed
				}
			}
			s.ReceivedAt = s.ReceivedAt.UTC().Truncate(time.Microsecond)
			q.ReceivedAt = q.ReceivedAt.UTC().Truncate(time.Microsecond)
			q.LastSnapshotAt = q.LastSnapshotAt.UTC().Truncate(time.Microsecond)
		}
		// Read the clock AFTER this book and its control state have been frozen.
		// The arrival time at the top of this function is not completion time.
		finished := e.now()
		q.CapturedAt = finished.UTC().Truncate(time.Microsecond)
		if finished.Before(at) || finished.After(at.Add(250*time.Millisecond)) {
			q.Sampled = false
			q.StreamValid = false
			q.Reason = model.DerivativeSamplingLag
		}
		if err := e.buffers[id].Sample(at, s, q); err != nil {
			return err
		}
	}
	for n, id := range e.run.Indexes {
		s := e.indexes[id]
		finished := e.now()
		if late || finished.Before(at) || finished.After(at.Add(250*time.Millisecond)) {
			s = options.IndexSample{State: options.IndexInvalid}
		} else if !e.connections["index"].ready {
			s = options.IndexSample{State: options.IndexDisconnected}
		} else if s.Price != nil {
			if s.ReceivedAt.After(at.Add(-time.Second)) {
				s.State = options.IndexObserved
			} else {
				s.State = options.IndexHeld
			}
			s.ReceivedAt = s.ReceivedAt.UTC().Truncate(time.Microsecond)
		}
		e.indexMinutes[n].Samples[at.Second()] = s
	}
	if at.Second() == 59 {
		batch := options.LiveEnvelope{RunID: e.run.ID, RunHash: e.run.Hash(), MinuteTime: e.minute, PreparedAt: e.now().UTC().Truncate(time.Microsecond), Indexes: e.indexMinutes}
		for _, m := range e.run.Members {
			b, err := e.buffers[m.InstrumentID].Complete()
			if err != nil {
				return err
			}
			batch.Books = append(batch.Books, b)
		}
		if e.catalog == nil {
			if err := batch.ValidateRun(e.run); err != nil {
				return err
			}
		}
		return e.complete(batch)
	}
	return nil
}
