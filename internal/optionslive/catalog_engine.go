package optionslive

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

var errShardRetired = errors.New("catalog shard retired")

type catalogState struct {
	InstrumentID                uint32
	Known, Open                 bool
	StateID, RuleObservationID  uuid.UUID
	StateKind                   uint8
	RuleID                      string
	StateAt, RuleAt, ObservedAt time.Time
	StateObservedAt             time.Time
	Epoch                       uuid.UUID // lifecycle evidence belongs to this epoch
}
type catalogGate struct {
	Generation                uint64
	Epoch                     uuid.UUID
	ConfirmedAt               time.Time
	Known                     bool
	BaselineID, MaintenanceID uuid.UUID
	LockMode                  string
	LockedIndexes             []string
	Maintenance               bool
	LockIDs                   map[string]uuid.UUID
	Locks                     map[string]bool
}
type catalogSwap struct {
	At     time.Time
	Run    options.LiveRun
	Specs  []options.ContractSpec
	Retire bool
}
type catalogIndex struct {
	ID         string
	Sample     *options.IndexSample
	Connection connection
}
type catalogEngine struct {
	warm        map[uint32]bool
	states      map[uint32]catalogState
	connections map[uint32]connection
	gate        catalogGate
	plans       []options.CollectionPlan
	swaps       []catalogSwap
	proof       map[uint32]*options.QualityEvidence
	complete    func(options.CatalogEnvelope) error
	recover     func(uint32)
	maxLevels   int
	owned       bool
	levelBudget *exchange.BufferBudget
	levels      map[uint32]int64
}

func newCatalogEngine(r options.LiveRun, specs []options.ContractSpec, start time.Time, maxLevels int, recover func(uint32), resetIndex func(), complete func(options.CatalogEnvelope) error) (*engine, error) {
	e, err := newEngine(r, specs, start, func(string) { resetIndex() }, func() {}, func(options.LiveEnvelope) error { return nil })
	if err != nil {
		return nil, err
	}
	e.catalog = &catalogEngine{states: map[uint32]catalogState{}, connections: map[uint32]connection{}, proof: map[uint32]*options.QualityEvidence{}, levels: map[uint32]int64{}, warm: map[uint32]bool{}, maxLevels: maxLevels, recover: recover, complete: complete}
	for _, s := range specs {
		b, err := orderbook.NewDerivative(s.Instrument.ID, false, maxLevels)
		if err != nil {
			return nil, err
		}
		e.books[s.Instrument.ID] = b
	}
	e.complete = func(v options.LiveEnvelope) error {
		var p *options.CollectionPlan
		for n := range e.catalog.plans {
			candidate := &e.catalog.plans[n]
			if !candidate.EffectiveMinute.After(v.MinuteTime) {
				p = candidate
			}
		}
		if p == nil {
			return nil
		} // Prewarming is not a committed live minute.
		if err := p.ValidateRun(e.run, v.MinuteTime); err != nil {
			return nil
		}
		out := options.CatalogEnvelope{PlanID: p.ID, PlanHash: p.RowHash, Live: v}
		for _, m := range e.run.Members {
			out.Evidence = append(out.Evidence, *e.catalog.proof[m.InstrumentID])
		}
		return complete(out)
	}
	return e, nil
}
func (e *engine) handleCatalog(v event) (bool, error) {
	c := e.catalog
	if v.Spec != nil {
		spec := *v.Spec
		c.warm[spec.Instrument.ID] = true
		if _, ok := e.specs[spec.Instrument.ID]; !ok {
			b, err := orderbook.NewDerivative(spec.Instrument.ID, false, c.maxLevels)
			if err != nil {
				return true, err
			}
			e.specs[spec.Instrument.ID] = spec
			e.books[spec.Instrument.ID] = b
		}
		return true, nil
	}
	if v.Index != nil {
		e.connections["index"] = v.Index.Connection
		if v.Index.Sample != nil {
			e.indexes[v.Index.ID] = *v.Index.Sample
		} else if !v.Index.Connection.ready {
			clear(e.indexes)
		}
		return true, nil
	}
	if v.State != nil {
		s := *v.State
		if _, ok := e.specs[s.InstrumentID]; !ok {
			return true, nil
		}
		previous := c.states[s.InstrumentID]
		if !s.Known || !s.Open {
			e.books[s.InstrumentID].Invalidate(model.DerivativeMetadataUncertain)
		}
		if s.Known && s.Open && (!previous.Known || !previous.Open) {
			e.books[s.InstrumentID].Invalidate(model.DerivativeMetadataUncertain)
			if c.connections[s.InstrumentID].epoch != uuid.Nil {
				c.recover(s.InstrumentID)
			}
		}
		if s.StateAt.IsZero() {
			s.StateAt = v.At.UTC().Truncate(time.Microsecond)
		}
		if s.RuleAt.IsZero() {
			s.RuleAt = v.At.UTC().Truncate(time.Microsecond)
		}
		c.states[s.InstrumentID] = s
		return true, nil
	}
	if v.Gate != nil {
		g := *v.Gate
		old := c.gate
		c.gate = g
		for id, s := range c.states {
			index := e.specs[id].IndexID
			blocked := !g.Known || g.Maintenance || g.LockMode == "true" || g.Locks[index] || containsString(g.LockedIndexes, index)
			wasBlocked := !old.Known || old.Maintenance || old.LockMode == "true" || old.Locks[index] || containsString(old.LockedIndexes, index)
			if blocked {
				e.books[id].Invalidate(model.DerivativeMetadataUncertain)
			} else if wasBlocked && s.Known && s.Open {
				e.books[id].Invalidate(model.DerivativeMetadataUncertain)
				if c.connections[id].epoch != uuid.Nil {
					c.recover(id)
				}
			}
		}
		return true, nil
	}
	if v.Plan != nil {
		p := *v.Plan
		if p.RowHash == "" {
			p.RowHash = p.Hash()
		}
		if len(c.plans) > 0 {
			previous := c.plans[len(c.plans)-1]
			if err := p.ValidateSuccessor(&previous); err != nil {
				return true, err
			}
		}
		c.plans = append(c.plans, p)
		return true, nil
	}
	if v.Swap != nil {
		s := *v.Swap
		if len(c.swaps) > 0 && !s.At.After(c.swaps[len(c.swaps)-1].At) {
			return true, fmt.Errorf("unsorted shard switch")
		}
		for _, spec := range s.Specs {
			if _, exists := e.specs[spec.Instrument.ID]; !exists {
				b, err := orderbook.NewDerivative(spec.Instrument.ID, false, c.maxLevels)
				if err != nil {
					return true, err
				}
				e.books[spec.Instrument.ID] = b
				e.specs[spec.Instrument.ID] = spec
			}
		}
		c.swaps = append(c.swaps, s)
		return true, nil
	}
	if v.InstrumentID == 0 {
		return false, nil
	}
	id := v.InstrumentID
	if _, ok := e.specs[id]; !ok {
		return true, nil
	}
	f := v.Stream
	received := f.ReceivedAt
	if received.IsZero() {
		received = v.At
	}
	conn := c.connections[id]
	if f.Kind == "connected" {
		c.levelBudget.Release(c.levels[id])
		delete(c.levels, id)
		conn = connection{epoch: f.Epoch, confirmed: received, started: received}
		c.connections[id] = conn
		return true, e.books[id].Reset(f.Epoch)
	}
	if f.Epoch != conn.epoch {
		return true, nil
	}
	conn.confirmed = received
	if f.Kind == "ready" || f.Kind == "subscribed" {
		conn.ready = true
	}
	if f.Kind == "disconnected" || f.Kind == "unsubscribed" {
		conn.ready = false
		e.books[id].Invalidate(model.DerivativeDisconnected)
	}
	c.connections[id] = conn
	if f.Kind != "message" {
		return true, nil
	}
	u, err := deribit.DecodeBook(f.Raw, e.specs[id].Instrument, f.Epoch, received)
	if err == nil {
		old := c.levels[id]
		estimate := old
		if u.Snapshot {
			estimate = int64(len(u.Bids) + len(u.Asks))
		} else {
			for _, side := range [][]orderbook.DerivativeChangeLevel{u.Bids, u.Asks} {
				for _, l := range side {
					if l.Action == orderbook.DerivativeNew {
						estimate++
					} else if l.Action == orderbook.DerivativeDelete {
						estimate--
					}
				}
			}
		}
		extra := estimate - old
		if extra < 0 {
			extra = 0
		}
		if !c.levelBudget.Reserve(extra) {
			e.books[id].Invalidate(model.DerivativeResourceLimit)
			err = fmt.Errorf("global full L2 level budget exceeded")
		} else {
			err = e.books[id].Apply(u)
			actual := int64(e.books[id].LevelCount())
			c.levelBudget.Release(old + extra - actual)
			c.levels[id] = actual
		}
	} else {
		e.books[id].Invalidate(model.DerivativeParseError)
	}
	if err != nil {
		c.recover(id)
	}
	return true, nil
}
func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
func (e *engine) catalogBoundary(at time.Time) error {
	c := e.catalog
	for len(c.swaps) > 0 && !c.swaps[0].At.After(at) {
		s := c.swaps[0]
		c.swaps = c.swaps[1:]
		if s.Retire {
			return errShardRetired
		}
		if at.Second() != 0 {
			return fmt.Errorf("late membership switch")
		}
		e.run = s.Run
		for _, m := range e.run.Members {
			delete(c.warm, m.InstrumentID)
		}
	}
	if at.Second() == 0 {
		c.owned = false
		var active *options.CollectionPlan
		for n := range c.plans {
			if !c.plans[n].EffectiveMinute.After(at) {
				active = &c.plans[n]
			}
		}
		if active != nil {
			c.owned = active.ValidateRun(e.run, at) == nil
		}
		keep := map[uint32]bool{}
		for id := range c.warm {
			if e.specs[id].Instrument.ExpiryTime.After(at) {
				keep[id] = true
			} else {
				delete(c.warm, id)
			}
		}
		for _, m := range e.run.Members {
			keep[m.InstrumentID] = true
		}
		for _, s := range c.swaps {
			for _, spec := range s.Specs {
				keep[spec.Instrument.ID] = true
			}
		}
		for id := range e.books {
			if !keep[id] {
				c.levelBudget.Release(c.levels[id])
				delete(c.levels, id)
				delete(e.books, id)
				delete(e.specs, id)
				delete(c.states, id)
				delete(c.connections, id)
			}
		}
		if active != nil {
			i := 0
			for n := range c.plans {
				if c.plans[n].ID == active.ID {
					i = n
				}
			}
			c.plans = c.plans[i:]
		}
		c.proof = map[uint32]*options.QualityEvidence{}
		for _, m := range e.run.Members {
			c.proof[m.InstrumentID] = &options.QualityEvidence{RunID: e.run.ID, InstrumentID: m.InstrumentID, MinuteTime: at, StateIDs: make([]*uuid.UUID, 60), StateKinds: make([]uint8, 60), RuleObservationIDs: make([]*uuid.UUID, 60), PlatformIDs: make([]*uuid.UUID, 60), MaintenanceIDs: make([]*uuid.UUID, 60), LockIDs: make([]*uuid.UUID, 60), LifecycleEpochs: make([]uuid.UUID, 60), LifecycleConfirmed: make([]*time.Time, 60)}
		}
	}
	return nil
}
func uuidPointer(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	v := id
	return &v
}
func (e *engine) catalogQuality(id uint32, at time.Time, q model.DerivativeQuality) model.DerivativeQuality {
	c := e.catalog
	s := c.states[id]
	conn := c.connections[id]
	g := c.gate
	index := e.specs[id].IndexID
	q.ConnectionConfirmedAt = conn.confirmed.UTC().Truncate(time.Microsecond)
	stateFresh := s.StateKind != 2 || !s.StateObservedAt.IsZero() && !s.StateObservedAt.After(at) && at.Sub(s.StateObservedAt) <= options.CatalogEvidenceMaxAge
	known := c.owned && s.Known && stateFresh && g.Known && s.Epoch == g.Epoch && !g.ConfirmedAt.After(at) && at.Sub(g.ConfirmedAt) <= 30*time.Second && !s.ObservedAt.After(at) && at.Sub(s.ObservedAt) <= options.CatalogEvidenceMaxAge
	if conn.ready && at.Sub(conn.confirmed) > 30*time.Second {
		conn.ready = false
		c.connections[id] = conn
		e.books[id].Invalidate(model.DerivativeDisconnected)
		c.recover(id)
	}
	q.MarketKnown = known
	if known {
		q.MarketOpen = s.Open && !g.Maintenance && g.LockMode != "true" && !containsString(g.LockedIndexes, index) && !g.Locks[index] && at.Before(*e.specs[id].Instrument.ExpiryTime)
		q.TradingRuleID = s.RuleID
		q.RulePublishedAt = s.RuleAt.UTC().Truncate(time.Microsecond)
		q.MarketStateAt = s.StateAt.UTC().Truncate(time.Microsecond)
		q.MarketStateBasis = s.StateKind
		proof := c.proof[id]
		sec := at.Second()
		proof.StateIDs[sec] = uuidPointer(s.StateID)
		proof.StateKinds[sec] = s.StateKind
		proof.RuleObservationIDs[sec] = uuidPointer(s.RuleObservationID)
		proof.PlatformIDs[sec] = uuidPointer(g.BaselineID)
		proof.MaintenanceIDs[sec] = uuidPointer(g.MaintenanceID)
		proof.LockIDs[sec] = uuidPointer(g.LockIDs[index])
		proof.LifecycleEpochs[sec] = g.Epoch
		confirmed := g.ConfirmedAt.UTC().Truncate(time.Microsecond)
		proof.LifecycleConfirmed[sec] = &confirmed
	}
	if !conn.ready {
		q.StreamValid = false
		q.Reason = model.DerivativeNotReady
	}
	if !known {
		q.StreamValid = false
		q.Reason = model.DerivativeMetadataUncertain
	}
	if known && !q.MarketOpen {
		q.StreamValid = false
		q.Reason = model.DerivativeMarketClosed
	}
	if !at.Before(*e.specs[id].Instrument.ExpiryTime) {
		q.StreamValid = false
		q.MarketOpen = false
		q.Reason = model.DerivativeMarketClosed
	}
	return q
}
