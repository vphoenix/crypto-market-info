package clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

type catalogCommit struct {
	RunID          uuid.UUID `ch:"run_id"`
	MinuteTime     time.Time `ch:"minute_time"`
	BatchID        string    `ch:"batch_id"`
	PlanID         uuid.UUID `ch:"plan_id"`
	PlanHash       string    `ch:"plan_hash"`
	RunHash        string    `ch:"run_hash"`
	PreparedAt     time.Time `ch:"prepared_at"`
	InstrumentIDs  []uint32  `ch:"instrument_ids"`
	MemberHashes   []string  `ch:"member_hashes"`
	EvidenceHashes []string  `ch:"evidence_hashes"`
	AnchorCount    uint32    `ch:"anchor_count"`
	DeltaCount     uint32    `ch:"delta_count"`
	IndexIDs       []string  `ch:"index_ids"`
	IndexHashes    []string  `ch:"index_hashes"`
}

func (c *Client) WriteOptionsCatalogMinute(ctx context.Context, e options.CatalogEnvelope) error {
	return c.WriteOptionsCatalogMinuteLazy(ctx, e.Live.RunID, e.Live.MinuteTime, func() (options.CatalogEnvelope, error) { return e, nil })
}

// Expand only after admission: waiting workers retain compressed typed minutes.
func (c *Client) WriteOptionsCatalogMinuteLazy(ctx context.Context, run uuid.UUID, minute time.Time, load func() (options.CatalogEnvelope, error)) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}

	unlock, err := c.catalogLock(ctx, "minute:"+run.String()+minute.String())
	if err != nil {
		return err
	}
	defer unlock()
	c.catalogSlotsOnce.Do(func() { c.catalogSlots = make(chan struct{}, 3) })
	// Waiting for the shared writer slot is not database execution time. The
	// caller owns cancellation/drain; each admitted attempt has its own budget.
	ctx, release, err := acquireCatalogWriteSlot(ctx, c.catalogSlots, 40*time.Second)
	if err != nil {
		return err
	}
	defer release()
	e, err := load()
	if err != nil {
		return err
	}
	if e.Live.RunID != run || !e.Live.MinuteTime.Equal(minute) {
		return fmt.Errorf("lazy catalog minute identity mismatch")
	}
	if err := e.Validate(); err != nil {
		return err
	}
	r, err := c.LoadOptionsRun(ctx, e.Live.RunID)
	if err != nil {
		return err
	}
	if err = e.Live.ValidateRun(r); err != nil {
		return err
	}
	p, err := c.OptionsPlanAt(ctx, e.Live.MinuteTime)
	if err != nil {
		return err
	}
	if p.ID != e.PlanID || p.Hash() != e.PlanHash {
		return fmt.Errorf("minute plan mismatch")
	}
	if err = p.ValidateRun(r, e.Live.MinuteTime); err != nil {
		return err
	}
	id := e.ID()
	old, err := keeperRead[catalogCommit](ctx, c, "options_catalog_live_minute_commit", "WHERE run_id=? AND minute_time=?", r.ID, e.Live.MinuteTime)
	if err != nil {
		return err
	}
	if len(old) > 0 {
		if len(old) != 1 || old[0].BatchID != id {
			return fmt.Errorf("immutable catalog minute conflict")
		}
		_, err = c.LoadOptionsCatalogMinute(ctx, r.ID, e.Live.MinuteTime)
		return err
	}
	if err = c.validateDerivativeReferences(ctx, e.Live.Books); err != nil {
		return err
	}
	if err = c.validateCatalogEvidence(ctx, r, e); err != nil {
		return err
	}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		if err := c.insertDerivativeDeltas(ctx, id, e.Live.Books); err != nil {
			return err
		}
		if err := c.derivativeInserted("catalog_delta"); err != nil {
			return err
		}
		if err := c.insertDerivativeMinutes(ctx, id, e.Live.Books); err != nil {
			return err
		}
		if err := c.insertDerivativeQuality(ctx, id, e.Live.Books); err != nil {
			return err
		}
		rows := append([]options.QualityEvidence(nil), e.Evidence...)
		var evidenceHashes []string
		for n := range rows {
			rows[n].BatchID = id
			rows[n].RowHash = rows[n].Hash()
			evidenceHashes = append(evidenceHashes, rows[n].RowHash)
		}
		if err := c.dexInsert(ctx, "options_catalog_quality_evidence_minute", keeperColumns(reflect.TypeOf(options.QualityEvidence{})), keeperValues(rows)); err != nil {
			return err
		}
		if err := c.derivativeInserted("catalog_evidence"); err != nil {
			return err
		}
		indexIDs, indexHashes, err := c.insertCatalogIndexes(ctx, id, e.Live)
		if err != nil {
			return err
		}
		ids, hashes, anchors, deltas := derivativeManifest(options.BookEnvelope{Batches: e.Live.Books})
		commit := catalogCommit{r.ID, e.Live.MinuteTime, id, p.ID, p.Hash(), e.Live.RunHash, e.Live.PreparedAt, ids, hashes, evidenceHashes, anchors, deltas, indexIDs, indexHashes}
		if err = c.dexInsert(ctx, "options_catalog_live_minute_commit", keeperColumns(reflect.TypeOf(commit)), keeperValues([]catalogCommit{commit})); err != nil {
			return err
		}
		return c.derivativeInserted("catalog_commit")
	})
}

func acquireCatalogWriteSlot(ctx context.Context, slots chan struct{}, executionBudget time.Duration) (context.Context, func(), error) {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	attempt, cancel := context.WithTimeout(ctx, executionBudget)
	return attempt, func() { cancel(); <-slots }, nil
}
func (c *Client) LoadOptionsCatalogMinute(ctx context.Context, run uuid.UUID, minute time.Time) (options.CatalogEnvelope, error) {
	var e options.CatalogEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	rows, err := keeperRead[catalogCommit](ctx, c, "options_catalog_live_minute_commit", "WHERE run_id=? AND minute_time=?", run, minute.UTC())
	if err != nil {
		return e, err
	}
	if len(rows) == 0 {
		return e, ErrNotFound
	}
	if len(rows) != 1 {
		return e, fmt.Errorf("%w: duplicate catalog commit", replay.ErrIncompleteDerivativeBatch)
	}
	v := rows[0]
	e.PlanID, e.PlanHash = v.PlanID, v.PlanHash
	e.Live = options.LiveEnvelope{RunID: v.RunID, RunHash: v.RunHash, MinuteTime: v.MinuteTime.UTC(), PreparedAt: v.PreparedAt.UTC()}
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return e, err
	}
	p, err := c.OptionsPlanAt(ctx, minute)
	if err != nil {
		return e, err
	}
	if p.ID != e.PlanID || p.Hash() != e.PlanHash {
		return e, fmt.Errorf("%w: plan identity", replay.ErrIncompleteDerivativeBatch)
	}
	if err = p.ValidateRun(r, minute); err != nil {
		return e, err
	}
	books, err := c.loadDerivativeBookRows(ctx, options.BookEnvelope{MinuteTime: minute}, v.BatchID, v.InstrumentIDs, v.MemberHashes, v.AnchorCount, v.DeltaCount)
	if err != nil {
		return e, err
	}
	e.Live.Books = books.Batches
	e.Live.Indexes, err = c.loadCatalogIndexes(ctx, minute, v.BatchID, v.IndexIDs, v.IndexHashes)
	if err != nil {
		return e, err
	}
	e.Evidence, err = keeperRead[options.QualityEvidence](ctx, c, "options_catalog_quality_evidence_minute", "WHERE run_id=? AND minute_time=? AND batch_id=? ORDER BY instrument_id", run, minute.UTC(), v.BatchID)
	if err != nil {
		return e, err
	}
	if len(e.Evidence) != len(v.EvidenceHashes) {
		return e, fmt.Errorf("%w: missing quality evidence", replay.ErrIncompleteDerivativeBatch)
	}
	for n, q := range e.Evidence {
		if q.BatchID != v.BatchID || q.RowHash != q.Hash() || q.Hash() != v.EvidenceHashes[n] {
			return e, fmt.Errorf("%w: quality evidence digest", replay.ErrIncompleteDerivativeBatch)
		}
	}
	if err = e.Validate(); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	if err = e.Live.ValidateRun(r); err != nil {
		return e, err
	}
	if e.ID() != v.BatchID {
		return e, fmt.Errorf("%w: catalog digest", replay.ErrIncompleteDerivativeBatch)
	}
	if err = c.validateCatalogEvidence(ctx, r, e); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	return e, nil
}

func (c *Client) validateCatalogEvidence(ctx context.Context, r options.LiveRun, e options.CatalogEnvelope) error {
	var catalogIDs, lifeIDs []uuid.UUID
	for _, q := range e.Evidence {
		for sec := 0; sec < 60; sec++ {
			if q.RuleObservationIDs[sec] != nil {
				catalogIDs = append(catalogIDs, *q.RuleObservationIDs[sec])
			}
			if q.StateIDs[sec] != nil {
				if q.StateKinds[sec] == 2 {
					catalogIDs = append(catalogIDs, *q.StateIDs[sec])
				} else {
					lifeIDs = append(lifeIDs, *q.StateIDs[sec])
				}
			}
			for _, id := range []*uuid.UUID{q.PlatformIDs[sec], q.MaintenanceIDs[sec], q.LockIDs[sec]} {
				if id != nil {
					lifeIDs = append(lifeIDs, *id)
				}
			}
		}
	}
	unique := func(ids []uuid.UUID) []uuid.UUID {
		slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
		return slices.Compact(ids)
	}
	catalog, err := loadCatalogRows[options.CatalogObservation](ctx, c, "options_catalog_scope_observation", unique(catalogIDs))
	if err != nil {
		return err
	}
	lifecycle, err := loadCatalogRows[options.LifecycleObservation](ctx, c, "options_lifecycle_observation", unique(lifeIDs))
	if err != nil {
		return err
	}
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		return err
	}
	for n, b := range e.Live.Books {
		proof := e.Evidence[n]
		for sec, q := range b.Quality {
			if !q.MarketKnown {
				if q.StreamValid {
					return fmt.Errorf("stream valid with unknown market")
				}
				continue
			}
			at := e.Live.MinuteTime.Add(time.Duration(sec) * time.Second)
			o, ok := catalog[*proof.RuleObservationIDs[sec]]
			if !ok || o.Status != "complete" || q.RulePublishedAt.Before(o.ObservedAt) || at.Sub(o.ObservedAt) > options.CatalogEvidenceMaxAge {
				return fmt.Errorf("missing/future/stale rule observation")
			}
			i := slices.Index(o.InstrumentIDs, b.InstrumentID)
			if i < 0 || o.Definitions[i] != r.Members[n].DefinitionHash || o.Rules[i] != q.TradingRuleID {
				return fmt.Errorf("wrong economic rule evidence")
			}
			open := false
			if proof.StateKinds[sec] == 2 {
				state := catalog[*proof.StateIDs[sec]]
				j := slices.Index(state.InstrumentIDs, b.InstrumentID)
				if state.Status != "complete" || j < 0 || state.Definitions[j] != r.Members[n].DefinitionHash || q.MarketStateAt.Before(state.ObservedAt) || at.Sub(state.ObservedAt) > options.CatalogEvidenceMaxAge {
					return fmt.Errorf("wrong catalog state evidence")
				}
				open = state.Active[j] && state.States[j] == "open"
			} else {
				state := lifecycle[*proof.StateIDs[sec]]
				if state.Kind != "state" || state.Symbol != r.Members[n].Symbol || q.MarketStateAt.Before(state.ReceivedAt) || state.Epoch != proof.LifecycleEpochs[sec] {
					return fmt.Errorf("wrong lifecycle state evidence")
				}
				open = state.State == "open"
			}
			platform := lifecycle[*proof.PlatformIDs[sec]]
			confirmed := proof.LifecycleConfirmed[sec]
			if platform.Kind != "status" || platform.Epoch != proof.LifecycleEpochs[sec] || platform.ReceivedAt.After(at) || confirmed == nil || confirmed.After(at) || at.Sub(*confirmed) > 30*time.Second {
				return fmt.Errorf("unknown/stale platform baseline")
			}
			if platform.LockMode == "true" || (platform.LockMode == "partial" && slices.Contains(platform.LockedIndexes, r.Members[n].IndexID)) {
				open = false
			}
			for _, id := range []*uuid.UUID{proof.MaintenanceIDs[sec], proof.LockIDs[sec]} {
				if id == nil {
					continue
				}
				v := lifecycle[*id]
				if v.Kind != "platform" || v.Epoch != platform.Epoch || v.ReceivedAt.After(at) {
					return fmt.Errorf("invalid platform event reference")
				}
				if v.Maintenance != nil && *v.Maintenance {
					open = false
				}
				if v.Locked != nil && v.IndexID == r.Members[n].IndexID && *v.Locked {
					open = false
				}
			}
			open = open && at.Before(*specs[n].Instrument.ExpiryTime)
			if q.MarketOpen != open {
				return fmt.Errorf("market state disagrees with persisted evidence")
			}
		}
	}
	return nil
}

func (c *Client) insertCatalogIndexes(ctx context.Context, id string, e options.LiveEnvelope) ([]string, []string, error) {
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("options_index_minute")+` (`+liveIndexColumns+`)`)
	if err != nil {
		return nil, nil, err
	}
	defer b.Abort()
	var ids, hashes []string
	for _, m := range e.Indexes {
		prices := make([]*decimal.Decimal, 60)
		sources, received := make([]*time.Time, 60), make([]*time.Time, 60)
		epochs := make([]uuid.UUID, 60)
		states := make([]uint8, 60)
		for n, s := range m.Samples {
			prices[n] = s.Price
			if !s.SourceTime.IsZero() {
				v := s.SourceTime
				sources[n] = &v
			}
			if !s.ReceivedAt.IsZero() {
				v := s.ReceivedAt
				received[n] = &v
			}
			epochs[n] = s.Epoch
			states[n] = uint8(s.State)
		}
		if err = b.Append(m.IndexID, e.MinuteTime, id, m.Hash(), prices, sources, received, epochs, states); err != nil {
			return nil, nil, err
		}
		ids = append(ids, m.IndexID)
		hashes = append(hashes, m.Hash())
	}
	return ids, hashes, b.Send()
}
func (c *Client) loadCatalogIndexes(ctx context.Context, minute time.Time, id string, ids, hashes []string) ([]options.IndexMinute, error) {
	if len(ids) != len(hashes) || len(ids) == 0 {
		return nil, fmt.Errorf("invalid index manifest")
	}
	rows, err := c.conn.Query(ctx, `SELECT `+liveIndexColumns+` FROM `+c.table("options_index_minute")+` FINAL WHERE minute_time=? AND batch_id=? ORDER BY index_id`, minute.UTC(), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []options.IndexMinute
	for rows.Next() {
		var m options.IndexMinute
		var t time.Time
		var batch, hash string
		var prices []*decimal.Decimal
		var sources, received []*time.Time
		var epochs []uuid.UUID
		var states []uint8
		if err = rows.Scan(&m.IndexID, &t, &batch, &hash, &prices, &sources, &received, &epochs, &states); err != nil {
			return nil, err
		}
		n := len(out)
		if n >= len(ids) || m.IndexID != ids[n] || hash != hashes[n] || batch != id || !t.Equal(minute) || len(prices) != 60 || len(sources) != 60 || len(received) != 60 || len(epochs) != 60 || len(states) != 60 {
			return nil, fmt.Errorf("%w: index identity/slots", replay.ErrIncompleteDerivativeBatch)
		}
		for sec := 0; sec < 60; sec++ {
			s := &m.Samples[sec]
			s.Price = prices[sec]
			s.Epoch = epochs[sec]
			s.State = options.IndexState(states[sec])
			if sources[sec] != nil {
				s.SourceTime = sources[sec].UTC()
			}
			if received[sec] != nil {
				s.ReceivedAt = received[sec].UTC()
			}
		}
		if err = m.Validate(minute); err != nil {
			return nil, err
		}
		if m.Hash() != hash {
			return nil, fmt.Errorf("%w: index digest", replay.ErrIncompleteDerivativeBatch)
		}
		out = append(out, m)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("%w: missing index", replay.ErrIncompleteDerivativeBatch)
	}
	return out, nil
}
