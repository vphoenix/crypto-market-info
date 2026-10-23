package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"time"
)

const liveCommitColumns = "run_id,minute_time,batch_id,run_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count,index_ids,index_hashes"
const liveIndexColumns = "index_id,minute_time,batch_id,row_hash,prices,source_times,received_times,epochs,states"

func digestBytes(id string) []byte { v, _ := hex.DecodeString(id); return v }

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

func (c *Client) insertCatalogIndexes(ctx context.Context, tx *sql.Tx, id string, e options.LiveEnvelope) ([]string, []string, error) {
	ids, hashes := []string{}, []string{}
	for _, m := range e.Indexes {
		prices := make([]*decimal.Decimal, 60)
		sources, received := make([]*time.Time, 60), make([]*time.Time, 60)
		epochs := make([]uuid.UUID, 60)
		states := make([]uint8, 60)
		for n, s := range m.Samples {
			prices[n] = s.Price
			sources[n] = nullableDerivativeTime(s.SourceTime)
			received[n] = nullableDerivativeTime(s.ReceivedAt)
			epochs[n] = s.Epoch
			states[n] = uint8(s.State)
		}
		if err := c.putValues(ctx, tx, "options_index_minute", liveIndexColumns, []any{m.IndexID, e.MinuteTime, id, m.Hash(), prices, sources, received, epochs, states}); err != nil {
			return nil, nil, err
		}
		ids = append(ids, m.IndexID)
		hashes = append(hashes, m.Hash())
	}
	return ids, hashes, nil
}
func (c *Client) loadCatalogIndexes(ctx context.Context, minute time.Time, id string, ids, hashes []string) ([]options.IndexMinute, error) {
	if len(ids) == 0 || len(ids) != len(hashes) {
		return nil, fmt.Errorf("invalid index manifest")
	}
	rows, err := c.records(ctx, c.db, "options_index_minute", "WHERE minute_time=? AND batch_id=? ORDER BY index_id", minute, []byte(id))
	if err != nil {
		return nil, err
	}
	if len(rows) != len(ids) {
		return nil, fmt.Errorf("index count mismatch")
	}
	out := make([]options.IndexMinute, len(ids))
	for n, v := range rows {
		m := &out[n]
		var t time.Time
		var batch, hash string
		var prices []*decimal.Decimal
		var sources, received []*time.Time
		var epochs []uuid.UUID
		var states []uint8
		if err = scanRecord(v, liveIndexColumns, &m.IndexID, &t, &batch, &hash, &prices, &sources, &received, &epochs, &states); err != nil {
			return nil, err
		}
		if m.IndexID != ids[n] || hash != hashes[n] || batch != id || !t.Equal(minute) || len(prices) != 60 || len(sources) != 60 || len(received) != 60 || len(epochs) != 60 || len(states) != 60 {
			return nil, fmt.Errorf("index identity/slots mismatch")
		}
		for sec := 0; sec < 60; sec++ {
			m.Samples[sec].Price = prices[sec]
			m.Samples[sec].SourceTime = derivativeTime(sources[sec])
			m.Samples[sec].ReceivedAt = derivativeTime(received[sec])
			m.Samples[sec].Epoch = epochs[sec]
			m.Samples[sec].State = options.IndexState(states[sec])
		}
		if err = m.Validate(minute); err != nil {
			return nil, err
		}
		if m.Hash() != hash {
			return nil, fmt.Errorf("index digest mismatch")
		}
	}
	return out, nil
}
func (c *Client) WriteOptionsMinute(ctx context.Context, e options.LiveEnvelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	e = e.Clone()
	if c.queue != nil {
		raw, err := codec.EncodeOptionsLive(e)
		if err != nil {
			return err
		}
		return c.queue.Accept(ctx, c.Delivery("options_live", e.RunID.String(), e.MinuteTime, uint32(len(e.Books)), raw))
	}
	r, err := c.LoadOptionsRun(ctx, e.RunID)
	if err != nil {
		return err
	}
	if err = e.ValidateRun(r); err != nil {
		return err
	}
	if r.Selection == options.CatalogSelection {
		return fmt.Errorf("catalog requires plan commit")
	}
	if err = c.validateDerivativeReferences(ctx, e.Books); err != nil {
		return err
	}
	if err = c.validateLiveMetadata(ctx, r, e); err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error { return c.persistOptionsLive(ctx, tx, e) })
}

func (c *Client) persistOptionsLive(ctx context.Context, tx *sql.Tx, e options.LiveEnvelope) error {
	if err := c.insertDerivativeBooks(ctx, tx, e.ID(), e.Books); err != nil {
		return err
	}
	indexes, indexHashes, err := c.insertCatalogIndexes(ctx, tx, e.ID(), e)
	if err != nil {
		return err
	}
	ids, hashes, a, d := derivativeManifest(options.BookEnvelope{Batches: e.Books})
	return c.putValues(ctx, tx, "options_live_minute_commit", liveCommitColumns, []any{e.RunID, e.MinuteTime, e.ID(), e.RunHash, e.PreparedAt, ids, hashes, a, d, indexes, indexHashes})
}

func (c *Client) LoadOptionsMinute(ctx context.Context, run uuid.UUID, minute time.Time) (options.LiveEnvelope, error) {
	var e options.LiveEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return e, err
	}
	if r.Selection == options.CatalogSelection {
		v, err := c.LoadOptionsCatalogMinute(ctx, run, minute)
		return v.Live, err
	}
	rows, err := c.records(ctx, c.db, "options_live_minute_commit", "WHERE run_id=? AND minute_time=?", run[:], minute.UTC())
	if err != nil {
		return e, err
	}
	if len(rows) == 0 {
		return e, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return e, contracts.ErrConflict
	}
	var id string
	var ids []uint32
	var hashes, indexes, indexHashes []string
	var a, d uint32
	if err = scanRecord(rows[0], liveCommitColumns, &e.RunID, &e.MinuteTime, &id, &e.RunHash, &e.PreparedAt, &ids, &hashes, &a, &d, &indexes, &indexHashes); err != nil {
		return e, err
	}
	books, err := c.loadDerivativeBookRows(ctx, options.BookEnvelope{MinuteTime: e.MinuteTime}, id, ids, hashes, a, d)
	if err != nil {
		return e, err
	}
	e.Books = books.Batches
	e.Indexes, err = c.loadCatalogIndexes(ctx, minute, id, indexes, indexHashes)
	if err != nil {
		return e, err
	}
	if err = e.ValidateRun(r); err != nil {
		return e, err
	}
	if e.ID() != id {
		return e, fmt.Errorf("live envelope digest mismatch")
	}
	return e, c.validateLiveMetadata(ctx, r, e)
}
func (c *Client) LatestOptionsMinute(ctx context.Context, run uuid.UUID) (time.Time, error) {
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return time.Time{}, err
	}
	name := "options_live_minute_commit"
	if r.Selection == options.CatalogSelection {
		name = "options_catalog_live_minute_commit"
	}
	var at time.Time
	err = c.db.QueryRowContext(ctx, "SELECT minute_time FROM "+c.table(name)+" WHERE run_id=? ORDER BY minute_time DESC LIMIT 1", run[:]).Scan(&at)
	if err == sql.ErrNoRows {
		return at, contracts.ErrNotFound
	}
	return at, err
}
func (c *Client) WriteOptionsCatalogMinute(ctx context.Context, e options.CatalogEnvelope) error {
	return c.WriteOptionsCatalogMinuteLazy(ctx, e.Live.RunID, e.Live.MinuteTime, func() (options.CatalogEnvelope, error) { return e, nil })
}
func (c *Client) WriteOptionsCatalogMinuteLazy(ctx context.Context, run uuid.UUID, minute time.Time, load func() (options.CatalogEnvelope, error)) error {
	if c.queue != nil {
		e, err := load()
		if err != nil {
			return err
		}
		if e.Live.RunID != run || !e.Live.MinuteTime.Equal(minute) {
			return fmt.Errorf("lazy option identity mismatch")
		}
		raw, err := codec.EncodeOptionsCatalog(e)
		if err != nil {
			return err
		}
		return c.queue.Accept(ctx, c.Delivery("options_catalog", run.String(), minute, uint32(len(e.Live.Books)), raw))
	}

	// The connection pool bounds admitted SQL work; expansion happens after acquiring
	// the producer lock, so waiting workers keep their existing compressed minute.
	return c.transaction(ctx, func(tx *sql.Tx) error {
		e, err := load()
		if err != nil {
			return err
		}
		return c.persistOptionsCatalog(ctx, tx, run, minute, e)
	})
}
func (c *Client) persistOptionsCatalog(ctx context.Context, tx *sql.Tx, run uuid.UUID, minute time.Time, e options.CatalogEnvelope) error {
	var err error

	if e.Live.RunID != run || !e.Live.MinuteTime.Equal(minute) {
		return fmt.Errorf("lazy minute identity mismatch")
	}
	if err = e.Validate(); err != nil {
		return err
	}
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return err
	}
	if err = e.Live.ValidateRun(r); err != nil {
		return err
	}
	p, err := c.OptionsPlanAt(ctx, minute)
	if err != nil {
		return err
	}
	if p.ID != e.PlanID || p.Hash() != e.PlanHash {
		return fmt.Errorf("minute plan mismatch")
	}
	if err = p.ValidateRun(r, minute); err != nil {
		return err
	}
	if err = c.validateDerivativeReferences(ctx, e.Live.Books); err != nil {
		return err
	}
	if err = c.validateCatalogEvidence(ctx, r, e); err != nil {
		return err
	}
	return c.persistOptionsCatalogRows(ctx, tx, r, p, e)
}
func (c *Client) persistOptionsCatalogRows(ctx context.Context, tx *sql.Tx, r options.LiveRun, p options.CollectionPlan, e options.CatalogEnvelope) error {
	minute := e.Live.MinuteTime
	var err error
	id := e.ID()
	if err = c.insertDerivativeBooks(ctx, tx, id, e.Live.Books); err != nil {
		return err
	}
	rows := append([]options.QualityEvidence(nil), e.Evidence...)
	var evidenceHashes []string
	for n := range rows {
		rows[n].BatchID = id
		rows[n].RowHash = rows[n].Hash()
		evidenceHashes = append(evidenceHashes, rows[n].RowHash)
	}
	if err = c.putQualityEvidence(ctx, tx, rows); err != nil {
		return err
	}
	indexes, indexHashes, err := c.insertCatalogIndexes(ctx, tx, id, e.Live)
	if err != nil {
		return err
	}
	ids, hashes, a, d := derivativeManifest(options.BookEnvelope{Batches: e.Live.Books})
	commit := catalogCommit{r.ID, minute, id, p.ID, p.Hash(), e.Live.RunHash, e.Live.PreparedAt, ids, hashes, evidenceHashes, a, d, indexes, indexHashes}
	return putModels(ctx, c, tx, "options_catalog_live_minute_commit", []catalogCommit{commit})
}

func (c *Client) LoadOptionsCatalogMinute(ctx context.Context, run uuid.UUID, minute time.Time) (options.CatalogEnvelope, error) {
	var e options.CatalogEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	rows, err := readModels[catalogCommit](ctx, c, c.db, "options_catalog_live_minute_commit", "WHERE run_id=? AND minute_time=?", run[:], minute.UTC())
	if err != nil {
		return e, err
	}
	if len(rows) == 0 {
		return e, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return e, contracts.ErrConflict
	}
	v := rows[0]
	e.PlanID, e.PlanHash = v.PlanID, v.PlanHash
	e.Live = options.LiveEnvelope{RunID: v.RunID, RunHash: v.RunHash, MinuteTime: v.MinuteTime, PreparedAt: v.PreparedAt}
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return e, err
	}
	p, err := c.OptionsPlanAt(ctx, minute)
	if err != nil {
		return e, err
	}
	if p.ID != e.PlanID || p.Hash() != e.PlanHash {
		return e, fmt.Errorf("stored plan mismatch")
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
	e.Evidence, err = c.loadQualityEvidence(ctx, run, minute, v.BatchID)
	if err != nil {
		return e, err
	}
	if len(e.Evidence) != len(v.EvidenceHashes) {
		return e, fmt.Errorf("evidence count mismatch")
	}
	for n, q := range e.Evidence {
		hash := q.Hash()
		if q.BatchID != v.BatchID || q.RowHash != hash || hash != v.EvidenceHashes[n] {
			return e, fmt.Errorf("evidence digest mismatch")
		}
	}
	if err = e.Validate(); err != nil {
		return e, err
	}
	if err = e.Live.ValidateRun(r); err != nil {
		return e, err
	}
	if e.ID() != v.BatchID {
		return e, fmt.Errorf("catalog digest mismatch")
	}
	return e, c.validateCatalogEvidence(ctx, r, e)
}
