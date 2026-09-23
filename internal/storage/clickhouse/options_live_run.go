package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

const liveRunColumns = `run_id,run_hash,started_at,rest_url,ws_url,selection,instrument_ids,symbols,definition_hashes,member_index_ids,index_ids,ref_symbols,ref_bids,ref_asks,ref_source_times,ref_received_times,ref_hashes`

func (c *Client) WriteOptionsRun(ctx context.Context, r options.LiveRun) error {
	if err := r.Validate(); err != nil {
		return err
	}
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	old, err := c.LoadOptionsRun(ctx, r.ID)
	if err == nil {
		if old.Hash() != r.Hash() {
			return fmt.Errorf("immutable live run conflict")
		}
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	var ids []uint32
	var symbols, hashes, indexes []string
	if err = c.validateOptionsRunSpecs(ctx, r); err != nil {
		return err
	}
	for _, m := range r.Members {
		ids = append(ids, m.InstrumentID)
		symbols = append(symbols, m.Symbol)
		hashes = append(hashes, m.DefinitionHash)
		indexes = append(indexes, m.IndexID)
	}
	var names, refHashes []string
	var bids, asks []int64
	var sources, received []time.Time
	for _, ref := range r.References {
		names = append(names, ref.Symbol)
		bids = append(bids, ref.Bid)
		asks = append(asks, ref.Ask)
		sources = append(sources, ref.SourceTime)
		received = append(received, ref.ReceivedAt)
		refHashes = append(refHashes, ref.PayloadHash)
	}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.insertDerivativeRow(ctx, "options_live_run", liveRunColumns, r.ID, r.Hash(), r.StartedAt, r.RESTURL, r.WSURL, r.Selection, ids, symbols, hashes, indexes, r.Indexes, names, bids, asks, sources, received, refHashes)
	})
}
func (c *Client) LoadOptionsRun(ctx context.Context, id uuid.UUID) (options.LiveRun, error) {
	var r options.LiveRun
	var hash string
	var ids []uint32
	var symbols, hashes, indexes, names, refHashes []string
	var bids, asks []int64
	var sources, received []time.Time
	err := c.conn.QueryRow(ctx, `SELECT `+liveRunColumns+` FROM `+c.table("options_live_run")+` FINAL WHERE run_id=?`, id).Scan(&r.ID, &hash, &r.StartedAt, &r.RESTURL, &r.WSURL, &r.Selection, &ids, &symbols, &hashes, &indexes, &r.Indexes, &names, &bids, &asks, &sources, &received, &refHashes)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if len(ids) != len(symbols) || len(ids) != len(hashes) || len(ids) != len(indexes) || len(names) != len(bids) || len(names) != len(asks) || len(names) != len(sources) || len(names) != len(received) || len(names) != len(refHashes) {
		return r, fmt.Errorf("incomplete live run arrays")
	}
	r.StartedAt = r.StartedAt.UTC()
	for n, id := range ids {
		r.Members = append(r.Members, options.LiveMember{InstrumentID: id, Symbol: symbols[n], DefinitionHash: hashes[n], IndexID: indexes[n]})
	}
	for n, name := range names {
		r.References = append(r.References, options.SelectionReference{Symbol: name, Bid: bids[n], Ask: asks[n], SourceTime: sources[n].UTC(), ReceivedAt: received[n].UTC(), PayloadHash: refHashes[n]})
	}
	if err := r.Validate(); err != nil {
		return r, err
	}
	if hash != r.Hash() {
		return r, fmt.Errorf("live run digest mismatch")
	}
	if err = c.validateOptionsRunSpecs(ctx, r); err != nil {
		return r, err
	}
	return r, nil
}
func (c *Client) LatestOptionsRun(ctx context.Context) (options.LiveRun, error) {
	var id uuid.UUID
	err := c.conn.QueryRow(ctx, `SELECT run_id FROM `+c.table("options_live_run")+` FINAL ORDER BY started_at DESC,run_id DESC LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return options.LiveRun{}, ErrNotFound
	}
	if err != nil {
		return options.LiveRun{}, err
	}
	return c.LoadOptionsRun(ctx, id)
}

const metadataColumns = `run_id,attempt_id,instrument_id,symbol,scope,source_url,requested_at,observed_at,payload_hash,status,definition_hash,trading_rule_id,state,active,scope_complete,scope_raw_count,scope_accepted_count,scope_excluded_count,row_hash`

func (c *Client) WriteOptionsMetadata(ctx context.Context, o options.MetadataObservation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	var old string
	err := c.conn.QueryRow(ctx, `SELECT row_hash FROM `+c.table("options_metadata_observation")+` FINAL WHERE run_id=? AND attempt_id=? AND instrument_id=?`, o.RunID, o.AttemptID, o.InstrumentID).Scan(&old)
	if err == nil {
		if old != o.Hash() {
			return fmt.Errorf("metadata retry conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.insertDerivativeRow(ctx, "options_metadata_observation", metadataColumns, o.RunID, o.AttemptID, o.InstrumentID, o.Symbol, o.Scope, o.SourceURL, o.RequestedAt, o.ObservedAt, o.PayloadHash, o.Status, o.DefinitionHash, o.TradingRuleID, o.State, o.Active, o.ScopeComplete, o.ScopeRawCount, o.ScopeAcceptedCount, o.ScopeExcludedCount, o.Hash())
	})
}
