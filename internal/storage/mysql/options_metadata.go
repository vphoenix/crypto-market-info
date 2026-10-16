package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

const liveRunColumns = "run_id,run_hash,started_at,rest_url,ws_url,selection,instrument_ids,symbols,definition_hashes,member_index_ids,index_ids,ref_symbols,ref_bids,ref_asks,ref_source_times,ref_received_times,ref_hashes"
const metadataColumns = "run_id,attempt_id,instrument_id,symbol,scope,source_url,requested_at,observed_at,payload_hash,status,definition_hash,trading_rule_id,state,active,scope_complete,scope_raw_count,scope_accepted_count,scope_excluded_count,row_hash"

func scanRecord(r record, cols string, dest ...any) error {
	names := strings.Split(cols, ",")
	if len(names) != len(dest) {
		return fmt.Errorf("typed scan width mismatch")
	}
	for i, n := range names {
		v, ok := r[strings.TrimSpace(n)]
		if !ok {
			return fmt.Errorf("missing typed field %s", n)
		}
		p := reflect.ValueOf(dest[i])
		if p.Kind() != reflect.Pointer || p.IsNil() {
			return fmt.Errorf("scan requires pointer")
		}
		if e := codec.Assign(p.Elem(), v); e != nil {
			return fmt.Errorf("%s: %w", n, e)
		}
	}
	return nil
}
func (c *Client) records(ctx context.Context, q executor, name, where string, args ...any) ([]record, error) {
	t, e := c.mapping(name)
	if e != nil {
		return nil, e
	}
	// Preserve physical occurrences: duplicate publications are ambiguous even
	// when their bytes match. Domain readers decide completeness and uniqueness.
	return readRecords(ctx, q, c.table(name), t, where, args...)
}
func (c *Client) validateOptionsRunSpecs(ctx context.Context, r options.LiveRun) error {
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, e := c.LoadDerivativeSpecs(ctx, ids)
	if e != nil {
		return e
	}
	return validateOptionsRunSpecList(r, specs)
}
func validateOptionsRunSpecList(r options.LiveRun, specs []options.ContractSpec) error {
	if len(specs) != len(r.Members) {
		return fmt.Errorf("incomplete run specs")
	}
	if e := options.ValidateLiveSpecs(r.Selection, specs); e != nil {
		return e
	}
	for n, s := range specs {
		m := r.Members[n]
		if s.Instrument.ID != m.InstrumentID || s.Instrument.ExchangeSymbol != m.Symbol || s.DefinitionHash() != m.DefinitionHash || s.IndexID != m.IndexID {
			return fmt.Errorf("run spec identity mismatch")
		}
	}
	return nil
}
func (c *Client) WriteOptionsRun(ctx context.Context, r options.LiveRun) error {
	return c.WriteOptionsRuns(ctx, []options.LiveRun{r})
}
func optionsRunRecord(r options.LiveRun) (record, error) {
	ids := []uint32{}
	symbols, hashes, indexes := []string{}, []string{}, []string{}
	for _, m := range r.Members {
		ids = append(ids, m.InstrumentID)
		symbols = append(symbols, m.Symbol)
		hashes = append(hashes, m.DefinitionHash)
		indexes = append(indexes, m.IndexID)
	}
	names, refHashes := []string{}, []string{}
	bids, asks := []int64{}, []int64{}
	sources, received := []time.Time{}, []time.Time{}
	for _, v := range r.References {
		names = append(names, v.Symbol)
		bids = append(bids, v.Bid)
		asks = append(asks, v.Ask)
		sources = append(sources, v.SourceTime)
		received = append(received, v.ReceivedAt)
		refHashes = append(refHashes, v.PayloadHash)
	}
	return namedRecord(liveRunColumns, []any{r.ID, r.Hash(), r.StartedAt, r.RESTURL, r.WSURL, r.Selection, ids, symbols, hashes, indexes, r.Indexes, names, bids, asks, sources, received, refHashes})
}
func (c *Client) LoadOptionsRun(ctx context.Context, id uuid.UUID) (options.LiveRun, error) {
	return cachedMetadata(ctx, c, "run/"+id.String(), func(ctx context.Context) (options.LiveRun, error) { return c.loadOptionsRun(ctx, id) })
}
func (c *Client) loadOptionsRun(ctx context.Context, id uuid.UUID) (options.LiveRun, error) {
	var r options.LiveRun
	rows, e := c.records(ctx, c.db, "options_live_run", "WHERE run_id=?", id[:])
	if e != nil {
		return r, e
	}
	if len(rows) == 0 {
		return r, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return r, contracts.ErrConflict
	}
	r, e = optionsRunFromRecord(rows[0])
	if e != nil {
		return r, e
	}
	return r, c.validateOptionsRunSpecs(ctx, r)
}
func optionsRunFromRecord(row record) (options.LiveRun, error) {
	var r options.LiveRun
	var hash string
	var ids []uint32
	var symbols, hashes, indexes, names, refHashes []string
	var bids, asks []int64
	var sources, received []time.Time
	if e := scanRecord(row, liveRunColumns, &r.ID, &hash, &r.StartedAt, &r.RESTURL, &r.WSURL, &r.Selection, &ids, &symbols, &hashes, &indexes, &r.Indexes, &names, &bids, &asks, &sources, &received, &refHashes); e != nil {
		return r, e
	}
	if len(ids) != len(symbols) || len(ids) != len(hashes) || len(ids) != len(indexes) || len(names) != len(bids) || len(names) != len(asks) || len(names) != len(sources) || len(names) != len(received) || len(names) != len(refHashes) {
		return r, fmt.Errorf("incomplete run arrays")
	}
	for n, id := range ids {
		r.Members = append(r.Members, options.LiveMember{InstrumentID: id, Symbol: symbols[n], DefinitionHash: hashes[n], IndexID: indexes[n]})
	}
	for n, name := range names {
		r.References = append(r.References, options.SelectionReference{Symbol: name, Bid: bids[n], Ask: asks[n], SourceTime: sources[n], ReceivedAt: received[n], PayloadHash: refHashes[n]})
	}
	if e := r.Validate(); e != nil {
		return r, e
	}
	if r.Hash() != hash {
		return r, fmt.Errorf("run digest mismatch")
	}
	return r, nil
}
func (c *Client) LatestOptionsRun(ctx context.Context) (options.LiveRun, error) {
	rows, e := c.records(ctx, c.db, "options_live_run", "ORDER BY started_at DESC,run_id DESC LIMIT 1")
	if e != nil {
		return options.LiveRun{}, e
	}
	if len(rows) == 0 {
		return options.LiveRun{}, contracts.ErrNotFound
	}
	return c.LoadOptionsRun(ctx, rows[0]["run_id"].(uuid.UUID))
}
func (c *Client) WriteOptionsMetadata(ctx context.Context, o options.MetadataObservation) error {
	if e := o.Validate(); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		return c.putValues(ctx, tx, "options_metadata_observation", metadataColumns, []any{o.RunID, o.AttemptID, o.InstrumentID, o.Symbol, o.Scope, o.SourceURL, o.RequestedAt, o.ObservedAt, o.PayloadHash, o.Status, o.DefinitionHash, o.TradingRuleID, o.State, o.Active, o.ScopeComplete, o.ScopeRawCount, o.ScopeAcceptedCount, o.ScopeExcludedCount, o.Hash()})
	})
}
func (c *Client) validateLiveMetadata(ctx context.Context, r options.LiveRun, e options.LiveEnvelope) error {
	rules := map[string]bool{}
	for _, b := range e.Books {
		for _, q := range b.Quality {
			if q.MarketKnown {
				if q.TradingRuleID == "" || q.MarketStateBasis != 2 || !q.MarketStateAt.Equal(q.RulePublishedAt) {
					return fmt.Errorf("live state lacks metadata reference")
				}
				rules[q.TradingRuleID] = true
			}
		}
	}
	if len(rules) == 0 {
		return nil
	}
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		return err
	}
	rows, err := c.records(ctx, c.db, "options_metadata_observation", "WHERE run_id=? AND status='complete'", r.ID[:])
	if err != nil {
		return err
	}
	byRule := map[string]options.MetadataObservation{}
	for _, v := range rows {
		var o options.MetadataObservation
		var hash string
		if err = scanRecord(v, metadataColumns, &o.RunID, &o.AttemptID, &o.InstrumentID, &o.Symbol, &o.Scope, &o.SourceURL, &o.RequestedAt, &o.ObservedAt, &o.PayloadHash, &o.Status, &o.DefinitionHash, &o.TradingRuleID, &o.State, &o.Active, &o.ScopeComplete, &o.ScopeRawCount, &o.ScopeAcceptedCount, &o.ScopeExcludedCount, &hash); err != nil {
			return err
		}
		if !rules[o.TradingRuleID] {
			continue
		}
		if err = o.Validate(); err != nil {
			return err
		}
		if hash != o.Hash() {
			return fmt.Errorf("metadata digest mismatch")
		}
		if _, ok := byRule[o.TradingRuleID]; ok {
			return fmt.Errorf("ambiguous metadata evidence")
		}
		byRule[o.TradingRuleID] = o
	}
	for n, b := range e.Books {
		for sec, q := range b.Quality {
			if !q.MarketKnown {
				continue
			}
			o, ok := byRule[q.TradingRuleID]
			if !ok || o.InstrumentID != b.InstrumentID || o.Symbol != r.Members[n].Symbol || o.DefinitionHash != r.Members[n].DefinitionHash || q.RulePublishedAt.Before(o.ObservedAt) {
				return fmt.Errorf("missing/future/wrong metadata")
			}
			open := o.Active && o.State == "open" && e.MinuteTime.Add(time.Duration(sec)*time.Second).Before(*specs[n].Instrument.ExpiryTime)
			if q.MarketOpen != open {
				return fmt.Errorf("metadata market state mismatch")
			}
		}
	}
	return nil
}
