package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"slices"
	"sort"
)

const canonicalMappingColumns = "instrument_id,mapping_revision,canonical_market_key,canonical_base_asset,canonical_quote_asset,canonical_settle_asset,canonical_base_units_per_venue_base_unit,mapping_kind,recorded_at"
const universeRunColumns = "run_id,selection_revision,mapping_revision,started_at,selection_config_json,enabled_venues,canonical_include,canonical_exclude,canonical_group_count,instrument_count"

func (c *Client) CanonicalMappings(ctx context.Context, revision string) ([]model.CanonicalMapping, error) {
	if e := model.ValidateMappingRevision(revision); e != nil {
		return nil, e
	}
	rows, e := c.records(ctx, c.db, "instrument_canonical_mapping", "WHERE mapping_revision=? ORDER BY instrument_id", []byte(revision))
	if e != nil {
		return nil, e
	}
	out := []model.CanonicalMapping{}
	for _, v := range rows {
		var m model.CanonicalMapping
		if e = scanRecord(v, canonicalMappingColumns, &m.InstrumentID, &m.MappingRevision, &m.CanonicalMarketKey, &m.CanonicalBaseAsset, &m.CanonicalQuoteAsset, &m.CanonicalSettleAsset, &m.CanonicalBaseUnitsPerVenueBaseUnit, &m.MappingKind, &m.RecordedAt); e != nil {
			return nil, e
		}
		if e = m.Validate(); e != nil {
			return nil, e
		}
		out = append(out, m)
	}
	return out, nil
}
func (c *Client) WriteCanonicalMappings(ctx context.Context, input []model.CanonicalMapping) error {
	seen := map[string]bool{}
	items, e := c.Instruments(ctx)
	if e != nil {
		return e
	}
	byID := map[uint32]model.Instrument{}
	for _, i := range items {
		byID[i.ID] = i
	}
	for _, m := range input {
		if e = m.Validate(); e != nil {
			return e
		}
		key := fmt.Sprintf("%d/%s", m.InstrumentID, m.MappingRevision)
		if seen[key] {
			return fmt.Errorf("duplicate mapping identity")
		}
		seen[key] = true
		i, ok := byID[m.InstrumentID]
		if !ok || i.MarketType != model.MarketPerpetual || i.QuoteAsset != "USDT" || i.SettleAsset == nil || *i.SettleAsset != "USDT" || (m.MappingKind == "identity" && i.BaseAsset != m.CanonicalBaseAsset) {
			return fmt.Errorf("mapping instrument identity mismatch")
		}
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, m := range input {
			rows, e := c.records(ctx, tx, "instrument_canonical_mapping", "WHERE instrument_id=? AND mapping_revision=? FOR UPDATE", m.InstrumentID, []byte(m.MappingRevision))
			if e != nil {
				return e
			}
			if len(rows) > 1 {
				return contracts.ErrConflict
			}
			if len(rows) == 1 {
				var old model.CanonicalMapping
				if e = scanRecord(rows[0], canonicalMappingColumns, &old.InstrumentID, &old.MappingRevision, &old.CanonicalMarketKey, &old.CanonicalBaseAsset, &old.CanonicalQuoteAsset, &old.CanonicalSettleAsset, &old.CanonicalBaseUnitsPerVenueBaseUnit, &old.MappingKind, &old.RecordedAt); e != nil {
					return e
				}
				if !old.SameMapping(m) {
					return contracts.ErrConflict
				}
				continue
			}
			if e = c.putValues(ctx, tx, "instrument_canonical_mapping", canonicalMappingColumns, []any{m.InstrumentID, m.MappingRevision, m.CanonicalMarketKey, m.CanonicalBaseAsset, m.CanonicalQuoteAsset, m.CanonicalSettleAsset, m.CanonicalBaseUnitsPerVenueBaseUnit, m.MappingKind, m.RecordedAt}); e != nil {
				return e
			}
		}
		return nil
	})
}
func (c *Client) PerpetualUniverse(ctx context.Context, id uuid.UUID) (model.PerpetualUniverseRun, []model.PerpetualUniverseMember, error) {
	var run model.PerpetualUniverseRun
	rows, e := c.records(ctx, c.db, "perpetual_universe_run", "WHERE run_id=?", id[:])
	if e != nil {
		return run, nil, e
	}
	if len(rows) == 0 {
		return run, nil, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return run, nil, contracts.ErrConflict
	}
	if e = scanRecord(rows[0], universeRunColumns, &run.RunID, &run.SelectionRevision, &run.MappingRevision, &run.StartedAt, &run.SelectionConfigJSON, &run.EnabledVenues, &run.CanonicalInclude, &run.CanonicalExclude, &run.CanonicalGroupCount, &run.InstrumentCount); e != nil {
		return run, nil, e
	}
	if e = run.Validate(); e != nil {
		return run, nil, e
	}
	rows, e = c.records(ctx, c.db, "perpetual_universe_member", "WHERE run_id=? ORDER BY instrument_id", id[:])
	if e != nil {
		return run, nil, e
	}
	members := []model.PerpetualUniverseMember{}
	for _, v := range rows {
		var m model.PerpetualUniverseMember
		if e = scanRecord(v, "run_id,instrument_id,canonical_market_key", &m.RunID, &m.InstrumentID, &m.CanonicalMarketKey); e != nil {
			return run, nil, e
		}
		members = append(members, m)
	}
	return run, members, validateUniverseMembers(run, members)
}
func (c *Client) WritePerpetualUniverseRun(ctx context.Context, r model.PerpetualUniverseRun, members []model.PerpetualUniverseMember) error {
	if e := r.Validate(); e != nil {
		return e
	}
	if e := validateUniverseMembers(r, members); e != nil {
		return e
	}
	members = append([]model.PerpetualUniverseMember(nil), members...)
	sort.Slice(members, func(i, j int) bool { return members[i].InstrumentID < members[j].InstrumentID })
	mappings, e := c.CanonicalMappings(ctx, r.MappingRevision)
	if e != nil {
		return e
	}
	byID := map[uint32]string{}
	for _, m := range mappings {
		byID[m.InstrumentID] = m.CanonicalMarketKey
	}
	items, e := c.Instruments(ctx)
	if e != nil {
		return e
	}
	venues := map[uint32]string{}
	for _, i := range items {
		venues[i.ID] = i.Exchange
	}
	groups := map[string]map[string]bool{}
	for _, m := range members {
		venue := venues[m.InstrumentID]
		if byID[m.InstrumentID] != m.CanonicalMarketKey || !slices.Contains(r.EnabledVenues, venue) {
			return fmt.Errorf("universe mapping/venue mismatch")
		}
		if groups[m.CanonicalMarketKey] == nil {
			groups[m.CanonicalMarketKey] = map[string]bool{}
		}
		if groups[m.CanonicalMarketKey][venue] {
			return fmt.Errorf("universe repeats venue")
		}
		groups[m.CanonicalMarketKey][venue] = true
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, m := range members {
			if e := c.putValues(ctx, tx, "perpetual_universe_member", "run_id,instrument_id,canonical_market_key", []any{m.RunID, m.InstrumentID, m.CanonicalMarketKey}); e != nil {
				return e
			}
		}
		var count int
		if e := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("perpetual_universe_member")+" WHERE run_id=?", r.RunID[:]).Scan(&count); e != nil {
			return e
		}
		if count != len(members) {
			return contracts.ErrConflict
		}
		return c.putValues(ctx, tx, "perpetual_universe_run", universeRunColumns, []any{r.RunID, r.SelectionRevision, r.MappingRevision, r.StartedAt, r.SelectionConfigJSON, r.EnabledVenues, r.CanonicalInclude, r.CanonicalExclude, r.CanonicalGroupCount, r.InstrumentCount})
	})
}
func (c *Client) CanonicalMappingsForRun(ctx context.Context, id uuid.UUID) ([]model.CanonicalMapping, error) {
	r, _, e := c.PerpetualUniverse(ctx, id)
	if e != nil {
		return nil, e
	}
	return c.CanonicalMappings(ctx, r.MappingRevision)
}

func validateUniverseMembers(run model.PerpetualUniverseRun, members []model.PerpetualUniverseMember) error {
	if uint64(len(members)) != uint64(run.InstrumentCount) {
		return fmt.Errorf("universe member count does not match run")
	}
	seen := make(map[uint32]struct{}, len(members))
	groups := make(map[string]int)
	for _, member := range members {
		if member.RunID != run.RunID || member.InstrumentID == 0 {
			return fmt.Errorf("invalid universe member identity")
		}
		if _, exists := seen[member.InstrumentID]; exists {
			return fmt.Errorf("duplicate universe member instrument %d", member.InstrumentID)
		}
		if err := model.ValidateCanonicalMarketKey(member.CanonicalMarketKey); err != nil {
			return err
		}
		if slices.Contains(run.CanonicalExclude, member.CanonicalMarketKey) || (len(run.CanonicalInclude) > 0 && !slices.Contains(run.CanonicalInclude, member.CanonicalMarketKey)) {
			return fmt.Errorf("universe member %s contradicts canonical include/exclude", member.CanonicalMarketKey)
		}
		seen[member.InstrumentID] = struct{}{}
		groups[member.CanonicalMarketKey]++
	}
	if uint64(len(groups)) != uint64(run.CanonicalGroupCount) {
		return fmt.Errorf("universe canonical group count does not match run")
	}
	for key, count := range groups {
		if count < 2 {
			return fmt.Errorf("universe group %s has fewer than two instruments", key)
		}
	}
	for _, included := range run.CanonicalInclude {
		if groups[included] == 0 {
			return fmt.Errorf("universe lacks explicitly included group %s", included)
		}
	}
	return nil
}
