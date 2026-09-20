package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

const canonicalMappingColumns = `instrument_id,mapping_revision,canonical_market_key,canonical_base_asset,canonical_quote_asset,canonical_settle_asset,canonical_base_units_per_venue_base_unit,mapping_kind,recorded_at`

// CanonicalMappings requires an explicit revision. recorded_at is audit data,
// never an authority selector; an A -> B -> A rollback must select A again.
func (c *Client) CanonicalMappings(ctx context.Context, revision string) ([]model.CanonicalMapping, error) {
	if err := model.ValidateMappingRevision(revision); err != nil {
		return nil, err
	}
	rows, err := c.conn.Query(ctx, `SELECT `+canonicalMappingColumns+` FROM `+c.table("instrument_canonical_mapping")+` FINAL WHERE mapping_revision=? ORDER BY instrument_id`, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var mappings []model.CanonicalMapping
	for rows.Next() {
		var m model.CanonicalMapping
		if err = rows.Scan(&m.InstrumentID, &m.MappingRevision, &m.CanonicalMarketKey, &m.CanonicalBaseAsset, &m.CanonicalQuoteAsset, &m.CanonicalSettleAsset, &m.CanonicalBaseUnitsPerVenueBaseUnit, &m.MappingKind, &m.RecordedAt); err != nil {
			return nil, err
		}
		if err = m.Validate(); err != nil {
			return nil, fmt.Errorf("invalid stored canonical mapping: %w", err)
		}
		mappings = append(mappings, m)
	}
	return mappings, rows.Err()
}

func (c *Client) WriteCanonicalMappings(ctx context.Context, mappings []model.CanonicalMapping) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	type identity struct {
		instrument uint32
		revision   string
	}
	seen := make(map[identity]model.CanonicalMapping, len(mappings))
	revisions := make(map[string]struct{})
	for _, m := range mappings {
		if err := m.Validate(); err != nil {
			return err
		}
		key := identity{m.InstrumentID, m.MappingRevision}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate canonical mapping instrument/revision")
		}
		seen[key] = m
		revisions[m.MappingRevision] = struct{}{}
	}
	if len(mappings) == 0 {
		return nil
	}
	instruments, err := c.Instruments(ctx)
	if err != nil {
		return err
	}
	byID := make(map[uint32]model.Instrument, len(instruments))
	for _, item := range instruments {
		byID[item.ID] = item
	}
	for _, m := range mappings {
		item, exists := byID[m.InstrumentID]
		if !exists || item.MarketType != model.MarketPerpetual || item.QuoteAsset != "USDT" || item.SettleAsset == nil || *item.SettleAsset != "USDT" {
			return fmt.Errorf("canonical mapping instrument %d is not a registered USDT perpetual", m.InstrumentID)
		}
		if m.MappingKind == "identity" && item.BaseAsset != m.CanonicalBaseAsset {
			return fmt.Errorf("identity mapping changes instrument %d base asset", m.InstrumentID)
		}
	}
	for revision := range revisions {
		stored, readErr := c.CanonicalMappings(ctx, revision)
		if readErr != nil {
			return readErr
		}
		for _, previous := range stored {
			key := identity{previous.InstrumentID, revision}
			if proposed, exists := seen[key]; exists {
				if !previous.SameMapping(proposed) {
					return fmt.Errorf("canonical mapping conflicts with existing instrument %d revision %s", proposed.InstrumentID, revision)
				}
				delete(seen, key) // Keep the original recorded_at, including after rollback.
			}
		}
	}
	additions := make([]model.CanonicalMapping, 0, len(seen))
	for _, m := range seen {
		additions = append(additions, m)
	}
	sort.Slice(additions, func(i, j int) bool {
		if additions[i].InstrumentID == additions[j].InstrumentID {
			return additions[i].MappingRevision < additions[j].MappingRevision
		}
		return additions[i].InstrumentID < additions[j].InstrumentID
	})
	if len(additions) == 0 {
		return nil
	}
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		batch, prepareErr := c.conn.PrepareBatch(writeCtx, `INSERT INTO `+c.table("instrument_canonical_mapping")+` (`+canonicalMappingColumns+`)`)
		if prepareErr != nil {
			return prepareErr
		}
		defer batch.Abort()
		for _, m := range additions {
			if appendErr := batch.Append(m.InstrumentID, m.MappingRevision, m.CanonicalMarketKey, m.CanonicalBaseAsset, m.CanonicalQuoteAsset, m.CanonicalSettleAsset, m.CanonicalBaseUnitsPerVenueBaseUnit, m.MappingKind, m.RecordedAt.UTC()); appendErr != nil {
				return appendErr
			}
		}
		return batch.Send()
	})
}

const universeRunColumns = `run_id,selection_revision,mapping_revision,started_at,selection_config_json,enabled_venues,canonical_include,canonical_exclude,canonical_group_count,instrument_count`

// PerpetualUniverse does not expose orphan members from an uncommitted run.
func (c *Client) PerpetualUniverse(ctx context.Context, runID uuid.UUID) (model.PerpetualUniverseRun, []model.PerpetualUniverseMember, error) {
	var run model.PerpetualUniverseRun
	if runID == uuid.Nil {
		return run, nil, fmt.Errorf("universe query requires run_id")
	}
	err := c.conn.QueryRow(ctx, `SELECT `+universeRunColumns+` FROM `+c.table("perpetual_universe_run")+` FINAL WHERE run_id=?`, runID).Scan(
		&run.RunID, &run.SelectionRevision, &run.MappingRevision, &run.StartedAt, &run.SelectionConfigJSON,
		&run.EnabledVenues, &run.CanonicalInclude, &run.CanonicalExclude, &run.CanonicalGroupCount, &run.InstrumentCount)
	if err != nil {
		return run, nil, err
	}
	if err = run.Validate(); err != nil {
		return run, nil, fmt.Errorf("invalid stored universe run: %w", err)
	}
	members, err := c.universeMembers(ctx, runID)
	if err == nil {
		err = validateUniverseMembers(run, members)
	}
	return run, members, err
}

func (c *Client) universeMembers(ctx context.Context, runID uuid.UUID) ([]model.PerpetualUniverseMember, error) {
	rows, err := c.conn.Query(ctx, `SELECT run_id,instrument_id,canonical_market_key FROM `+c.table("perpetual_universe_member")+` FINAL WHERE run_id=? ORDER BY instrument_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []model.PerpetualUniverseMember
	for rows.Next() {
		var m model.PerpetualUniverseMember
		if err = rows.Scan(&m.RunID, &m.InstrumentID, &m.CanonicalMarketKey); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
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

// WritePerpetualUniverseRun must be called after all local runtime constructors
// have succeeded. The run row is the final visibility marker, never written
// before every member and its matching revision have been validated/persisted.
func (c *Client) WritePerpetualUniverseRun(ctx context.Context, run model.PerpetualUniverseRun, members []model.PerpetualUniverseMember) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	if err := run.Validate(); err != nil {
		return err
	}
	if err := validateUniverseMembers(run, members); err != nil {
		return err
	}
	members = append([]model.PerpetualUniverseMember(nil), members...)
	sort.Slice(members, func(i, j int) bool { return members[i].InstrumentID < members[j].InstrumentID })
	mappings, err := c.CanonicalMappings(ctx, run.MappingRevision)
	if err != nil {
		return err
	}
	mappingByID := make(map[uint32]string, len(mappings))
	for _, m := range mappings {
		mappingByID[m.InstrumentID] = m.CanonicalMarketKey
	}
	instruments, err := c.Instruments(ctx)
	if err != nil {
		return err
	}
	venueByID := make(map[uint32]string, len(instruments))
	for _, item := range instruments {
		venueByID[item.ID] = item.Exchange
	}
	groupVenues := make(map[string]map[string]bool)
	for _, member := range members {
		if mappingByID[member.InstrumentID] != member.CanonicalMarketKey {
			return fmt.Errorf("universe member %d lacks matching persisted canonical mapping", member.InstrumentID)
		}
		venue := venueByID[member.InstrumentID]
		if !slices.Contains(run.EnabledVenues, venue) {
			return fmt.Errorf("universe member %d belongs to disabled venue %q", member.InstrumentID, venue)
		}
		if groupVenues[member.CanonicalMarketKey] == nil {
			groupVenues[member.CanonicalMarketKey] = make(map[string]bool)
		}
		if groupVenues[member.CanonicalMarketKey][venue] {
			return fmt.Errorf("universe group %s repeats venue %s", member.CanonicalMarketKey, venue)
		}
		groupVenues[member.CanonicalMarketKey][venue] = true
	}
	previous, previousMembers, err := c.PerpetualUniverse(ctx, run.RunID)
	if err == nil {
		if !sameUniverseRun(previous, run) || !slices.Equal(previousMembers, members) {
			return fmt.Errorf("universe run conflicts with existing run_id %s", run.RunID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// A failed attempt can leave members; reuse only exact subsets of this run.
	orphans, err := c.universeMembers(ctx, run.RunID)
	if err != nil {
		return err
	}
	byID := make(map[uint32]model.PerpetualUniverseMember, len(members))
	for _, member := range members {
		byID[member.InstrumentID] = member
	}
	for _, orphan := range orphans {
		if byID[orphan.InstrumentID] != orphan {
			return fmt.Errorf("orphan member conflicts with run %s", run.RunID)
		}
	}
	if len(members) > 0 {
		if err = c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertUniverseMembers(writeCtx, members) }); err != nil {
			return fmt.Errorf("write universe members: %w", err)
		}
	}
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		batch, err := c.conn.PrepareBatch(writeCtx, `INSERT INTO `+c.table("perpetual_universe_run")+` (`+universeRunColumns+`)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		if err = batch.Append(
			run.RunID, run.SelectionRevision, run.MappingRevision, run.StartedAt.UTC(), run.SelectionConfigJSON,
			run.EnabledVenues, run.CanonicalInclude, run.CanonicalExclude, run.CanonicalGroupCount, run.InstrumentCount); err != nil {
			return err
		}
		return batch.Send()
	})
}

func (c *Client) insertUniverseMembers(ctx context.Context, members []model.PerpetualUniverseMember) error {
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("perpetual_universe_member")+` (run_id,instrument_id,canonical_market_key)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, member := range members {
		if err = batch.Append(member.RunID, member.InstrumentID, member.CanonicalMarketKey); err != nil {
			return err
		}
	}
	return batch.Send()
}

func sameUniverseRun(a, b model.PerpetualUniverseRun) bool {
	return a.RunID == b.RunID && a.SelectionRevision == b.SelectionRevision && a.MappingRevision == b.MappingRevision &&
		a.StartedAt.Equal(b.StartedAt) && a.SelectionConfigJSON == b.SelectionConfigJSON && slices.Equal(a.EnabledVenues, b.EnabledVenues) &&
		slices.Equal(a.CanonicalInclude, b.CanonicalInclude) && slices.Equal(a.CanonicalExclude, b.CanonicalExclude) &&
		a.CanonicalGroupCount == b.CanonicalGroupCount && a.InstrumentCount == b.InstrumentCount
}

func (c *Client) CanonicalMappingsForRun(ctx context.Context, runID uuid.UUID) ([]model.CanonicalMapping, error) {
	run, _, err := c.PerpetualUniverse(ctx, runID)
	if err != nil {
		return nil, err
	}
	return c.CanonicalMappings(ctx, run.MappingRevision)
}
