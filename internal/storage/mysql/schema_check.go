package mysql

import (
	"context"
	"crypto/sha256"
	"fmt"
)

// Collectors only check an administratively installed schema. These methods
// retain the domain interfaces without doing CREATE or ALTER during startup.
func (c *Client) checkSchema(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	if len(names) > 128 {
		return fmt.Errorf("schema check exceeds table budget")
	}
	want := make(map[string][32]byte, len(names))
	args := []any{c.database}
	for _, name := range names {
		t, e := c.mapping(name)
		if e != nil {
			return e
		}
		if _, exists := want[name]; !exists {
			args = append(args, name)
		}
		want[name] = sha256.Sum256([]byte(t.DDL))
	}
	rows, err := c.db.QueryContext(ctx, "SELECT table_name,version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name IN ("+placeholders(len(want))+")", args...)
	if err != nil {
		return fmt.Errorf("schema metadata read: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]bool, len(want))
	for rows.Next() {
		var name string
		var version uint16
		var digest []byte
		if err = rows.Scan(&name, &version, &digest); err != nil {
			return fmt.Errorf("decode schema metadata: %w", err)
		}
		expected, exists := want[name]
		if !exists || seen[name] || version != 1 || string(digest) != string(expected[:]) {
			return fmt.Errorf("schema %s requires administrative migration", name)
		}
		seen[name] = true
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("schema metadata read: %w", err)
	}
	if len(seen) != len(want) {
		return fmt.Errorf("schema metadata is incomplete: checked %d of %d tables", len(seen), len(want))
	}
	return nil
}

// InitMinuteSchema validates the entire writer table set in one bounded read.
// Periodic checks must not repeatedly queue behind the preparation workers.
func (c *Client) InitMinuteSchema(ctx context.Context) error {
	names := MinuteSchemaTables()
	if c.qualityStorageAvailable {
		names = append(names, QualityStorageTableNames()...)
	}
	if c.sourceStorageAvailable {
		names = append(names, SourceStorageTableNames()...)
	}
	if err := c.checkSchema(ctx, names); err != nil {
		return err
	}
	c.discoveryEnabled.Store(true)
	return nil
}
func (c *Client) InitSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"instrument", "order_book_minute", "order_book_second_delta", "funding_rate_hourly", "yield_route", "yield_observation", "instrument_canonical_mapping", "perpetual_universe_run", "perpetual_universe_member"})
}
func (c *Client) InitAcrossSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"across_capture", "across_deposit", "across_deposit_update", "across_fill", "across_refund", "across_tx_receipt", "across_order_probe", "across_receipt_transfers"})
}
func (c *Client) InitReserveSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"reserve_capture", "reserve_folio_state", "reserve_route_quote", "reserve_receipt_data", "reserve_route_simulation", "dex_log", "dex_tx_receipt"})
}
func (c *Client) InitLSTSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"instrument", "lst_capture", "lst_protocol_state", "lst_quote_observation", "lst_withdrawal_request", "lst_withdrawal_finalization", "lst_withdrawal_claim", "lst_funding_settlement"})
}
func (c *Client) InitKeeperSchema(ctx context.Context) error {
	if e := c.checkSchema(ctx, []string{"jl_keeper_capture", "jl_keeper_rental_event", "jl_keeper_tx_receipt", "jl_keeper_probe", "jl_keeper_cost_observation", "jl_keeper_indexed_event", "jl_keeper_index_page"}); e != nil {
		return e
	}
	installed, e := checkKeeperCaptureTimeIndex(ctx, c.db, c.database)
	if e == nil && !installed {
		return fmt.Errorf("Keeper capture time index requires administrative installation")
	}
	return e
}
func (c *Client) InitDEXSchema(ctx context.Context) error {
	if err := c.checkSchema(ctx, []string{"dex_block", "dex_route_quote", "dex_sky_state", "dex_log", "dex_tx_receipt"}); err != nil {
		return err
	}
	for _, index := range dexBlockIndexes {
		installed, err := checkDEXBlockIndex(ctx, c.db, c.database, index)
		if err != nil {
			return err
		}
		if !installed {
			return fmt.Errorf("DEX query index requires administrative installation: %s", index.name)
		}
	}
	return nil
}
func (c *Client) InitDerivativeSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"derivative_contract_spec", "derivative_trading_rule", "derivative_book_minute", "derivative_book_second_delta", "derivative_book_quality_minute", "derivative_book_foundation_commit"})
}
func (c *Client) InitOptionsLiveSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"options_live_run", "options_metadata_observation", "options_index_minute", "options_live_minute_commit"})
}
func (c *Client) InitOptionsCatalogSchema(ctx context.Context) error {
	return c.checkSchema(ctx, []string{"options_catalog_scope_observation", "options_lifecycle_observation", "options_collection_plan", "options_catalog_quality_evidence_minute", "options_catalog_live_minute_commit"})
}
