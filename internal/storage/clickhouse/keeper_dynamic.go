package clickhouse

import (
	"context"
	"errors"
	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"reflect"
	"strings"
)

func KeeperDynamicSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	var out []string
	for _, t := range keeper.DynamicTables() {
		var cols []string
		for _, col := range keeper.DynamicColumns(t.Model) {
			cols = append(cols, "`"+col.Name+"` "+col.Type)
		}
		out = append(out, "CREATE TABLE IF NOT EXISTS `"+database+"`.`"+t.Name+"` ("+strings.Join(cols, ",")+") ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(capture_started_at) ORDER BY ("+strings.Join(t.Key, ",")+")")
	}
	return out, nil
}
func (c *Client) InitKeeperDynamicSchema(ctx context.Context) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	ss, err := KeeperDynamicSchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, s := range ss {
		if err = c.conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	c.keeperDynamicSchemaChecked = false
	return c.CheckKeeperDynamicSchema(ctx)
}
func (c *Client) CheckKeeperDynamicSchema(ctx context.Context) error {
	ok, err := c.keeperDynamicAvailable(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("keeper_dynamic_schema_required")
	}
	return nil
}
func (c *Client) keeperDynamicAvailable(ctx context.Context) (bool, error) {
	c.keeperDynamicSchemaMu.Lock()
	defer c.keeperDynamicSchemaMu.Unlock()
	if c.keeperDynamicSchemaChecked {
		return c.keeperDynamicSchemaAvailable, nil
	}
	var count uint64
	names := []string{}
	for _, t := range keeper.DynamicTables() {
		names = append(names, t.Name)
	}
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name IN (?)", c.database, names).Scan(&count); err != nil {
		return false, err
	}
	if count != 0 && count != 4 {
		return false, errors.New("keeper_dynamic_schema_incomplete")
	}
	if count == 4 {
		if _, err := keeperRead[keeper.RentalRateObservation](ctx, c, "jl_keeper_rental_rate", "LIMIT 0"); err != nil {
			return false, err
		}
		if _, err := keeperRead[keeper.RentalRateBatchProof](ctx, c, "jl_keeper_rental_rate_batch", "LIMIT 0"); err != nil {
			return false, err
		}
		if _, err := keeperRead[keeper.SelectionMember](ctx, c, "jl_keeper_selection_member", "LIMIT 0"); err != nil {
			return false, err
		}
		if _, err := keeperRead[keeper.SelectionRound](ctx, c, "jl_keeper_selection_round", "LIMIT 0"); err != nil {
			return false, err
		}
	}
	c.keeperDynamicSchemaChecked = true
	c.keeperDynamicSchemaAvailable = count == 4
	return count == 4, nil
}
func (c *Client) readKeeperDynamic(ctx context.Context, out map[uuid.UUID]keeper.Batch, ids []uuid.UUID) error {
	ok, err := c.keeperDynamicAvailable(ctx)
	if err != nil {
		return err
	}
	if !ok {
		for _, b := range out {
			if b.Capture.CaptureKind == "selection_round" || b.Capture.CaptureKind == "rental_rate" {
				return errors.New("keeper_dynamic_schema_required")
			}
		}
		return nil
	}
	rowsRates, err := keeperRead[keeper.RentalRateObservation](ctx, c, "jl_keeper_rental_rate", "WHERE capture_id IN (?) ORDER BY capture_id,observation_index", ids)
	if err != nil {
		return err
	}
	for _, r := range rowsRates {
		b, ok := out[r.CaptureId]
		if !ok {
			return errors.New("unexpected_dynamic_member")
		}
		b.Rates = append(b.Rates, r)
		out[r.CaptureId] = b
	}
	rowsRateProof, err := keeperRead[keeper.RentalRateBatchProof](ctx, c, "jl_keeper_rental_rate_batch", "WHERE capture_id IN (?) ORDER BY capture_id,capture_id", ids)
	if err != nil {
		return err
	}
	for _, r := range rowsRateProof {
		b, ok := out[r.CaptureId]
		if !ok {
			return errors.New("unexpected_dynamic_member")
		}
		if b.RateProof != nil {
			return errors.New("duplicate_dynamic_proof")
		}
		row := r
		b.RateProof = &row
		out[r.CaptureId] = b
	}
	rowsSelections, err := keeperRead[keeper.SelectionMember](ctx, c, "jl_keeper_selection_member", "WHERE capture_id IN (?) ORDER BY capture_id,slot_no", ids)
	if err != nil {
		return err
	}
	for _, r := range rowsSelections {
		b, ok := out[r.CaptureId]
		if !ok {
			return errors.New("unexpected_dynamic_member")
		}
		b.Selections = append(b.Selections, r)
		out[r.CaptureId] = b
	}
	rowsSelectionRound, err := keeperRead[keeper.SelectionRound](ctx, c, "jl_keeper_selection_round", "WHERE capture_id IN (?) ORDER BY capture_id,capture_id", ids)
	if err != nil {
		return err
	}
	for _, r := range rowsSelectionRound {
		b, ok := out[r.CaptureId]
		if !ok {
			return errors.New("unexpected_dynamic_member")
		}
		if b.SelectionRound != nil {
			return errors.New("duplicate_dynamic_proof")
		}
		row := r
		b.SelectionRound = &row
		out[r.CaptureId] = b
	}
	return nil
}
func (c *Client) writeKeeperDynamic(ctx context.Context, b keeper.Batch) error {
	ok, err := c.keeperDynamicAvailable(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if b.Capture.CaptureKind == "selection_round" || b.Capture.CaptureKind == "rental_rate" {
			return errors.New("keeper_dynamic_schema_required")
		}
		return nil
	}
	rowsRates := b.Rates
	if err = keeperPartial(ctx, c, "jl_keeper_rental_rate", b.Capture.CaptureId, rowsRates); err != nil {
		return err
	}
	if err = c.dexInsert(ctx, "jl_keeper_rental_rate", keeperColumns(reflect.TypeOf(keeper.RentalRateObservation{})), keeperValues(rowsRates)); err != nil {
		return err
	}
	var rowsRateProof []keeper.RentalRateBatchProof
	if b.RateProof != nil {
		rowsRateProof = []keeper.RentalRateBatchProof{*b.RateProof}
	}
	if err = keeperPartial(ctx, c, "jl_keeper_rental_rate_batch", b.Capture.CaptureId, rowsRateProof); err != nil {
		return err
	}
	if err = c.dexInsert(ctx, "jl_keeper_rental_rate_batch", keeperColumns(reflect.TypeOf(keeper.RentalRateBatchProof{})), keeperValues(rowsRateProof)); err != nil {
		return err
	}
	rowsSelections := b.Selections
	if err = keeperPartial(ctx, c, "jl_keeper_selection_member", b.Capture.CaptureId, rowsSelections); err != nil {
		return err
	}
	if err = c.dexInsert(ctx, "jl_keeper_selection_member", keeperColumns(reflect.TypeOf(keeper.SelectionMember{})), keeperValues(rowsSelections)); err != nil {
		return err
	}
	var rowsSelectionRound []keeper.SelectionRound
	if b.SelectionRound != nil {
		rowsSelectionRound = []keeper.SelectionRound{*b.SelectionRound}
	}
	if err = keeperPartial(ctx, c, "jl_keeper_selection_round", b.Capture.CaptureId, rowsSelectionRound); err != nil {
		return err
	}
	if err = c.dexInsert(ctx, "jl_keeper_selection_round", keeperColumns(reflect.TypeOf(keeper.SelectionRound{})), keeperValues(rowsSelectionRound)); err != nil {
		return err
	}
	return nil
}
