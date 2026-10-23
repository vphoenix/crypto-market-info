package clickhouse

import (
	"context"
	_ "embed"
	"errors"
	"reflect"
	"regexp"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

//go:embed keeper_rental_state_schema.sql
var keeperRentalDDL string

func KeeperRentalStateSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	ss := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (jl_keeper_\w+).*?;`).FindAllString(keeperRentalDDL, -1)
	if len(ss) != 2 {
		return nil, errors.New("keeper_rental_state_expected_two_tables")
	}
	for i, s := range ss {
		ss[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (jl_keeper_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	return ss, nil
}

// Only the explicit administrative command invokes this DDL path.
func (c *Client) InitKeeperRentalStateSchema(ctx context.Context) error {
	if c.readOnly {
		return errors.New("read_only")
	}
	ss, err := KeeperRentalStateSchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, s := range ss {
		if err = c.conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	c.keeperRentalSchemaMu.Lock()
	c.keeperRentalSchemaChecked = false
	c.keeperRentalSchemaMu.Unlock()
	return c.CheckKeeperRentalStateSchema(ctx)
}
func (c *Client) CheckKeeperRentalStateSchema(ctx context.Context) error {
	available, err := c.keeperRentalStateAvailable(ctx)
	if err != nil {
		return err
	}
	if !available {
		return errors.New("keeper_rental_state_schema_required")
	}
	return nil
}
func (c *Client) keeperRentalStateAvailable(ctx context.Context) (bool, error) {
	c.keeperRentalSchemaMu.Lock()
	defer c.keeperRentalSchemaMu.Unlock()
	if c.keeperRentalSchemaChecked {
		return c.keeperRentalSchemaAvailable, nil
	}
	var count uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name IN ('jl_keeper_rental_state_observation','jl_keeper_rental_state_batch')", c.database).Scan(&count); err != nil {
		return false, err
	}
	if count != 0 && count != 2 {
		return false, errors.New("keeper_rental_state_schema_incomplete")
	}
	if count == 2 {
		// Verify every typed column even for an empty installation.
		if _, err := keeperRead[keeper.RentalStateObservation](ctx, c, "jl_keeper_rental_state_observation", "LIMIT 0"); err != nil {
			return false, err
		}
		if _, err := keeperRead[keeper.RentalStateBatchProof](ctx, c, "jl_keeper_rental_state_batch", "LIMIT 0"); err != nil {
			return false, err
		}
	}
	c.keeperRentalSchemaChecked = true
	c.keeperRentalSchemaAvailable = count == 2
	return c.keeperRentalSchemaAvailable, nil
}
func (c *Client) readKeeperRentalStates(ctx context.Context, out map[uuid.UUID]keeper.Batch, ids []uuid.UUID) error {
	available, err := c.keeperRentalStateAvailable(ctx)
	if err != nil {
		return err
	}
	if !available {
		for _, b := range out {
			if b.Capture.CaptureKind == "rental_state" {
				return errors.New("keeper_rental_state_schema_required")
			}
		}
		return nil
	}
	states, err := keeperRead[keeper.RentalStateObservation](ctx, c, "jl_keeper_rental_state_observation", "WHERE capture_id IN (?) ORDER BY capture_id,observation_index", ids)
	if err != nil {
		return err
	}
	for _, r := range states {
		b, ok := out[r.CaptureId]
		if !ok {
			return errors.New("unexpected_rental_state_capture")
		}
		b.RentalStates = append(b.RentalStates, r)
		out[r.CaptureId] = b
	}
	proofs, err := keeperRead[keeper.RentalStateBatchProof](ctx, c, "jl_keeper_rental_state_batch", "WHERE capture_id IN (?)", ids)
	if err != nil {
		return err
	}
	for _, r := range proofs {
		b, ok := out[r.CaptureId]
		if !ok || b.RentalStateProof != nil {
			return errors.New("unexpected_or_duplicate_rental_state_proof")
		}
		proof := r
		b.RentalStateProof = &proof
		out[r.CaptureId] = b
	}
	return nil
}
func (c *Client) writeKeeperRentalState(ctx context.Context, b keeper.Batch) error {
	available, err := c.keeperRentalStateAvailable(ctx)
	if err != nil {
		return err
	}
	if !available {
		if b.Capture.CaptureKind == "rental_state" {
			return errors.New("keeper_rental_state_schema_required")
		}
		return nil
	}
	var proofs []keeper.RentalStateBatchProof
	if b.RentalStateProof != nil {
		proofs = []keeper.RentalStateBatchProof{*b.RentalStateProof}
	}
	if err = keeperPartial(ctx, c, "jl_keeper_rental_state_observation", b.Capture.CaptureId, b.RentalStates); err != nil {
		return err
	}
	if err = keeperPartial(ctx, c, "jl_keeper_rental_state_batch", b.Capture.CaptureId, proofs); err != nil {
		return err
	}
	if err = c.dexInsert(ctx, "jl_keeper_rental_state_observation", keeperColumns(reflect.TypeOf(keeper.RentalStateObservation{})), keeperValues(b.RentalStates)); err != nil {
		return err
	}
	return c.dexInsert(ctx, "jl_keeper_rental_state_batch", keeperColumns(reflect.TypeOf(keeper.RentalStateBatchProof{})), keeperValues(proofs))
}
