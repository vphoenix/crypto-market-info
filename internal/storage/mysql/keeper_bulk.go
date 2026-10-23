package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"reflect"
	"time"
)

func readKeeperGroup[T any](ctx context.Context, c *Client, q executor, table, where string, args []any, out map[uuid.UUID]keeper.Batch, set func(*keeper.Batch, T) error) error {
	rows, err := readModels[T](ctx, c, q, table, where, args...)
	if err != nil {
		return err
	}
	for _, r := range rows {
		id := reflect.ValueOf(r).FieldByName("CaptureId").Interface().(uuid.UUID)
		b, ok := out[id]
		if !ok {
			return contracts.ErrConflict
		}
		if err = set(&b, r); err != nil {
			return err
		}
		out[id] = b
	}
	return nil
}

// KeeperBatches authenticates a bounded set in one consistent snapshot. All
// member tables, including expected-empty extensions, are fetched once per set.
func (c *Client) KeeperBatches(ctx context.Context, caps []keeper.Capture) (map[uuid.UUID]keeper.Batch, error) {
	if len(caps) > 256 {
		return nil, fmt.Errorf("keeper bulk read exceeds quota")
	}
	out := map[uuid.UUID]keeper.Batch{}
	if len(caps) == 0 {
		return out, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var args []any
	for _, cap := range caps {
		if _, ok := out[cap.CaptureId]; ok {
			return nil, contracts.ErrConflict
		}
		out[cap.CaptureId] = keeper.Batch{Capture: cap}
		id := cap.CaptureId
		args = append(args, id[:])
	}
	where := "WHERE capture_id IN (" + placeholders(len(args)) + ")"
	actual, err := readModels[keeper.Capture](ctx, c, tx, "jl_keeper_capture", where, args...)
	if err != nil {
		return nil, err
	}
	if len(actual) != len(caps) {
		return nil, contracts.ErrConflict
	}
	for _, cap := range actual {
		b, ok := out[cap.CaptureId]
		if !ok || !cap.Committed || !keeperCaptureEqual(cap, b.Capture) {
			return nil, contracts.ErrConflict
		}
	}
	rental, err := c.keeperRentalStateAvailable(ctx, tx)
	if err != nil {
		return nil, err
	}
	dynamic, err := c.keeperDynamicAvailable(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.RentalEvent](ctx, c, tx, "jl_keeper_rental_event", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.RentalEvent) error { b.Events = append(b.Events, r); return nil }); err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.TxReceipt](ctx, c, tx, "jl_keeper_tx_receipt", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.TxReceipt) error { b.Receipts = append(b.Receipts, r); return nil }); err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.Probe](ctx, c, tx, "jl_keeper_probe", where+" ORDER BY capture_id,probe_index", args, out, func(b *keeper.Batch, r keeper.Probe) error { b.Probes = append(b.Probes, r); return nil }); err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.CostObservation](ctx, c, tx, "jl_keeper_cost_observation", where+" ORDER BY capture_id,observation_index", args, out, func(b *keeper.Batch, r keeper.CostObservation) error { b.Costs = append(b.Costs, r); return nil }); err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.IndexedEvent](ctx, c, tx, "jl_keeper_indexed_event", where+" ORDER BY capture_id,row_ordinal", args, out, func(b *keeper.Batch, r keeper.IndexedEvent) error {
		b.IndexedEvents = append(b.IndexedEvents, r)
		return nil
	}); err != nil {
		return nil, err
	}
	if err = readKeeperGroup[keeper.IndexPage](ctx, c, tx, "jl_keeper_index_page", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.IndexPage) error {
		if b.IndexPage != nil {
			return contracts.ErrConflict
		}
		row := r
		b.IndexPage = &row
		return nil
	}); err != nil {
		return nil, err
	}
	if rental {
		if err = readKeeperGroup[keeper.RentalStateObservation](ctx, c, tx, "jl_keeper_rental_state_observation", where+" ORDER BY capture_id,observation_index", args, out, func(b *keeper.Batch, r keeper.RentalStateObservation) error {
			b.RentalStates = append(b.RentalStates, r)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if rental {
		if err = readKeeperGroup[keeper.RentalStateBatchProof](ctx, c, tx, "jl_keeper_rental_state_batch", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.RentalStateBatchProof) error {
			if b.RentalStateProof != nil {
				return contracts.ErrConflict
			}
			row := r
			b.RentalStateProof = &row
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if dynamic {
		if err = readKeeperGroup[keeper.RentalRateObservation](ctx, c, tx, "jl_keeper_rental_rate", where+" ORDER BY capture_id,observation_index", args, out, func(b *keeper.Batch, r keeper.RentalRateObservation) error { b.Rates = append(b.Rates, r); return nil }); err != nil {
			return nil, err
		}
	}
	if dynamic {
		if err = readKeeperGroup[keeper.RentalRateBatchProof](ctx, c, tx, "jl_keeper_rental_rate_batch", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.RentalRateBatchProof) error {
			if b.RateProof != nil {
				return contracts.ErrConflict
			}
			row := r
			b.RateProof = &row
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if dynamic {
		if err = readKeeperGroup[keeper.SelectionMember](ctx, c, tx, "jl_keeper_selection_member", where+" ORDER BY capture_id,slot_no", args, out, func(b *keeper.Batch, r keeper.SelectionMember) error {
			b.Selections = append(b.Selections, r)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if dynamic {
		if err = readKeeperGroup[keeper.SelectionRound](ctx, c, tx, "jl_keeper_selection_round", where+" ORDER BY capture_id,capture_id", args, out, func(b *keeper.Batch, r keeper.SelectionRound) error {
			if b.SelectionRound != nil {
				return contracts.ErrConflict
			}
			row := r
			b.SelectionRound = &row
			return nil
		}); err != nil {
			return nil, err
		}
	}
	for _, b := range out {
		if err = keeper.Validate(b); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
func keeperCaptureEqual(a, b keeper.Capture) bool {
	x, e := keeper.FactBytes(a)
	y, f := keeper.FactBytes(b)
	return e == nil && f == nil && string(x) == string(y)
}
