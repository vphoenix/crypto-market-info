package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (c *Client) validateOptionsRunGroup(ctx context.Context, runs []options.LiveRun) error {
	if len(runs) > 2048 {
		return fmt.Errorf("run group read quota exceeded")
	}
	ids := []uint32{}
	seen := map[uint32]bool{}
	for _, r := range runs {
		if err := r.Validate(); err != nil {
			return err
		}
		for _, m := range r.Members {
			if !seen[m.InstrumentID] {
				seen[m.InstrumentID] = true
				ids = append(ids, m.InstrumentID)
			}
		}
	}
	slices.Sort(ids)
	byID := map[uint32]options.ContractSpec{}
	for start := 0; start < len(ids); start += 448 {
		specs, err := c.LoadDerivativeSpecs(ctx, ids[start:min(start+448, len(ids))])
		if err != nil {
			return err
		}
		for _, s := range specs {
			byID[s.Instrument.ID] = s
		}
	}
	for _, r := range runs {
		specs := make([]options.ContractSpec, len(r.Members))
		for n, m := range r.Members {
			specs[n] = byID[m.InstrumentID]
		}
		if err := validateOptionsRunSpecList(r, specs); err != nil {
			return err
		}
	}
	return nil
}

// WriteOptionsRuns freezes the same individual run identities in one bounded
// metadata transaction. Plan predecessor/boundary publication is still separate.
func (c *Client) WriteOptionsRuns(ctx context.Context, runs []options.LiveRun) error {
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if err := c.validateOptionsRunGroup(ctx, runs); err != nil {
		return err
	}
	table, err := c.mapping("options_live_run")
	if err != nil {
		return err
	}
	rows := make([]record, 0, len(runs))
	for _, r := range runs {
		row, e := optionsRunRecord(r)
		if e != nil {
			return e
		}
		rows = append(rows, row)
	}
	return c.transaction(ctx, func(tx *sql.Tx) error { return putImmutableRows(ctx, tx, c.table(table.Name), table, rows) })
}

func (c *Client) loadOptionsRunGroup(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]options.LiveRun, error) {
	if len(ids) > 2048 {
		return nil, fmt.Errorf("run group read quota exceeded")
	}
	out := map[uuid.UUID]options.LiveRun{}
	for start := 0; start < len(ids); start += 256 {
		part := ids[start:min(start+256, len(ids))]
		want := map[uuid.UUID]bool{}
		args := make([]any, len(part))
		for n, id := range part {
			if want[id] {
				return nil, contracts.ErrConflict
			}
			want[id] = true
			args[n] = id[:]
		}
		rows, err := c.records(ctx, c.db, "options_live_run", "WHERE run_id IN ("+placeholders(len(args))+")", args...)
		if err != nil {
			return nil, err
		}
		if len(rows) != len(part) {
			return nil, fmt.Errorf("missing run group reference")
		}
		for _, row := range rows {
			r, err := optionsRunFromRecord(row)
			if err != nil {
				return nil, err
			}
			if !want[r.ID] {
				return nil, contracts.ErrConflict
			}
			if _, ok := out[r.ID]; ok {
				return nil, contracts.ErrConflict
			}
			out[r.ID] = r
		}
	}
	runs := make([]options.LiveRun, 0, len(out))
	for _, id := range ids {
		runs = append(runs, out[id])
	}
	if err := c.validateOptionsRunGroup(ctx, runs); err != nil {
		return nil, err
	}
	return out, nil
}
