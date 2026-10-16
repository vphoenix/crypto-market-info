package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestOptionsRunGroupAtomicAcrossSQLBlocksAndStrictReferences(t *testing.T) {
	c := minimalOptionsPlanClient(t)
	ctx := context.Background()
	original, _ := catalogDBFixture(t, c)
	seed := original
	seed.ID = uuid.New()
	if err := c.WriteOptionsRun(ctx, seed); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_live_run")).Scan(&before); err != nil {
		t.Fatal(err)
	}
	runs := make([]options.LiveRun, 513)
	ids := make([]uuid.UUID, len(runs))
	for n := range runs {
		runs[n] = original
		runs[n].ID = uuid.New()
		ids[n] = runs[n].ID
	}
	runs[512] = seed
	runs[512].StartedAt = seed.StartedAt.Add(time.Second)
	ids[512] = seed.ID
	if err := c.WriteOptionsRuns(ctx, runs); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("late conflict accepted", err)
	}
	var count int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_live_run")).Scan(&count); err != nil || count != before {
		t.Fatal("earlier SQL block survived rollback", count, err)
	}
	runs[512] = seed
	for range 2 {
		if err := c.WriteOptionsRuns(ctx, runs); err != nil {
			t.Fatal("frozen group retry", err)
		}
	}
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_live_run")).Scan(&count); err != nil || count != before+512 {
		t.Fatal("group retry duplicated or omitted runs", count, err)
	}
	loaded, err := c.loadOptionsRunGroup(ctx, ids)
	if err != nil || len(loaded) != len(runs) {
		t.Fatal("group reference read", len(loaded), err)
	}
	for _, run := range runs {
		if loaded[run.ID].Hash() != run.Hash() {
			t.Fatal("run content changed")
		}
	}
	if _, err = c.loadOptionsRunGroup(ctx, []uuid.UUID{uuid.New()}); err == nil {
		t.Fatal("missing reference accepted")
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("options_live_run")+" SET run_hash=? WHERE run_id=?", "invalid", ids[0][:]); err != nil {
		t.Fatal(err)
	}
	if _, err = c.loadOptionsRunGroup(ctx, ids); err == nil {
		t.Fatal("invalid stored run digest accepted")
	}
}
