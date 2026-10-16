package justlendkeeper

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"testing"
	"time"
)

func TestRuntimeHandoffRestartRetainsParentWithoutReplayingScan(t *testing.T) {
	c, _, now := testCollector(t)
	db := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}, lose: true}
	c.Store = db
	dir, binding := keeperRuntimeFixture(t, c)
	b := keeperRuntimePage(c, now.Add(-time.Minute), *now, "", "", "history")
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, paths := runtimeinputtest.Handoff(t, binding, raw)
	if _, err = c.HandoffRuntime(context.Background(), plan); err == nil {
		t.Fatal("lost commit accepted")
	}
	if err = c.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	if err = c.OpenRuntime(context.Background(), dir, binding); !errors.Is(err, runtimecheckpoint.ErrHandoff) {
		t.Fatal(err)
	}
	if err = os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	ctx := runtimecheckpoint.MaintenanceContext(context.Background())
	if err = c.OpenRuntime(ctx, dir, binding); err != nil {
		t.Fatal(err)
	}
	report, err := c.HandoffRuntime(ctx, plan)
	if err != nil || !report.Complete {
		t.Fatal(report, err)
	}
	if db.writes != 2 || db.history != 0 || c.State.Runtime.parents.Len() != 1 {
		t.Fatal(db.writes, db.history, c.State.Runtime.parents.Len())
	}
}
func (d *runtimeKeeperStore) KeeperCaptureExists(_ context.Context, cap Capture) (bool, error) {
	_, ok := d.rows[cap.CaptureId]
	return ok, nil
}

func TestRuntimeHandoffRequiresParentBeforeChild(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(map[bool]string{false: "child_first", true: "parent_first"}[ordered], func(t *testing.T) {
			c, _, now := testCollector(t)
			db := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}}
			c.Store = db
			_, binding := keeperRuntimeFixture(t, c)
			parent := keeperRuntimePage(c, now.Add(-time.Minute), *now, "", "", "history")
			child := Batch{Capture: keeperRuntimeChild(c, parent.Capture, "complete")}
			Seal(&child)
			a, err := EncodeRuntimeHandoffBatch(parent)
			if err != nil {
				t.Fatal(err)
			}
			b, err := EncodeRuntimeHandoffBatch(child)
			if err != nil {
				t.Fatal(err)
			}
			if !ordered {
				a, b = b, a
			}
			plan, _ := runtimeinputtest.Handoff(t, binding, a, b)
			report, err := c.HandoffRuntime(context.Background(), plan)
			if ordered {
				if err != nil || !report.Complete || c.State.Runtime.parents.Len() != 0 {
					t.Fatal(report, err)
				}
			} else if err == nil || db.writes != 0 {
				t.Fatal("child admitted without recoverable parent", db.writes, err)
			}
		})
	}
}
