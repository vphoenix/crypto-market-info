package lst

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"testing"
)

func TestRuntimeHandoffRestartBlocksWatchAndKeepsOriginalPayload(t *testing.T) {
	c, db, dir, binding, b := runtimeFixture(t)
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, paths := runtimeinputtest.Handoff(t, binding, raw)
	db.lose = true
	if _, err = c.HandoffRuntime(context.Background(), plan); err == nil {
		t.Fatal("lost commit accepted")
	}
	if err = c.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	if err = c.OpenRuntime(context.Background(), dir, binding, nil); !errors.Is(err, runtimecheckpoint.ErrHandoff) {
		t.Fatal("watch accepted partial plan", err)
	}
	if err = os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	ctx := runtimecheckpoint.MaintenanceContext(context.Background())
	if err = c.OpenRuntime(ctx, dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	report, err := c.HandoffRuntime(ctx, plan)
	if err != nil || !report.Complete || report.Next != 1 {
		t.Fatal(report, err)
	}
	if db.writes != 2 || db.history != 0 {
		t.Fatal(db.writes, db.history)
	}
}
func (d *runtimeTestDB) LSTCaptureExists(_ context.Context, cap Capture) (bool, error) {
	_, ok := d.rows[cap.CaptureId]
	return ok, nil
}

func TestRuntimeHandoffPrevalidatesRevisionBeforeAnySQL(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_predecessor", true: "ordered_batch_revision"}[valid], func(t *testing.T) {
			c, db, _, binding, b := runtimeFixture(t)
			raw, err := EncodeRuntimeHandoffBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			revision := b.Capture
			revision.Revision++
			expected := b.Capture.Revision
			if !valid {
				expected += 10
				revision.Revision = expected + 1
			}
			next, err := EncodeRuntimeHandoffRevision(revision, expected)
			if err != nil {
				t.Fatal(err)
			}
			plan, _ := runtimeinputtest.Handoff(t, binding, raw, next)
			report, err := c.HandoffRuntime(context.Background(), plan)
			if valid {
				if err != nil || !report.Complete || report.Next != 2 {
					t.Fatal(report, err)
				}
			} else if err == nil || db.writes != 0 {
				t.Fatal("invalid predecessor admitted SQL", db.writes, err)
			}
		})
	}
}
