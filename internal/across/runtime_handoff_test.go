package across

import (
	"context"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"testing"
)

func TestRuntimeHandoffRecoversOriginalFromOwnedReference(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	c := Collector{Manifest: m, Store: db, runtime: o}
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, paths := runtimeinputtest.Handoff(t, binding, raw)
	db.loseReturn = true
	if _, err = c.HandoffRuntime(context.Background(), plan); err == nil {
		t.Fatal("lost commit accepted")
	}
	if err = os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	report, err := c.HandoffRuntime(context.Background(), plan)
	if err != nil || !report.Complete || report.Next != 1 {
		t.Fatal(report, err)
	}
	if db.writes != 2 || db.historyReads != 0 {
		t.Fatal(db.writes, db.historyReads)
	}
	if _, err = c.HandoffRuntime(context.Background(), plan); err != nil || db.writes != 2 {
		t.Fatal("completed plan replayed SQL", err)
	}
}
func (d *runtimeTestDB) AcrossCaptureExists(_ context.Context, cap Capture) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.rows[cap.CaptureId]
	return ok, nil
}
func TestRuntimeHandoffRejectsPreviouslyImportedBatchInNewPlan(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	c := Collector{Manifest: m, Store: db, runtime: o}
	if err := db.WriteAcrossBatch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := runtimeinputtest.Handoff(t, binding, raw)
	if _, err = c.HandoffRuntime(context.Background(), plan); err == nil || db.writes != 1 {
		t.Fatal("old completed identity could recreate pending work", err)
	}
}

func TestRuntimeHandoffPrevalidatesRevisionBeforeAnySQL(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_predecessor", true: "ordered_batch_revision"}[valid], func(t *testing.T) {
			o, db, _, binding, b, m := runtimeOwnerFixture(t)
			c := Collector{Manifest: m, Store: db, runtime: o}
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

func TestRuntimeHandoffNewFactsInCompletedRangeRequireRebuild(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	c := Collector{Manifest: m, Store: db, runtime: o}
	if err := o.change(context.Background(), func(s *runtimeState) (any, error) {
		if len(b.Deposits) == 0 {
			t.Fatal("fixture lacks facts")
		}
		d := b.Deposits[0]
		s.Chains[d.ChainId].Decoded = []RuntimeRange{{d.BlockNumber, d.BlockNumber}}
		return true, nil
	}, false); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := runtimeinputtest.Handoff(t, binding, raw)
	if _, err = c.HandoffRuntime(context.Background(), plan); err == nil || db.writes != 0 {
		t.Fatal("completed range silently discarded new facts", db.writes, err)
	}
}
