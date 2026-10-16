package reserve

import (
	"context"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"testing"
)

func TestRuntimeHandoffRecoversOriginalFromOwnedReference(t *testing.T) {
	o, db, _, binding, m := runtimeFixture(t)
	c := Collector{Manifest: m, Store: db, runtime: o, StartBlock: 1}
	b := runtimeBatch(m.Hash, "handoff", 1, 10, "latest")
	raw, err := EncodeRuntimeHandoffBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	plan, paths := runtimeinputtest.Handoff(t, binding, raw)
	db.lose = true
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
	if db.writes != 2 || db.history != 0 {
		t.Fatal(db.writes, db.history)
	}
}
func (d *runtimeTestDB) ReserveCaptureExists(_ context.Context, cap Capture) (bool, error) {
	_, ok := d.rows[runtimeIdentity(cap)]
	return ok, nil
}

func TestRuntimeHandoffPrevalidatesRevisionBeforeAnySQL(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_predecessor", true: "ordered_batch_revision"}[valid], func(t *testing.T) {
			o, db, _, binding, m := runtimeFixture(t)
			c := Collector{Manifest: m, Store: db, runtime: o, StartBlock: 1}
			b := runtimeBatch(m.Hash, "ordered", 1, 10, "latest")
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
