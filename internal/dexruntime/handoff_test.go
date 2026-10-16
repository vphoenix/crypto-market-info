package dexruntime

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"testing"
)

func TestRuntimeHandoffUnknownCommitAndFiniteGapRepair(t *testing.T) {
	s, db, _ := fixtureRuntime(t)
	ctx := context.Background()
	for _, n := range []uint64{100, 102} {
		if err := s.WriteDEXBatch(ctx, dex.Batch{Block: fixtureBlock(n)}); err != nil {
			t.Fatal(err)
		}
	}
	b := fixtureBlock(101)
	raw, err := EncodeHandoffBatch(dex.Batch{Block: b})
	if err != nil {
		t.Fatal(err)
	}
	plan, paths := runtimeinputtest.Handoff(t, fixtureBinding(), raw)
	db.lose = true
	if _, err = s.Handoff(ctx, plan); err == nil {
		t.Fatal("lost commit accepted")
	}
	if err = os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	report, err := s.Handoff(ctx, plan)
	if err != nil || !report.Complete {
		t.Fatal(report, err)
	}
	if gap, err := s.DEXGapStart(ctx, b.Manifest); err != nil || gap != 0 {
		t.Fatal(gap, err)
	}
	if db.writes != 4 || db.history != 0 {
		t.Fatal(db.writes, db.history)
	}
}

func TestRuntimeHandoffPrevalidatesRevisionBeforeAnySQL(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_predecessor", true: "ordered_batch_revision"}[valid], func(t *testing.T) {
			s, db, _ := fixtureRuntime(t)
			b := fixtureBlock(100)
			raw, err := EncodeHandoffBatch(dex.Batch{Block: b})
			if err != nil {
				t.Fatal(err)
			}
			expected := b.Revision
			b.Revision++
			if !valid {
				expected += 10
				b.Revision = expected + 1
			}
			next, err := EncodeHandoffRevision(b, expected)
			if err != nil {
				t.Fatal(err)
			}
			plan, _ := runtimeinputtest.Handoff(t, fixtureBinding(), raw, next)
			report, err := s.Handoff(context.Background(), plan)
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
