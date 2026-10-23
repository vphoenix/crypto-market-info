package across

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
)

func TestRuntimeConversionRetainsStorageWithdrawalWithoutCoverage(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "older_interrupted_conversion"}[resume], func(t *testing.T) {
			o, db, _, binding, b, m := runtimeOwnerFixture(t)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			db.rows[b.Capture.CaptureId] = b
			withdrawn := runtimeCapture(m, "known-loss", 11, 20, "finalized")
			withdrawn.Committed = false
			withdrawn.Reason = "storage_integrity_loss: audited failure; recapture required"
			db.rows[withdrawn.CaptureId] = Batch{Capture: withdrawn}
			c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
			dir := filepath.Join(t.TempDir(), "runtime")
			input := runtimeinputtest.Inventory(t, binding, "across")
			check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
			if resume {
				ctx, cancel := context.WithCancel(context.Background())
				_, err := c.ConvertRuntime(ctx, dir, binding, input, check, func(string) { cancel() })
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				stageBinding := binding
				stageBinding.StateVersion += 1000
				files, err := runtimecheckpoint.Open(dir+".conversion", stageBinding, 0)
				if err != nil {
					t.Fatal(err)
				}
				var p conversionPlan
				if err = runtimecheckpoint.UnmarshalTyped(files.State(), &p); err != nil {
					t.Fatal(err)
				}
				// The old converter classified the unchanged withdrawal as unreadable.
				p.Report.WithdrawnDeclarations = nil
				p.Report.Unreadable = []string{Hex(withdrawn.CaptureId) + ": uncommitted declaration"}
				raw, err := runtimecheckpoint.MarshalTyped(p)
				if err != nil {
					t.Fatal(err)
				}
				refs := append(append([]runtimecheckpoint.Reference(nil), p.Pages...), p.Report.InputInventory.References()...)
				if err = files.Checkpoint(raw, refs); err != nil {
					t.Fatal(err)
				}
				if err = files.Close(); err != nil {
					t.Fatal(err)
				}
			}
			report, err := c.ConvertRuntime(context.Background(), dir, binding, input, check, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !report.Verified || len(report.WithdrawnDeclarations) != 1 || ID(report.WithdrawnDeclarations[0]) != ID(withdrawn) || len(report.Unreadable) != 0 || report.RawCursors[withdrawn.ChainId] != 10 {
				t.Fatal("withdrawal changed or incorrectly contributed coverage", report)
			}
			if db.writes != 0 || ID(db.rows[withdrawn.CaptureId].Capture) != ID(withdrawn) {
				t.Fatal("conversion mutated source")
			}
			reads := db.historyReads
			if err = c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
				t.Fatal(err)
			}
			inspection, err := c.InspectRuntime(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			if len(inspection.Conversion.WithdrawnDeclarations) != 1 || db.historyReads != reads || inspection.RawCursors[withdrawn.ChainId] != 10 {
				t.Fatal(inspection)
			}
			if err = c.CloseRuntime(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeConversionStillRejectsUnresolvedDeclarationsAndMissingFacts(t *testing.T) {
	for _, kind := range []string{"uncommitted", "unanchored_withdrawal", "committed_missing_members"} {
		t.Run(kind, func(t *testing.T) {
			o, db, _, binding, b, m := runtimeOwnerFixture(t)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "uncommitted":
				b.Capture.Committed = false
			case "unanchored_withdrawal":
				b.Capture.Committed = false
				b.Capture.Reason = "storage_integrity_loss: audited failure"
				b.Capture.FromBlock = nil
			case "committed_missing_members":
				b.Capture.Reason = "storage_integrity_loss: must not hide committed corruption"
				b.Deposits = nil
			}
			db.rows[b.Capture.CaptureId] = b
			c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
			dir := filepath.Join(t.TempDir(), "runtime")
			report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
			if err == nil || report.Verified {
				t.Fatal("unsafe input accepted", report, err)
			}
			if _, err = os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("published unresolved state", err)
			}
			if db.writes != 0 {
				t.Fatal("conversion wrote facts")
			}
		})
	}
}
