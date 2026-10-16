package across

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func runtimeCapture(m Manifest, id string, from, to uint64, finality string) Capture {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	hash := ID(fmt.Sprint(to))
	return Capture{ManifestHash: m.Hash, CaptureId: ID(id), ChainId: m.Chains[0].ChainID, CaptureKind: "logs", CaptureMode: "live", FromBlock: Ptr(from), ToBlock: Ptr(to), FromHash: Ptr(hash), ToHash: Ptr(hash), StartedAt: at, AvailableAt: at, Status: "complete", Canonical: true, Finality: finality, Revision: 1, Committed: true, ExpectedTasks: 1, CompletedTasks: 1}
}
func TestRuntimeCoverageMatchesHistoricalUnionWithoutRetainingHistory(t *testing.T) {
	m := reportTestManifest()
	s := newRuntimeState(m)
	var history []Capture
	for i := 0; i < 10000; i++ {
		c := runtimeCapture(m, fmt.Sprint(i), uint64(i*2), uint64(i*2+1), "finalized")
		// Keep two actual holes rather than a maximum-height cursor.
		if i == 20 || i == 700 {
			continue
		}
		history = append(history, c)
		if err := s.capture(c); err != nil {
			t.Fatal(err)
		}
	}
	got := s.coverage(m.Chains[0].ChainID, false)
	want := logIntervals(history, m.Chains[0].ChainID)
	for _, start := range []uint64{0, 39, 40, 41, 42, 1399, 1400, 1402, 19999} {
		if firstUncovered(start, 20000, got) != firstUncovered(start, 20000, want) {
			t.Fatal("crossed historical gap", start, got)
		}
	}
	if len(got) != 3 || len(s.Active) != 0 || len(s.captureView()) != 1 {
		t.Fatal("retained completed historical captures", len(got), len(s.Active), len(s.captureView()))
	}
	if gap, ok := newestRawGap(got); !ok || gap.From != 1400 || gap.To != 1401 {
		t.Fatal("lost oldest or newest gap", gap, ok)
	}
}
func TestRuntimeRevisionReorgAndPartialCoverage(t *testing.T) {
	m := reportTestManifest()
	s := newRuntimeState(m)
	final := runtimeCapture(m, "final", 1, 10, "finalized")
	partial := runtimeCapture(m, "partial", 11, 20, "head")
	partial.Status = "partial"
	partial.UnknownEventCount = 1
	newest := runtimeCapture(m, "new", 30, 40, "head")
	for _, c := range []Capture{final, partial, newest} {
		if err := s.capture(c); err != nil {
			t.Fatal(err)
		}
	}
	if firstUncovered(1, 40, s.coverage(final.ChainId, false)) != 21 || firstUncovered(1, 40, s.coverage(final.ChainId, true)) != 11 {
		t.Fatal("raw/decoded conflated")
	}
	orphan := partial
	orphan.Revision = 2
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	s.Watch.Cursors[partial.ChainId] = 20
	if err := s.revise(1, orphan); err != nil {
		t.Fatal(err)
	}
	if s.Watch.Cursors[partial.ChainId] != 10 || len(s.Partial) != 0 {
		t.Fatal("orphan retained coverage")
	}
	promoted := newest
	promoted.Revision = 3
	promoted.Finality = "finalized"
	if err := s.revise(1, promoted); err != nil {
		t.Fatal(err)
	}
	if err := s.revise(1, newest); err != nil {
		t.Fatal(err)
	}
	if s.Chains[newest.ChainId].Anchor.Finality != "finalized" {
		t.Fatal("stale revision undid finality")
	}
	bad := promoted
	bad.Revision++
	bad.Canonical = false
	if err := s.revise(3, bad); err == nil {
		t.Fatal("orphaned finalized anchor")
	}
}
func TestRuntimeReceiptBranchAndUnknownFeeStayDistinct(t *testing.T) {
	m := reportTestManifest()
	s := newRuntimeState(m)
	c := runtimeCapture(m, "event", 1, 2, "head")
	if err := s.capture(c); err != nil {
		t.Fatal(err)
	}
	a := receiptTask{c.ChainId, ID("branch-a"), ID("tx")}
	b := receiptTask{c.ChainId, ID("branch-b"), ID("tx")}
	s.receiptFact(a, 1, c.CaptureId, runtimeReceiptEvent|runtimeReceiptRefund)
	s.receiptFact(b, 1, ID("other-event"), runtimeReceiptEvent)
	s.receiptFact(a, 1, ID("receipt"), runtimeReceiptKnown)
	if !s.Receipts[ID(a)].Pending || s.Receipts[ID(a)].FeeComplete {
		t.Fatal("missing transfers/unknown fee counted complete")
	}
	s.receiptFact(a, 1, ID("transfer"), runtimeReceiptTransfers)
	if s.Receipts[ID(a)].Pending || s.Receipts[ID(a)].FeeComplete {
		t.Fatal("unknown fee manufactured or repeated indefinitely")
	}
	orphan := c
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	if err := s.revise(1, orphan); err != nil {
		t.Fatal(err)
	}
	if !s.Receipts[ID(b)].Pending {
		t.Fatal("different block hash dependency was removed")
	}
}
func TestRuntimeTypedRoundTripPreservesPointersAndIndependentViews(t *testing.T) {
	m := reportTestManifest()
	s := newRuntimeState(m)
	c := runtimeCapture(m, "zero", 0, 0, "head")
	if err := s.capture(c); err != nil {
		t.Fatal(err)
	}
	raw, err := runtimecheckpoint.MarshalTyped(s)
	if err != nil {
		t.Fatal(err)
	}
	var got runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatal("typed state changed during checkpoint")
	}
	changed := got.Active[c.CaptureId]
	*changed.FromBlock = 2
	if *s.Active[c.CaptureId].FromBlock != 0 {
		t.Fatal("worker clone shared pointer with owner")
	}
}

func TestRuntimeOrphanKeepsOriginalCoverageBoundaries(t *testing.T) {
	m := reportTestManifest()
	s := newRuntimeState(m)
	a := runtimeCapture(m, "start", 10, 20, "head")
	b := runtimeCapture(m, "end", 30, 40, "head")
	for _, c := range []Capture{a, b} {
		if err := s.capture(c); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []Capture{a, b} {
		c.Revision++
		c.Canonical = false
		c.Finality = "orphaned"
		if err := s.revise(1, c); err != nil {
			t.Fatal(err)
		}
	}
	gap, ok := s.newestRawGap(a.ChainId)
	if !ok || gap.From != 10 || gap.To != 40 || s.Watch.Cursors[a.ChainId] != 9 {
		t.Fatal("lost original gap endpoints after reorg", gap, ok, s.Watch.Cursors)
	}
}

func TestRuntimeTerminalProbeCancelsRemainingSamplesBeforePruning(t *testing.T) {
	m := reportTestManifest()
	m.MaxPending = 10
	m.FreshMillis = 5000
	s := newRuntimeState(m)
	d := reportTestDeposit(7)
	d.FillDeadline = uint32(Now().Unix() + 60)
	d.AvailableAt = Now()
	d.LiveReceivedAt = Ptr(d.AvailableAt)
	if !s.Watch.add(d, "live", m) {
		t.Fatal("admission")
	}
	work := s.selectProbe(m, Now(), nil)
	if !work.Exists || !work.Live {
		t.Fatal("live baseline priority lost", work)
	}
	p := OrderProbe{ProbeStatus: "ok", FillStatus: Ptr(uint8(2)), AvailableAt: Now()}
	if err := s.probeCommitted(work.Transition, p); err != nil {
		t.Fatal(err)
	}
	if s.Watch.Orders[orderKey(d)] == nil || len(s.Watch.Tasks) != 3 {
		t.Fatal("dropped future samples without cancellation")
	}
	for i := 0; i < 3; i++ {
		work = s.selectProbe(m, Now(), nil)
		if !work.Exists || work.Forced != "cancelled_terminal" {
			t.Fatal(work)
		}
		p.ProbeStatus = "cancelled_terminal"
		if err := s.probeCommitted(work.Transition, p); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.Watch.Orders) != 0 || len(s.Watch.Tasks) != 0 {
		t.Fatal("completed order retained")
	}
}
