package across

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func TestRuntimePagedCapturesKeepCoverageAndBoundReconcileView(t *testing.T) {
	o, _, _, binding, _, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Create(t.TempDir(), binding, []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	s := newRuntimeState(m)
	if err = s.enableCapturePages(files); err != nil {
		t.Fatal(err)
	}
	plain := newRuntimeState(m)
	for i := 0; i < 1000; i++ {
		cap := runtimeCapture(m, fmt.Sprint(i), uint64(2*i+1), uint64(2*i+2), "head")
		if i%17 == 0 {
			cap.Status = "partial"
			cap.UnknownEventCount = 1
		}
		if err = s.capture(cap); err != nil {
			t.Fatal(err)
		}
		if err = plain.capture(cap); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.Active) != 0 || len(s.Partial) != 0 || s.captureCount(false) != 1000 {
		t.Fatal("capture working set not paged")
	}
	chain := m.Chains[0].ChainID
	for _, decoded := range []bool{false, true} {
		if !reflect.DeepEqual(s.coverage(chain, decoded), plain.coverage(chain, decoded)) {
			t.Fatal("paged coverage differs")
		}
	}
	rows := s.reconcileCaptures(chain, 24)
	if len(rows) != 24 || *rows[0].FromBlock != 1 || *rows[23].ToBlock != 48 {
		t.Fatal("changed oldest-anchor selection", len(rows))
	}
	orphan := rows[0]
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	if err = s.revise(1, orphan); err != nil {
		t.Fatal(err)
	}
	if s.Watch.Cursors[chain] != 0 || s.captureCount(false) != 999 {
		t.Fatal("orphan lost original prefix gap")
	}
	refs, err := s.captureReferences()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := runtimecheckpoint.MarshalTyped(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = files.Checkpoint(raw, refs); err != nil {
		t.Fatal(err)
	}
	var restored runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &restored); err != nil {
		t.Fatal(err)
	}
	if err = restored.enableCapturePages(files); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.coverage(chain, false), s.coverage(chain, false)) || restored.captureCount(false) != 999 {
		t.Fatal("paged capture restart changed state")
	}
}

func TestRuntimeReorgRestartsRemainingCaptureRevisionsWithoutRPCOrHistory(t *testing.T) {
	o, db, dir, binding, b, m := runtimeOwnerFixture(t)
	var captures []Capture
	for i := 0; i < 55; i++ {
		cap := b.Capture
		cap.CaptureId = ID(fmt.Sprint("reorg-", i))
		db.rows[cap.CaptureId] = Batch{Capture: cap}
		captures = append(captures, cap)
	}
	if err := o.change(context.Background(), func(s *runtimeState) (any, error) {
		for _, cap := range captures {
			if err := s.capture(cap); err != nil {
				return false, err
			}
		}
		return true, nil
	}, false); err != nil {
		t.Fatal(err)
	}
	job := runtimeInvalidation{Chain: b.Capture.ChainId, Bad: []uint64{*b.Capture.ToBlock}, Reason: "finality_proof=frozen-test-proof"}
	if err := o.change(context.Background(), func(s *runtimeState) (any, error) {
		s.Invalidations[ID(job)] = job
		s.rebuildCoverage(job.Chain)
		return true, s.ioError()
	}, true); err != nil {
		t.Fatal(err)
	}
	flaky := &runtimeReorgDB{runtimeTestDB: db}
	first := Collector{Manifest: m, Store: flaky, runtime: o}
	if err := first.runtimeResumeInvalidations(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("did not stop at lost commit result", err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Open(dir, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := newRuntimeOwner(context.Background(), files, m, flaky, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.close()
	c := Collector{Manifest: m, Store: flaky, runtime: restored}
	if err = c.runtimeResumeInvalidations(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = restored.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureCount(false), nil }, &count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	for _, row := range db.rows {
		if row.Capture.Canonical || row.Capture.Finality != "orphaned" || row.Capture.Revision != b.Capture.Revision+1 {
			t.Fatal("missed capture beyond first page")
		}
	}
	if db.historyReads != 0 {
		t.Fatal("reorg recovery scanned SQL history")
	}
}

type runtimeReorgDB struct {
	*runtimeTestDB
	revisions int
}

func (d *runtimeReorgDB) WriteAcrossRevision(ctx context.Context, c Capture) error {
	if err := d.runtimeTestDB.WriteAcrossRevision(ctx, c); err != nil {
		return err
	}
	d.revisions++
	if d.revisions == 17 {
		return context.DeadlineExceeded
	}
	return nil
}
