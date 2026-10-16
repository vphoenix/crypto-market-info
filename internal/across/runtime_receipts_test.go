package across

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func TestRuntimePagedReceiptsPreserveOriginalPriorityAndEveryIdentity(t *testing.T) {
	o, _, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	s := newRuntimeState(m)
	files, err := runtimecheckpoint.Create(filepath.Join(t.TempDir(), "pages"), binding, []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err = s.enableReceiptPages(files); err != nil {
		t.Fatal(err)
	}
	at := Now()
	expected := map[string]runtimeReceipt{}
	for i := 0; i < 1000; i++ {
		task := receiptTask{b.Capture.ChainId, ID(fmt.Sprintf("block-%d", i)), ID(fmt.Sprintf("tx-%d", i))}
		key := ID(task)
		r := runtimeReceipt{Task: task, Block: uint64(i), FirstObserved: at, Parents: map[string]uint8{"": runtimeReceiptEvent}}
		if i%7 == 0 {
			r.Parents[""] |= runtimeReceiptRefund
		}
		r.recompute()
		if i%3 == 0 {
			r.LastAttempt = at.Add(-2 * time.Minute)
		} else if i%5 == 0 {
			r.LastAttempt = at.Add(-time.Second)
		}
		expected[key] = r
		s.receiptSet(key, r)
	}
	if s.receiptErr != nil {
		t.Fatal(s.receiptErr)
	}
	if len(s.Receipts) != 0 || s.receiptCount() != 1000 {
		t.Fatal("unbounded duplicate receipt map", len(s.Receipts), s.receiptCount())
	}
	readers := map[uint64]*Reader{b.Capture.ChainId: {}}
	for step := 0; step < 20; step++ {
		key := ""
		var selected runtimeReceipt
		for id, r := range expected {
			if !r.Pending || !r.LastAttempt.IsZero() && at.Sub(r.LastAttempt) < time.Minute {
				continue
			}
			urgent := r.Refund && !r.Transfers
			priorUrgent := selected.Refund && !selected.Transfers
			if key == "" || urgent && !priorUrgent || urgent == priorUrgent && (r.LastAttempt.Before(selected.LastAttempt) || r.LastAttempt.Equal(selected.LastAttempt) && id < key) {
				key, selected = id, r
			}
		}
		got, value, ok := s.selectReceipt(readers, at)
		if !ok || got != key || !reflect.DeepEqual(selected, value) {
			t.Fatal("changed original selection", step, Hex(got), Hex(key), s.receiptErr)
		}
		selected.LastAttempt = at
		expected[key] = selected
		s.receiptSet(key, selected)
	}
	refs, err := s.receiptReferences()
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
	var reopened runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &reopened); err != nil {
		t.Fatal(err)
	}
	if err = reopened.enableReceiptPages(files); err != nil {
		t.Fatal(err)
	}
	actual := map[string]runtimeReceipt{}
	reopened.eachReceipt(func(id string, r runtimeReceipt) { actual[id] = r })
	if !reflect.DeepEqual(actual, expected) || reopened.receiptErr != nil {
		t.Fatal("paged restart lost work", len(actual), reopened.receiptErr)
	}
}

func TestRuntimePagedReceiptBranchDependencyOnlyRemovesAffectedParent(t *testing.T) {
	o, _, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Create(t.TempDir(), binding, []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	s := newRuntimeState(m)
	if err = s.enableReceiptPages(files); err != nil {
		t.Fatal(err)
	}
	task := receiptTask{b.Capture.ChainId, ID("block"), ID("tx")}
	s.receiptFact(task, 10, b.Capture.CaptureId, runtimeReceiptEvent, b.Capture.AvailableAt)
	other := ID("other-parent")
	s.receiptFact(task, 10, other, runtimeReceiptRefund, b.Capture.AvailableAt)
	if err = s.capture(b.Capture); err != nil {
		t.Fatal(err)
	}
	orphan := b.Capture
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	if err = s.revise(b.Capture.Revision, orphan); err != nil {
		t.Fatal(err)
	}
	r, ok := s.receiptGet(ID(task))
	if !ok || !r.Pending || !r.Refund || len(r.Parents) != 1 || r.Parents[other] == 0 || len(s.parentReceipts(b.Capture.CaptureId)) != 0 {
		t.Fatal(r, s.receiptErr)
	}
}
