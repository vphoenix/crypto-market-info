package runtimecheckpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestHandoffIncompleteBlocksStartupAndResumesSamePlan(t *testing.T) {
	s, dir := createTest(t)
	source := []byte("frozen explicit IDs and target")
	work := []HandoffWork{{ID: "one", Kind: "capture", Payload: []byte("original one")}, {ID: "two", Kind: "revision", Payload: []byte("original two"), ExpectedRevision: 3, NewRevision: 4}}
	if err := s.StartHandoff(source, work); err != nil {
		t.Fatal(err)
	}
	item, _ := s.HandoffNext()
	op, err := s.Begin(item.ID, item.Kind, item.Payload, item.ExpectedRevision, item.NewRevision)
	if err != nil {
		t.Fatal(err)
	}
	// SQL committed, process dies before state/checkpoint publication.
	s.Close()
	if normal, err := Open(dir, testBinding(), 0); !errors.Is(err, ErrHandoff) {
		if normal != nil {
			normal.Close()
		}
		t.Fatal("incomplete plan admitted collector", err)
	}
	s, err = OpenContext(MaintenanceContext(context.Background()), dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Pending(0, 16); len(got) != 1 || got[0] != op {
		t.Fatal(got)
	}
	if err = s.StartHandoff([]byte("another plan"), work); !errors.Is(err, ErrHandoff) {
		t.Fatal("replaced in-progress plan", err)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("one and its child"), nil); err != nil {
		t.Fatal(err)
	}
	item, _ = s.HandoffNext()
	if item.ID != "two" {
		t.Fatal(item)
	}
	if _, err = s.Begin("unlisted", "capture", item.Payload, 0, 0); !errors.Is(err, ErrHandoff) {
		t.Fatal("unlisted SQL admitted", err)
	}
	op, err = s.Begin(item.ID, item.Kind, item.Payload, item.ExpectedRevision, item.NewRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("both verified and applied"), nil); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Handoff().Complete || s.Handoff().Next != 2 {
		t.Fatal(s.Handoff())
	}
	if err = s.StartHandoff(source, work); err != nil {
		t.Fatal("completed plan replay", err)
	}
	if _, more := s.HandoffNext(); more {
		t.Fatal("completed plan reapplied")
	}
	s.Close()
}
func TestHandoffCheckpointDiskFullRetainsUncoveredOriginal(t *testing.T) {
	s, dir := createTest(t)
	work := []HandoffWork{{ID: "one", Kind: "capture", Payload: []byte("original")}}
	if err := s.StartHandoff([]byte("plan"), work); err != nil {
		t.Fatal(err)
	}
	item, _ := s.HandoffNext()
	op, err := s.Begin(item.ID, item.Kind, item.Payload, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	s.fault = func(stage string) error {
		if stage == "checkpoint.current_before_replace" {
			return syscall.ENOSPC
		}
		return nil
	}
	if err = s.Checkpoint([]byte("committed and children"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenContext(MaintenanceContext(context.Background()), dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Handoff().Complete || s.Handoff().Next != 0 || len(s.Pending(0, 16)) != 1 {
		t.Fatal("disk full forged completion", s.Handoff())
	}
	if _, err = os.Stat(filepath.Join(dir, "objects", op.Payload.Hash+".blob")); err != nil {
		t.Fatal("last original reference removed", err)
	}
}

func TestUnresolvedTaskRejectsDifferentRevisionButAllowsOriginalRetry(t *testing.T) {
	s, _ := createTest(t)
	a, err := s.PutObject([]byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Begin("capture", "capture", a, 0, 1, "same-task")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.PutObject([]byte("new proposal"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin("revision", "revision", b, 1, 2, "same-task"); !errors.Is(err, ErrBusy) {
		t.Fatal("unresolved task admitted twice", err)
	}
	retry, err := s.Begin("capture", "capture", a, 0, 1, "same-task")
	if err != nil || retry != first {
		t.Fatal(retry, err)
	}
	if err = s.Applied(first); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin("revision", "revision", b, 1, 2, "same-task"); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffPreviousRecoveryRetainsUnissuedWork(t *testing.T) {
	s, dir := createTest(t)
	work := []HandoffWork{{ID: "one", Kind: "capture", Payload: []byte("first")}, {ID: "two", Kind: "capture", Payload: []byte("not issued yet")}}
	if err := s.StartHandoff([]byte("two explicit operations"), work); err != nil {
		t.Fatal(err)
	}
	item, _ := s.HandoffNext()
	op, err := s.Begin(item.ID, item.Kind, item.Payload, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Close() // SQL result unknown, current checkpoint then becomes unreadable.
	if err = os.WriteFile(filepath.Join(dir, "checkpoint.current"), []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	recovered := filepath.Join(t.TempDir(), "recovered")
	if err = RecoverPrevious(dir, recovered, testBinding(), 0); err != nil {
		t.Fatal(err)
	}
	if normal, err := Open(recovered, testBinding(), 0); !errors.Is(err, ErrHandoff) {
		if normal != nil {
			normal.Close()
		}
		t.Fatal("previous recovery forgot maintenance barrier", err)
	}
	s, err = OpenContext(MaintenanceContext(context.Background()), recovered, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Pending(0, 16); len(got) != 1 || got[0] != op {
		t.Fatal(got)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	item, ok := s.HandoffNext()
	if !ok || item.ID != "two" || s.Handoff().Complete {
		t.Fatal("lost unissued work", item, s.Handoff())
	}
	if raw, err := s.ReadObject(item.Payload); err != nil || string(raw) != "not issued yet" {
		t.Fatal(string(raw), err)
	}
}

func TestHandoffPreviousMirrorFailureDoesNotAdmitSQL(t *testing.T) {
	s, dir := createTest(t)
	defer s.Close()
	previousWrites := 0
	s.fault = func(stage string) error {
		if stage == "checkpoint.previous_before_replace" {
			previousWrites++
			if previousWrites == 2 {
				return syscall.ENOSPC
			}
		}
		return nil
	}
	if err := s.StartHandoff([]byte("plan"), []HandoffWork{{ID: "one", Kind: "capture", Payload: []byte("original")}}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	item, _ := s.HandoffNext()
	if _, err := s.Begin(item.ID, item.Kind, item.Payload, 0, 0); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal("SQL admitted after failed barrier", err)
	}

	s.Close()
	var err error
	s, err = OpenContext(MaintenanceContext(context.Background()), dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.previous.Handoff.Plan != s.current.Handoff.Plan {
		t.Fatal("resume failed to restore both roots")
	}
	s.Close()
}

func TestHandoffInspectionAndExportPreserveIncompleteSource(t *testing.T) {
	s, dir := createTest(t)
	if err := s.StartHandoff([]byte("finite plan"), []HandoffWork{{ID: "one", Kind: "capture", Payload: []byte("original")}, {ID: "two", Kind: "capture", Payload: []byte("unissued")}}); err != nil {
		t.Fatal(err)
	}
	item, _ := s.HandoffNext()
	if _, err := s.Begin(item.ID, item.Kind, item.Payload, 0, 0); err != nil {
		t.Fatal(err)
	}
	s.Close()
	names := []string{"checkpoint.current", "checkpoint.previous", "issued"}
	before := map[string]string{}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		before[name] = string(raw)
	}
	export := filepath.Join(t.TempDir(), "export")
	report, err := InspectDirectory(dir, testBinding(), 0, export)
	if err != nil || report.Handoff.Complete || report.Handoff.Total != 2 || report.Pending != 1 {
		t.Fatal(report, err)
	}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(raw) != before[name] {
			t.Fatal("inspection modified source", name, err)
		}
	}
	copied, err := InspectDirectory(export, testBinding(), 0, "")
	if err != nil || copied.Handoff != report.Handoff || copied.Pending != 1 {
		t.Fatal(copied, err)
	}
	if normal, err := Open(export, testBinding(), 0); !errors.Is(err, ErrHandoff) {
		if normal != nil {
			normal.Close()
		}
		t.Fatal("export cleared maintenance gate", err)
	}
}
