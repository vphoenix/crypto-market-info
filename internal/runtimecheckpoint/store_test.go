package runtimecheckpoint

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testBinding() Binding {
	return Binding{Service: "across", Database: "test_across", ServerUUID: "server", ScopeHash: "manifest", SchemaHash: "schema", Producer: "across", Holder: "holder", RecoveryEpoch: "restore-1", ProducerGeneration: 1, StateVersion: 1}
}
func createTest(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Create(dir, testBinding(), []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}
func beginTest(t *testing.T, s *Store, id string) Operation {
	t.Helper()
	ref, err := s.PutObject([]byte("frozen-" + id))
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.Begin(id, "capture", ref, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	return op
}
func reopenTest(t *testing.T, s *Store, dir string) *Store {
	t.Helper()
	s.Close()
	s, err := Open(dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestFrozenIntentRestartAndOutOfOrderCheckpoint(t *testing.T) {
	s, dir := createTest(t)
	one := beginTest(t, s, "one")
	two := beginTest(t, s, "two")
	if err := s.Applied(two); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint([]byte("two committed; one pending; child retained"), nil); err != nil {
		t.Fatal(err)
	}
	s = reopenTest(t, s, dir)
	if got := s.Pending(0, 10); !reflect.DeepEqual(got, []Operation{one}) {
		t.Fatal("checkpoint skipped an unresolved predecessor", got)
	}
	if got, err := s.ReadObject(one.Payload); err != nil || string(got) != "frozen-one" {
		t.Fatal(got, err)
	}
	if got, err := s.Begin("one", "capture", one.Payload, 0, 1); err != nil || got != one {
		t.Fatal("retry changed operation", got, err)
	}
	if !bytes.Equal(s.State(), []byte("two committed; one pending; child retained")) {
		t.Fatal("state lost")
	}
	if err := s.Applied(one); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint([]byte("one and two committed"), nil); err != nil {
		t.Fatal(err)
	}
	// The previous checkpoint still needs one's original payload for replay.
	if _, err := s.ReadObject(one.Payload); err != nil {
		t.Fatal("previous recovery chain was pruned", err)
	}
	three := beginTest(t, s, "three")
	if err := s.Applied(three); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint([]byte("three committed"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.dir, "objects", one.Payload.Hash+".blob")); !os.IsNotExist(err) {
		t.Fatal("completed history was retained", err)
	}
	s = reopenTest(t, s, dir)
	four := beginTest(t, s, "four")
	if four.Sequence != 4 {
		t.Fatal("sequence reused", four)
	}
}

func TestRecoverySealsUnpublishedSequenceBeforeRetry(t *testing.T) {
	s, dir := createTest(t)
	ref, err := s.PutObject([]byte("original identity"))
	if err != nil {
		t.Fatal(err)
	}
	s.fault = func(stage string) error {
		if stage == "issued_before_replace" {
			return errors.New("power loss before high-water replacement")
		}
		return nil
	}
	if _, err = s.Begin("one", "capture", ref, 0, 1); err == nil {
		t.Fatal("write was permitted")
	}
	s = reopenTest(t, s, dir)
	op, err := s.Begin("one", "capture", ref, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := s.entries[op.Sequence].Path
	s.Close()
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir, testBinding(), 0); err == nil {
		reopened.Close()
		t.Fatal("last recovered SQL intent could disappear undetected")
	}
}

func TestPreviousCheckpointRecoveryTailMustBeComplete(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			s, dir := createTest(t)
			op := beginTest(t, s, "one")
			if pending {
				if err := s.Checkpoint([]byte("one pending"), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Applied(op); err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint([]byte("one committed with child"), nil); err != nil {
				t.Fatal(err)
			}
			path := s.entries[op.Sequence].Path
			s.Close()
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(dir, testBinding(), 0); err == nil {
				reopened.Close()
				t.Fatal("previous checkpoint silently lost its recovery chain")
			}
		})
	}
}

func TestIdentityBindingDamageAndBudgetFailClosed(t *testing.T) {
	t.Run("scope and recovery epoch", func(t *testing.T) {
		s, dir := createTest(t)
		s.Close()
		for _, change := range []func(*Binding){func(b *Binding) { b.RecoveryEpoch = "restore-2" }, func(b *Binding) { b.ProducerGeneration++ }, func(b *Binding) { b.ScopeHash = "other" }, func(b *Binding) { b.ServerUUID = "other" }} {
			b := testBinding()
			change(&b)
			if opened, err := Open(dir, b, 0); err == nil {
				opened.Close()
				t.Fatal("changed binding accepted")
			}
		}
	})
	t.Run("conflict", func(t *testing.T) {
		s, _ := createTest(t)
		op := beginTest(t, s, "same")
		different, err := s.PutObject([]byte("different"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Begin("same", "capture", different, 0, 1); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		op.NewRevision++
		if err = s.Applied(op); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	})
	for _, damage := range []string{"checkpoint", "record", "payload", "missing-last", "missing-middle", "issued"} {
		t.Run(damage, func(t *testing.T) {
			s, dir := createTest(t)
			one := beginTest(t, s, "one")
			two := beginTest(t, s, "two")
			path := ""
			switch damage {
			case "checkpoint":
				path = filepath.Join(dir, "checkpoint.current")
			case "record":
				path = s.entries[one.Sequence].Path
			case "payload":
				path, _ = s.objectPath(one.Payload)
			case "issued":
				path = filepath.Join(dir, "issued")
			case "missing-last":
				path = s.entries[two.Sequence].Path
			case "missing-middle":
				path = s.entries[one.Sequence].Path
			}
			s.Close()
			if strings.HasPrefix(damage, "missing-") {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)/2] ^= 0xff
				if err = os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if opened, err := Open(dir, testBinding(), 0); err == nil {
				opened.Close()
				t.Fatal("damaged recovery chain accepted")
			}
		})
	}
	t.Run("budget preserves pending", func(t *testing.T) {
		s, dir := createTest(t)
		op := beginTest(t, s, "one")
		s.hotLimit = s.metrics.HotBytes + 10
		if err := s.Checkpoint(bytes.Repeat([]byte("x"), 1024), nil); !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
		s = reopenTest(t, s, dir)
		if got := s.Pending(0, 1); len(got) != 1 || got[0] != op {
			t.Fatal("pending lost on budget error", got)
		}
	})
}

func TestNoChangeDoesNotSyncAndCompletedTailIsBounded(t *testing.T) {
	s, _ := createTest(t)
	before := s.Metrics()
	for range 10 {
		if err := s.Checkpoint(s.State(), nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Metrics(); got.FileSyncs != before.FileSyncs || got.DirectorySyncs != before.DirectorySyncs {
		t.Fatal("idle checkpoint rewrote state")
	}
	for n := range 128 {
		op := beginTest(t, s, fmt.Sprint(n))
		if err := s.Applied(op); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Due() {
		t.Fatal("128 operation boundary ignored")
	}
	ref, err := s.PutObject([]byte("next"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin("next", "capture", ref, 0, 1); !errors.Is(err, ErrCheckpointDue) {
		t.Fatal("accepted SQL work beyond checkpoint bound", err)
	}
	if err = s.Checkpoint([]byte("128 applied"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin("next", "capture", ref, 0, 1); err == nil {
		t.Fatal("unreferenced payload should have been collected; caller must refreeze")
	}
	// No permission to write SQL was returned for that discarded orphan object.
}

func TestPendingFileIsLinkedAndSurvivesOriginalRemoval(t *testing.T) {
	s, _ := createTest(t)
	path := filepath.Join(t.TempDir(), "pending-batch.gob")
	if err := os.WriteFile(path, []byte("existing frozen payload"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := s.LinkPending(path)
	if err != nil {
		t.Fatal(err)
	}
	linked, _ := s.objectPath(ref)
	a, _ := os.Stat(path)
	b, _ := os.Stat(linked)
	if !os.SameFile(a, b) {
		t.Fatal("pending payload was copied")
	}
	op, err := s.Begin("pending", "capture", ref, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ReadObject(op.Payload); err != nil || string(got) != "existing frozen payload" {
		t.Fatal(string(got), err)
	}
}

func TestSingleOwnerLock(t *testing.T) {
	s, dir := createTest(t)
	if second, err := Open(dir, testBinding(), 0); err == nil {
		second.Close()
		t.Fatal("second owner accepted")
	}
	s.Close()
	second, err := Open(dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}

func TestCheckpointCrashChild(t *testing.T) {
	dir := os.Getenv("CMI_RUNTIME_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	s, err := Open(dir, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stage := os.Getenv("CMI_RUNTIME_CRASH_STAGE")
	s.fault = func(at string) error {
		if at == stage {
			os.Exit(86)
		}
		return nil
	}
	ref, err := s.PutObject([]byte("stable SQL payload"))
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.Begin("operation", "capture", ref, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	// This sentinel is a stand-in for the permission-to-write-SQL boundary; the
	// separate domain integration tests exercise real transactional commit.
	if err = os.WriteFile(filepath.Join(dir, "sql-permitted"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("parent applied and child created"), nil); err != nil {
		t.Fatal(err)
	}
	if stage == "cleanup_before_remove" {
		next := beginTest(t, s, "second")
		s.Applied(next)
		s.Checkpoint([]byte("second applied"), nil)
	}
	t.Fatal("crash boundary not reached")
}

func TestActualProcessCrashesPreserveRecoveryChain(t *testing.T) {
	for _, stage := range []string{"payload_before_replace", "payload_after_replace", "intent_before_replace", "intent_after_replace", "issued_before_replace", "issued_after_replace", "checkpoint.previous_before_replace", "checkpoint.previous_after_replace", "checkpoint.current_before_replace", "checkpoint.current_after_replace", "cleanup_before_remove"} {
		t.Run(stage, func(t *testing.T) {
			s, dir := createTest(t)
			s.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointCrashChild$", "-test.timeout=20s")
			cmd.Env = append(os.Environ(), "CMI_RUNTIME_CRASH_DIR="+dir, "CMI_RUNTIME_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatal("child did not crash at boundary", err, string(out))
			}
			recovered, err := Open(dir, testBinding(), 0)
			if err != nil {
				t.Fatal("intact crash chain could not reopen", err)
			}
			defer recovered.Close()
			_, sqlErr := os.Stat(filepath.Join(dir, "sql-permitted"))
			pending := recovered.Pending(0, 10)
			state := string(recovered.State())
			if sqlErr == nil && len(pending) == 0 && state == "initial" {
				t.Fatal("SQL permission escaped without a recoverable operation")
			}
			if state != "initial" && state != "parent applied and child created" && state != "second applied" {
				t.Fatal("partial checkpoint became visible", state)
			}
			for _, op := range pending {
				if _, err := recovered.ReadObject(op.Payload); err != nil {
					t.Fatal("recovery payload lost", err)
				}
			}
		})
	}
}

func TestCheckpointThirtySecondMergeBoundary(t *testing.T) {
	s, _ := createTest(t)
	at := s.current.WrittenAt
	s.now = func() time.Time { return at.Add(29 * time.Second) }
	op := beginTest(t, s, "one")
	s.Applied(op)
	if s.Due() {
		t.Fatal("early checkpoint")
	}
	s.now = func() time.Time { return at.Add(30 * time.Second) }
	if !s.Due() {
		t.Fatal("checkpoint age ignored")
	}
}

func TestLinkedCheckpointKeepsOriginalOperationAndRejectsOtherGeneration(t *testing.T) {
	s, dir := createTest(t)
	ref, err := s.LinkCurrent()
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(filepath.Join(dir, "checkpoint.current"))
	if err != nil {
		t.Fatal(err)
	}
	linked, err := os.Stat(filepath.Join(dir, "objects", ref.Hash+".blob"))
	if err != nil || !os.SameFile(original, linked) {
		t.Fatal("pending payload was copied", err)
	}
	op, err := s.Begin("existing-ops", "keeper_operation", ref, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("later state"), nil); err != nil {
		t.Fatal(err)
	}
	s = reopenTest(t, s, dir)
	raw, err := s.ReadStateObject(ref)
	if err != nil || string(raw) != "initial" {
		t.Fatal("original Ops lost", string(raw), err)
	}
	other, _ := createTest(t)
	body, err := s.ReadObject(ref)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := other.PutObject(body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.ReadStateObject(foreign); !errors.Is(err, ErrRecovery) {
		t.Fatal("accepted another generation", err)
	}
	if err = s.Applied(op); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("complete including children"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadStateObject(ref); err != nil {
		t.Fatal("previous checkpoint lost its original operation", err)
	}
}
