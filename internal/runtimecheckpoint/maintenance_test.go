package runtimecheckpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDataRecoveryEpochRequiresExplicitRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outside-backup.epoch")
	if _, err := ReadEpoch(path, "mysql/market"); !errors.Is(err, ErrRecovery) {
		t.Fatal(err)
	}
	one, err := RotateEpoch(path, "mysql/market", "", "verified first conversion")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RotateEpoch(path, "mysql/market", "", "accidental init"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err = RotateEpoch(path, "mysql/market", "wrong", "restore"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err = ReadEpoch(path, "other/database"); !errors.Is(err, ErrRecovery) {
		t.Fatal(err)
	}
	two, err := RotateEpoch(path, "mysql/market", one.ID, "restored database snapshot; collectors stopped")
	if err != nil || two.ID == one.ID || two.Previous != one.ID {
		t.Fatal(two, err)
	}
	s, dir := createTest(t)
	s.Close()
	b := testBinding()
	b.RecoveryEpoch = two.ID
	if opened, err := Open(dir, b, 0); err == nil {
		opened.Close()
		t.Fatal("database rollback accepted existing runtime checkpoint")
	}
}

func TestExportAndExplicitPreviousRecoveryPreserveOriginal(t *testing.T) {
	s, dir := createTest(t)
	op := beginTest(t, s, "parent")
	if err := s.Applied(op); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint([]byte("parent and child committed"), nil); err != nil {
		t.Fatal(err)
	}
	export := filepath.Join(t.TempDir(), "export")
	if err := s.Export(export); err != nil {
		t.Fatal(err)
	}
	copy, err := Open(export, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(copy.State()) != "parent and child committed" {
		t.Fatal("export discarded new state")
	}
	copy.Close()
	if err = s.Export(export); err == nil {
		t.Fatal("overwrote existing export")
	}
	s.Close()
	current := filepath.Join(dir, "checkpoint.current")
	if err = os.WriteFile(current, []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	recovered := filepath.Join(t.TempDir(), "recovered")
	if err = RecoverPrevious(dir, recovered, testBinding(), 0); err != nil {
		t.Fatal(err)
	}
	r, err := Open(recovered, testBinding(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if string(r.State()) != "initial" {
		t.Fatal("not the previous snapshot")
	}
	if pending := r.Pending(0, 10); len(pending) != 1 || pending[0] != op {
		t.Fatal("uncheckpointed commit not retained", pending)
	}
	raw, err := os.ReadFile(current)
	if err != nil || string(raw) != "damaged" {
		t.Fatal("changed damaged source", err)
	}
	if err = os.Remove(s.entries[op.Sequence].Path); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "bad")
	if err = RecoverPrevious(dir, bad, testBinding(), 0); err == nil {
		t.Fatal("published incomplete recovery chain")
	}
	if _, err = os.Stat(bad); !os.IsNotExist(err) {
		t.Fatal("failed conversion exposed destination", err)
	}
}
