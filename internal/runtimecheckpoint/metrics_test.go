package runtimecheckpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryByteCountersMatchFilesAfterCleanup(t *testing.T) {
	s, _ := createTest(t)
	for i := 0; i < 4; i++ {
		op := beginTest(t, s, string(rune('a'+i)))
		if err := s.Applied(op); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint([]byte{byte(i)}, nil); err != nil {
			t.Fatal(err)
		}
		actual, err := s.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		m := s.Metrics()
		if actual.RecoveryBytes != m.RecoveryBytes || m.PeakRecoveryBytes < m.RecoveryBytes {
			t.Fatal(actual.RecoveryBytes, m)
		}
	}
}

func TestCachedImmutableReferenceStillRejectsChangedFile(t *testing.T) {
	s, dir := createTest(t)
	ref, err := s.PutObject([]byte("frozen"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("has-reference"), []Reference{ref}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "objects", ref.Hash+".blob")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint([]byte("changed-state"), []Reference{ref}); err == nil {
		t.Fatal("trusted modified payload because modification time was restored")
	}
}
