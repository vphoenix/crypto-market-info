package justlendkeeper

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPendingStatusIsReadOnlyAndPreservesFrozenWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.gob")
	if _, err := ReadPendingStatus(path); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("inspection created state")
	}
	now := time.Now().UTC().Truncate(time.Second)
	s := State{Version: 1, Ops: []Operation{{Scheduled: now, Frozen: true}, {Scheduled: now.Add(-time.Hour)}, {Scheduled: now.Add(-time.Minute), Frozen: true}}}
	for _, legacy := range []bool{true, false} {
		if legacy {
			raw, err := Freeze(s)
			if err != nil {
				t.Fatal(err)
			}
			if err = Atomic(path, append([]byte(Hash(raw)), raw...)); err != nil {
				t.Fatal(err)
			}
		} else if err := SaveState(path, s); err != nil {
			t.Fatal(err)
		}
		before, _ := os.ReadFile(path)
		got, err := ReadPendingStatus(path)
		if err != nil || got.FrozenWrites != 2 || got.ScheduledOperations != 3 || !got.OldestFrozen.Equal(now.Add(-time.Minute)) {
			t.Fatal(got, err)
		}
		after, _ := os.ReadFile(path)
		if string(before) != string(after) {
			t.Fatal("inspection modified checkpoint")
		}
		before[len(before)-1] ^= 1
		if err := os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadPendingStatus(path); err == nil {
			t.Fatal("corrupted checkpoint treated as empty queue")
		}
	}
}
