package justlendkeeper

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func checkpointFixture(t *testing.T) (*Collector, State) {
	t.Helper()
	c, _, now := testCollector(t)
	s := c.State
	s.Day, s.Used = now.Format("2006-01-02"), 321
	s.NextGlobal = now.Add(123456789 * time.Nanosecond)
	s.Sources["publicnode"] = SourceLimit{Blocked: true, Limited: 2, Cooldown: now.Add(time.Hour)}
	s.EventCursors["Liquidate"] = *now
	s.SolidAnchors[42] = Hash([]byte("anchored block"))
	s.Candidates = []Candidate{{Renter: "renter", Receiver: "receiver", LastActivity: *now, Origin: RawEvent{Result: map[string]json.RawMessage{"amount": []byte(`"123"`), "receiver": []byte(`"receiver"`)}}}}
	o := c.NewOperation("test", "live", "publicnode")
	o.Frozen = true
	o.Batch.Events = []RentalEvent{{RewardSun: big.NewInt(12345)}}
	o.Requests = []Request{{Body: []byte("pending")}}
	s.Ops = []Operation{o}
	raw, err := Freeze(*s)
	if err != nil {
		t.Fatal(err)
	}
	var copy State
	if err = Thaw(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return c, copy
}

func loadCheckpoint(t *testing.T, c *Collector) State {
	t.Helper()
	s, err := LoadState(c.Path, "test", c.Config)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCheckpointLegacyMigrationAndUnchangedSaves(t *testing.T) {
	c, expected := checkpointFixture(t)
	raw, err := Freeze(expected)
	if err != nil {
		t.Fatal(err)
	}
	if err = Atomic(c.Path, append([]byte(Hash(raw)), raw...)); err != nil {
		t.Fatal(err)
	}
	if got := loadCheckpoint(t, c); !reflect.DeepEqual(got, expected) {
		t.Fatal("legacy recovery changed")
	}
	if err = c.Save(); err != nil {
		t.Fatal(err)
	}
	if got := loadCheckpoint(t, c); !reflect.DeepEqual(got, expected) {
		t.Fatal("migration changed recovery state")
	}
	before, err := os.Stat(c.Path)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	c.stateWriter.write = func(path string, b []byte) error { writes++; return Atomic(path, b) }
	for i := 0; i < 30; i++ {
		// Map iteration, empty slices, and gob normalization must not look dirty.
		if err = c.Save(); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := os.Stat(c.Path)
	if writes != 0 || !os.SameFile(before, after) {
		t.Fatal("idle collector still rewrites state", writes)
	}
	files, _ := os.ReadDir(filepath.Dir(c.Path))
	if len(files) != 2 {
		t.Fatal("expected one checkpoint and one candidate cache", len(files))
	}
}

func TestCheckpointHotStateAndInPlaceMutationsSurviveRestart(t *testing.T) {
	c, _ := checkpointFixture(t)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	cold := candidatePath(c.Path, c.stateWriter.last.CandidateHash)
	before, _ := os.Stat(cold)
	writes := 0
	c.stateWriter.write = func(path string, b []byte) error { writes++; return Atomic(path, b) }
	c.State.Used++
	c.State.NextGlobal = c.State.NextGlobal.Add(time.Nanosecond)
	c.State.Sources["trongrid"] = SourceLimit{Cooldown: c.State.NextGlobal.Add(time.Hour)}
	c.State.EventCursors["Liquidate"] = c.State.NextGlobal
	c.State.Ops[0].Batch.Events[0].RewardSun.SetInt64(67890)
	c.State.Ops[0].Requests[0].Body[0] = 'P'
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(cold)
	if writes != 1 || !os.SameFile(before, after) {
		t.Fatal("hot update rewrote candidate cache", writes)
	}
	got := loadCheckpoint(t, c)
	if got.Used != 322 || !got.NextGlobal.Equal(c.State.NextGlobal) || got.Ops[0].Batch.Events[0].RewardSun.Int64() != 67890 || string(got.Ops[0].Requests[0].Body) != "Pending" || !got.Ops[0].Frozen || got.Ops[0].Batch.Capture.CaptureId != c.State.Ops[0].Batch.Capture.CaptureId {
		t.Fatal("lost durable progress or pending identity")
	}
	if !reflect.DeepEqual(got.Sources, c.State.Sources) || !reflect.DeepEqual(got.EventCursors, c.State.EventCursors) {
		t.Fatal("lost gates/cursors")
	}
	c.State.Candidates[0].Origin.Result["amount"][1] = '9'
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	got = loadCheckpoint(t, c)
	if string(got.Candidates[0].Origin.Result["amount"]) != `"923"` {
		t.Fatal("in-place candidate mutation missed")
	}
	if _, err := os.Stat(cold); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("obsolete cache retained", err)
	}
	// A restarted writer can advance the budget and retain the same frozen batch.
	restarted := NewCollector(c.Config, &got, c.Path, Archive{}, nil)
	restarted.State.Used++
	if err := restarted.Save(); err != nil {
		t.Fatal(err)
	}
	if loadCheckpoint(t, restarted).Used != 323 {
		t.Fatal("restart reset budget")
	}
}

func TestCheckpointInterruptedCommitAndOrphanCleanup(t *testing.T) {
	for _, afterRename := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_commit", true: "after_commit"}[afterRename], func(t *testing.T) {
			c, old := checkpointFixture(t)
			if err := c.Save(); err != nil {
				t.Fatal(err)
			}
			c.State.Candidates[0].Renter = "new renter"
			c.State.Used++
			failure := errors.New("simulated commit interruption")
			c.stateWriter.write = func(path string, b []byte) error {
				if path == c.Path {
					if afterRename {
						if err := Atomic(path, b); err != nil {
							return err
						}
					}
					return failure
				}
				return Atomic(path, b)
			}
			if err := c.Save(); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			got := loadCheckpoint(t, c)
			if afterRename {
				if got.Used != old.Used+1 || got.Candidates[0].Renter != "new renter" {
					t.Fatal("new commit incomplete")
				}
			} else if !reflect.DeepEqual(got, old) {
				t.Fatal("uncommitted cache changed recovery")
			}
			restarted := NewCollector(c.Config, &got, c.Path, Archive{}, nil)
			// Must not delete unrelated files while cleaning crash leftovers.
			if err := os.WriteFile(c.Path+".candidates-not-a-hash", []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Save(); err != nil {
				t.Fatal(err)
			}
			files, _ := os.ReadDir(filepath.Dir(c.Path))
			if len(files) != 3 {
				t.Fatal("orphan cleanup", len(files))
			}
			if !reflect.DeepEqual(loadCheckpoint(t, restarted), got) {
				t.Fatal("cleanup changed state")
			}
		})
	}
}

func TestCheckpointMissingOrCorruptCacheFailsClosed(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "primary"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := checkpointFixture(t)
			if err := c.Save(); err != nil {
				t.Fatal(err)
			}
			path := candidatePath(c.Path, c.stateWriter.last.CandidateHash)
			if mode == "primary" {
				path = c.Path
			}
			if mode == "missing" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else {
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				b[len(b)-1] ^= 1
				if err = os.WriteFile(path, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := LoadState(c.Path, "test", c.Config); err == nil {
				t.Fatal("incomplete recovery accepted")
			}
		})
	}
}

func TestCheckpointBudgetSavesDoNotRewriteLargeCandidates(t *testing.T) {
	c, _ := checkpointFixture(t)
	for i := 0; i < 1000; i++ {
		c.State.Candidates = append(c.State.Candidates, Candidate{Renter: strings.Repeat("public-address-", 20), Origin: RawEvent{Result: map[string]json.RawMessage{"value": []byte(`"100000000000000000001"`)}}})
	}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	legacy, err := Freeze(*c.State)
	if err != nil {
		t.Fatal(err)
	}
	var written int
	c.stateWriter.write = func(path string, b []byte) error { written += len(b); return Atomic(path, b) }
	for i := 0; i < 10; i++ {
		c.State.Used++
		if err := c.Save(); err != nil {
			t.Fatal(err)
		}
	}
	if written >= len(legacy) {
		t.Fatal("ten budget updates wrote more than one legacy snapshot", written, len(legacy))
	}
	data, err := os.ReadFile(c.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Old binaries must reject the new format, never silently lose the cache.
	var old State
	if err := Thaw(data[32:], &old); err == nil {
		t.Fatal("old reader accepted split checkpoint")
	}
	if !bytes.Equal(data[:32], []byte(Hash(data[32:]))) {
		t.Fatal("checkpoint hash")
	}
}
