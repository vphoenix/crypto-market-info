package justlendkeeper

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSchedulerCheckpointSeparatesColdCandidates(t *testing.T) {
	c, _, now := testCollector(t)
	for i := 0; i < 1226; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
	}
	c.State.Scheduler = &SchedulerState{Version: 1, NextCheck: now.Add(time.Minute), Ledger: map[string]ResearchEntry{}}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	writes := map[string]int{}
	c.stateWriter.write = func(path string, b []byte) error { writes[path] += len(b); return Atomic(path, b) }
	c.State.Scheduler.NextCheck = now.Add(2 * time.Minute)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	schedulerBytes := 0
	for path, n := range writes {
		if strings.Contains(path, ".candidates-") {
			t.Fatal("scheduler rewrote cold candidates")
		}
		if strings.Contains(path, ".scheduler-") {
			schedulerBytes += n
		}
	}
	if schedulerBytes == 0 {
		t.Fatal("scheduler remains embedded in hot checkpoint")
	}
	restored, err := LoadState(c.Path, "test", c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Scheduler, c.State.Scheduler) {
		t.Fatal("scheduler restart changed durable progress")
	}
	writes = map[string]int{}
	if err = c.Save(); err != nil {
		t.Fatal(err)
	}
	if len(writes) != 0 {
		t.Fatal("unchanged save performed IO")
	}
	t.Logf("1226 candidates: scheduler update wrote %d scheduler bytes; cold candidate writes 0", schedulerBytes)
}

func TestSchedulerCheckpointPreservesKnownFalsePendingTuple(t *testing.T) {
	c, _, now := testCollector(t)
	c.State.Scheduler = &SchedulerState{Version: 1, Ledger: map[string]ResearchEntry{}}
	o := c.NewOperation("rental_state", "live", "publicnode")
	o.Batch.RentalStates = []RentalStateObservation{{Liquidatable: Ptr(false), RentIndex: big.NewInt(0), SecurityDepositSun: big.NewInt(0)}}
	c.State.Ops = []Operation{o}
	c.State.NextResearchRequest = now.Add(5 * time.Second)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	s, err := LoadState(c.Path, "test", c.Config)
	if err != nil {
		t.Fatal(err)
	}
	r := s.Ops[0].Batch.RentalStates[0]
	if r.Liquidatable == nil || *r.Liquidatable || r.RentIndex == nil || r.RentIndex.Sign() != 0 || r.SecurityDepositSun == nil || !s.NextResearchRequest.Equal(c.State.NextResearchRequest) {
		t.Fatal("known false/zero or request gate lost across restart")
	}
}

func TestRuntimeSchedulerObjectReusesCandidatesAndRestores(t *testing.T) {
	c, _, now := testCollector(t)
	for i := 0; i < 1226; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
	}
	c.State.Scheduler = &SchedulerState{Version: 1, NextCheck: now.Add(time.Minute), Ledger: map[string]ResearchEntry{}}
	dir, binding := keeperRuntimeFixture(t, c)
	before := *c.runtime.last
	m := c.runtime.files.Metrics()
	c.State.Scheduler.NextCheck = now.Add(2 * time.Minute)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	after := *c.runtime.last
	if after.Version != 2 {
		t.Fatal("old runtime readers could silently drop the scheduler")
	}
	if before.Candidates != after.Candidates {
		t.Fatal("runtime scheduler rewrote cold candidates")
	}
	if before.Scheduler == nil || after.Scheduler == nil || *before.Scheduler == *after.Scheduler {
		t.Fatal("scheduler lacks independent immutable reference")
	}
	if after.State.Scheduler != nil {
		t.Fatal("scheduler embedded in request checkpoint")
	}
	t.Logf("runtime scheduler update: bytes=%d file_syncs=%d directory_syncs=%d", c.runtime.files.Metrics().WrittenBytes-m.WrittenBytes, c.runtime.files.Metrics().FileSyncs-m.FileSyncs, c.runtime.files.Metrics().DirectorySyncs-m.DirectorySyncs)
	if err := c.runtime.files.Close(); err != nil {
		t.Fatal(err)
	}
	c.runtime = nil
	c.State.Runtime = nil
	if err := c.OpenRuntime(context.Background(), dir, binding); err != nil {
		t.Fatal(err)
	}
	if c.State.Scheduler == nil || !c.State.Scheduler.NextCheck.Equal(now.Add(2*time.Minute)) {
		t.Fatal("runtime restart lost scheduler")
	}
}

func TestSchedulerCheckpointCorruptionFailsClosed(t *testing.T) {
	c, _, _ := testCollector(t)
	c.State.Scheduler = &SchedulerState{Version: 1, Ledger: map[string]ResearchEntry{}}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	path := schedulerPath(c.Path, c.stateWriter.last.SchedulerHash)
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadState(c.Path, "test", c.Config); err == nil {
		t.Fatal("corrupt scheduler accepted")
	}
}

// Equal logical request updates, with admission and completion mutations for
// each three-request chain. Measures checkpoint overhead only, not SQL/Redis,
// HTTP payload persistence, publication WALs, or physical device IO.
func TestRuntimeSchedulerFixedSequenceIO(t *testing.T) {
	if testing.Short() {
		t.Skip("fixed-sequence IO measurement is run separately")
	}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			c, _, now := testCollector(t)
			for i := 0; i < 1226; i++ {
				c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
			}
			if enabled {
				c.State.Scheduler = &SchedulerState{Version: 1, NextCheck: *now, Ledger: map[string]ResearchEntry{}}
				for _, can := range c.State.Candidates {
					c.State.Scheduler.Ledger[researchGeneration(can)] = ResearchEntry{Renter: can.Renter, Receiver: can.Receiver, OriginNumber: can.Origin.BlockNumber, Verified: true, Revision: seedRevision, NextCheck: *now}
				}
			}
			keeperRuntimeFixture(t, c)
			cold := c.runtime.last.Candidates
			start := c.runtime.files.Metrics()
			for i := 0; i < 60; i++ {
				*now = now.Add(5 * time.Second)
				if enabled && i%3 == 0 {
					s := c.State.Scheduler
					s.InFlightCapture = fmt.Sprintf("00000000-0000-0000-0000-%012d", i/3+1)
					s.Phase = (s.Phase + 1) % 4
					s.OpportunityCursor = researchGeneration(c.State.Candidates[i/3])
				}
				c.State.Used++
				c.State.NextGlobal = now.Add(time.Second)
				c.State.NextResearchRequest = now.Add(5 * time.Second)
				if err := c.Save(); err != nil {
					t.Fatal(err)
				}
				if i%3 == 2 {
					c.State.Rotation++
					if enabled {
						s := c.State.Scheduler
						s.NextCheck = now.Add(30 * time.Second)
						s.InFlightCapture = ""
						key := researchGeneration(c.State.Candidates[i/3])
						entry := s.Ledger[key]
						entry.NextCheck = s.NextCheck
						s.Ledger[key] = entry
						s.OpportunityCursor = key
					}
				}
				if err := c.Save(); err != nil {
					t.Fatal(err)
				}
				if c.runtime.last.Candidates != cold {
					t.Fatal("request sequence rewrote candidate cache")
				}
			}
			end := c.runtime.files.Metrics()
			beforeIdle := end
			for range 60 {
				if err := c.Save(); err != nil {
					t.Fatal(err)
				}
			}
			if c.runtime.files.Metrics().WrittenBytes != beforeIdle.WrittenBytes {
				t.Fatal("idle polling wrote checkpoint")
			}
			t.Logf("opportunity=%v requests=60 completed_chains=20 bytes=%d file_syncs=%d directory_syncs=%d checkpoint_bytes=%d cold_rewrites=0 idle_writes=0", enabled, end.WrittenBytes-start.WrittenBytes, end.FileSyncs-start.FileSyncs, end.DirectorySyncs-start.DirectorySyncs, end.CheckpointBytes)
		})
	}
}

func TestRuntimeRentalPendingRequiresVersionedScheduler(t *testing.T) {
	c, _, _ := testCollector(t)
	keeperRuntimeFixture(t, c)
	c.State.Ops = append(c.State.Ops, c.NewOperation("rental_state", "live", "publicnode"))
	if err := c.Save(); err == nil {
		t.Fatal("new pending observations encoded in an old runtime snapshot")
	}
}
