package across

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// Writes notify the test only after observing an uncancelled persistence
// context. The runner remains the sole owner of the embedded store's slices.
type priorityProbeStore struct {
	runnerProtocolStore
	writes chan Batch
}

func (s *priorityProbeStore) WriteAcrossBatch(ctx context.Context, b Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.runnerProtocolStore.WriteAcrossBatch(ctx, b); err != nil {
		return err
	}
	s.writes <- b
	return nil
}

func priorityProbeCollector(t *testing.T) (*Collector, *priorityProbeStore) {
	t.Helper()
	m := runnerManifest(t)
	source, _ := m.Chain(8453)
	target, _ := m.Chain(42161)
	store := &priorityProbeStore{writes: make(chan Batch, 32)}
	c := &Collector{Manifest: m, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}, Readers: map[uint64]*Reader{
		8453: protocolChainReader(t, source, nil), 42161: protocolChainReader(t, target, nil),
	}}
	return c, store
}

func TestProbePriorityLiveDiscoveryPreemptsAndPersistsOrdinaryProbe(t *testing.T) {
	c, store := priorityProbeCollector(t)
	old := protocolDepositForRunner(c.Manifest)
	live := old
	live.DepositId = big.NewInt(2)
	live.RelayHash = strings.Repeat("n", 32)
	live.BlockNumber++
	w := &watcher{Orders: map[string]*pendingOrder{}, Cursors: map[uint64]uint64{}}
	if !w.add(old, "restart", c.Manifest) {
		t.Fatal("fixture order rejected")
	}
	entered := make(chan time.Duration, 1)
	cancelled := make(chan struct{})
	var first atomic.Bool
	var urgentCalls atomic.Int32
	c.Readers[8453].RPC.BeforeRequest = func(ctx context.Context, _ int) error {
		urgent, _ := ctx.Value(probePriorityKey{}).(bool)
		if urgent {
			urgentCalls.Add(1)
			return nil
		}
		if first.CompareAndSwap(false, true) {
			deadline, _ := ctx.Deadline()
			entered <- time.Until(deadline)
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	input := make(chan discoveredOrder, 1)
	done := make(chan error, 1)
	go func() { done <- c.probeLoop(ctx, w, input) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("probe loop cancellation: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("probe loop did not stop")
		}
	}()
	select {
	case budget := <-entered:
		if budget < 10*time.Second || budget > 12*time.Second {
			t.Fatalf("restored probe has wrong network budget: %s", budget)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary probe did not enter cancellable network wait")
	}
	live.AvailableAt = Now()
	input <- discoveredOrder{deposit: live, origin: "live"}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("live discovery did not preempt old network context")
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for step := 0; step < 2; step++ {
		select {
		case b := <-store.writes:
			if len(b.Probes) != 1 || !b.Capture.Committed {
				t.Fatal("missing committed probe evidence")
			}
			p := b.Probes[0]
			if step == 0 {
				if p.RelayHash != old.RelayHash || p.ProbeStatus != "rpc_error" || !strings.HasPrefix(p.Reason, "probe_preempted_for_live:") || b.Capture.CompletedTasks != 0 {
					t.Fatalf("preemption was not durably recorded as unknown: %+v", p)
				}
			} else if p.RelayHash != live.RelayHash || p.ProbeStatus != "ok" || p.ObservationOrigin != "live" || p.TargetDelayMs != nil || b.Capture.CompletedTasks != 1 {
				t.Fatalf("fresh live baseline was not the next actual probe: %+v", p)
			}
		case <-deadline.C:
			t.Fatal("cancelled old network budget blocked persistence or the fresh probe")
		}
	}
	if urgentCalls.Load() == 0 {
		t.Fatal("new live baseline did not use urgent RPC quota")
	}
}

func TestProbePriorityFreshBaselineBeforeLateFollowupAndRestored(t *testing.T) {
	c, store := priorityProbeCollector(t)
	base := protocolDepositForRunner(c.Manifest)
	fresh, late, restored := base, base, base
	fresh.RelayHash, late.RelayHash, restored.RelayHash = ID("fresh"), ID("late"), ID("restored")
	fresh.DepositId, late.DepositId, restored.DepositId = big.NewInt(1), big.NewInt(2), big.NewInt(3)
	keyFresh, keyLate, keyRestored := orderKey(fresh), orderKey(late), orderKey(restored)
	when := Now()
	w := &watcher{Orders: map[string]*pendingOrder{
		keyFresh: {Deposit: fresh, Origin: "live"}, keyLate: {Deposit: late, Origin: "live"}, keyRestored: {Deposit: restored, Origin: "restart"},
	}, Tasks: []probeTask{
		{Key: keyRestored, Planned: when.Add(-time.Hour), Kind: "baseline"},
		{Key: keyLate, Planned: when.Add(-2 * time.Second), Kind: "followup", Delay: Ptr(uint32(2000))},
		{Key: keyFresh, Planned: when.Add(-time.Millisecond), Kind: "baseline"},
	}}
	if err := c.runTasks(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 1 || store.batches[0].Probes[0].RelayHash != fresh.RelayHash || store.batches[0].Probes[0].ProbeStatus != "ok" {
		t.Fatal("old planned timestamp displaced the fresh baseline", store.batches)
	}
	if len(w.Tasks) != 2 || w.Tasks[0].Key != keyLate || w.Tasks[1].Key != keyRestored {
		t.Fatalf("unexecuted tasks were dropped or reordered incorrectly: %+v", w.Tasks)
	}
}

func TestProbePriorityEarliestDeadlineFollowupBeforeFreshBaseline(t *testing.T) {
	c, store := priorityProbeCollector(t)
	c.Manifest.FreshMillis = 5000
	baseline := protocolDepositForRunner(c.Manifest)
	followup := baseline
	followup.DepositId = big.NewInt(2)
	followup.RelayHash = ID("earliest-deadline-followup")
	baselineKey, followupKey := orderKey(baseline), orderKey(followup)
	now := Now()
	w := &watcher{Orders: map[string]*pendingOrder{
		baselineKey: {Deposit: baseline, Origin: "live"},
		followupKey: {Deposit: followup, Origin: "live"},
	}, Tasks: []probeTask{
		// The baseline was planned first and is still fresh, but has four
		// seconds left. The later followup has only half a second left.
		{Key: baselineKey, Planned: now.Add(-time.Second), Kind: "baseline"},
		{Key: followupKey, Planned: now.Add(-500 * time.Millisecond), Kind: "followup", Delay: Ptr(uint32(2000))},
	}}
	if err := c.runTasks(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 1 {
		t.Fatalf("expected one admitted probe, got %d", len(store.batches))
	}
	p := store.batches[0].Probes[0]
	if p.RelayHash != followup.RelayHash || p.TargetDelayMs == nil || *p.TargetDelayMs != 2000 || p.ProbeStatus != "ok" {
		t.Fatalf("baseline priority or planned-time ordering displaced the urgent followup: %+v", p)
	}
	if len(w.Tasks) != 1 || w.Tasks[0].Key != baselineKey {
		t.Fatalf("fresh baseline was lost instead of retained for the next admission: %+v", w.Tasks)
	}
}

func TestProbePriorityWakeWaitsForFutureLiveInsteadOfSpinning(t *testing.T) {
	for _, kind := range []string{"restored", "open_check", "late_followup"} {
		t.Run(kind, func(t *testing.T) {
			now := Now()
			ordinary := &pendingOrder{Origin: "restart"}
			task := probeTask{Key: "ordinary", Planned: now.Add(-time.Second), Kind: "baseline"}
			if kind == "open_check" {
				ordinary.Origin, task.Kind = "live", "open_check"
			} else if kind == "late_followup" {
				ordinary.Origin, task.Kind, task.Delay = "live", "followup", Ptr(uint32(2000))
				task.Planned = now.Add(-2 * time.Second)
			}
			w := &watcher{Orders: map[string]*pendingOrder{"ordinary": ordinary, "fresh": {Origin: "live"}}, Tasks: []probeTask{task, {Key: "fresh", Planned: now.Add(300 * time.Millisecond), Kind: "followup", Delay: Ptr(uint32(2000))}}}
			if wait := nextProbeWake(w); wait < 150*time.Millisecond || wait > 300*time.Millisecond {
				t.Fatalf("due ordinary task caused a busy spin ahead of live deadline: %s", wait)
			}
			w.Tasks[1].Planned = now.Add(20 * time.Second)
			if wait := nextProbeWake(w); wait != 0 {
				t.Fatalf("ordinary work was needlessly withheld outside the live window: %s", wait)
			}
		})
	}
}

func priorityQuotaState(t *testing.T, q *SourceQuota, update func(*[4]int64)) [4]int64 {
	t.Helper()
	var snapshot [4]int64
	_, err := q.state(context.Background(), func(s *[4]int64) (time.Duration, error) {
		if update != nil {
			update(s)
		}
		snapshot = *s
		return 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestProbePriorityQuotaSuccessfulMembersDoNotRenewWindow(t *testing.T) {
	q, err := NewSourceQuota(t.TempDir(), "fixed-window", 50*time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(probeContext(context.Background()), 2*time.Second)
	defer cancel()
	before := time.Now()
	if err := q.Before(ctx, 1); err != nil {
		t.Fatal(err)
	}
	first := priorityQuotaState(t, q, nil)
	if first[2] < before.Add(450*time.Millisecond).UnixNano() || first[2] > time.Now().Add(500*time.Millisecond).UnixNano() {
		t.Fatalf("priority window is not bounded by 10 intervals: %+v", first)
	}
	if err := q.Before(ctx, 1); err != nil {
		t.Fatal(err)
	}
	second := priorityQuotaState(t, q, nil)
	if second[0] <= first[0] || second[2] != first[2] {
		t.Fatalf("a successful later RPC renewed the fixed priority window: first=%v second=%v", first, second)
	}
}

func TestProbePriorityQuotaExpiredWindowYieldsAfterAdmissionBecomesAvailable(t *testing.T) {
	for _, providerCooldown := range []bool{false, true} {
		name := "future_admission"
		if providerCooldown {
			name = "extended_provider_cooldown"
		}
		t.Run(name, func(t *testing.T) {
			q, err := NewSourceQuota(t.TempDir(), name, 100*time.Millisecond, 3)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			ready := now.Add(250 * time.Millisecond).UnixNano()
			priorityQuotaState(t, q, func(s *[4]int64) {
				s[0], s[2] = ready, now.Add(-time.Millisecond).UnixNano()
				if providerCooldown {
					s[0] = now.Add(50 * time.Millisecond).UnixNano()
					s[1] = ready
					// An earlier yield is already in progress, but a later provider
					// cooldown must still leave one usable background interval.
					s[2] = -now.Add(150 * time.Millisecond).UnixNano()
				}
			})
			urgent, cancel := context.WithTimeout(probeContext(context.Background()), 20*time.Millisecond)
			err = q.Before(urgent, 1)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("urgent request skipped the background opportunity: %v", err)
			}
			state := priorityQuotaState(t, q, nil)
			wantYield := ready + int64(q.Interval)
			if state[2] != -wantYield {
				t.Fatalf("yield ended before quota was usable: state=%v want=%d", state, -wantYield)
			}
			background, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := q.Before(background, 3); err != nil {
				t.Fatalf("background did not consume its admission: %v", err)
			}
			if at := time.Now().UnixNano(); at >= wantYield {
				t.Fatalf("negative yield blocked background until %d (deadline %d)", at, wantYield)
			}
			if after := priorityQuotaState(t, q, nil); after[2] != 0 || after[0] <= ready {
				t.Fatalf("background admission did not end yield and reserve members: %v", after)
			}
		})
	}
}
