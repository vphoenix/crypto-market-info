package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSharedQuotaAcrossReaderInstances(t *testing.T) {
	dir := t.TempDir()
	q1, err := NewSourceQuota(dir, "same-host", 15*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	q2, _ := NewSourceQuota(dir, "same-host", 15*time.Millisecond, 1)
	var mu sync.Mutex
	starts := []time.Time{}
	var wg sync.WaitGroup
	for _, q := range []*SourceQuota{q1, q2} {
		wg.Add(1)
		go func(q *SourceQuota) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				if e := q.Before(context.Background(), 1); e != nil {
					t.Error(e)
					return
				}
				mu.Lock()
				starts = append(starts, time.Now())
				mu.Unlock()
			}
		}(q)
	}
	wg.Wait()
	if len(starts) != 6 {
		t.Fatal(starts)
	}
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < 14*time.Millisecond {
			t.Fatal("shared quota burst", starts)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	if e := q1.Before(ctx, 1); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
}
func TestSharedQuotaRateLimitAndPriority(t *testing.T) {
	q, _ := NewSourceQuota(t.TempDir(), "host", 20*time.Millisecond, 3)
	q.After(1, &ethereum.RPCError{Code: -32016, Message: "over rate limit"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := q.Before(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	q2, _ := NewSourceQuota(t.TempDir(), "host", 20*time.Millisecond, 3)
	if err := q2.Before(probeContext(context.Background()), 1); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel2()
	if err := q2.Before(ctx2, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("maintenance bypassed probe priority", err)
	}
	if err := q2.Before(probeContext(context.Background()), 1); err != nil {
		t.Fatal(err)
	}
	// Background work regains admission after the short grant.
	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()
	if err := q2.Before(ctx3, 1); err != nil {
		t.Fatal("background was starved by probe grant", err)
	}
}
func TestQueuedProbeDoesNotRenewPriority(t *testing.T) {
	q, err := NewSourceQuota(t.TempDir(), "host", time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Before(probeContext(context.Background()), 3); err != nil {
		t.Fatal(err)
	}
	readGrant := func() int64 {
		var grant int64
		_, err := q.state(context.Background(), func(s *[4]int64) (time.Duration, error) {
			grant = s[2]
			return 0, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return grant
	}
	grant := readGrant()
	ctx, cancel := context.WithTimeout(probeContext(context.Background()), 5*time.Millisecond)
	defer cancel()
	if err = q.Before(ctx, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if got := readGrant(); got != grant {
		t.Fatalf("queued probe extended grant: %d -> %d", grant, got)
	}
}
func TestSingleMemberProbesCannotBorrowLargeBatchCredits(t *testing.T) {
	q, err := NewSourceQuota(t.TempDir(), "host", 40*time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Before(probeContext(context.Background()), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(probeContext(context.Background()), 5*time.Millisecond)
	defer cancel()
	if err = q.Before(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("small probe borrowed reserved batch capacity: %v", err)
	}
	background, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = q.Before(background, 3); err != nil {
		t.Fatalf("three-member background batch never admitted: %v", err)
	}
}
func TestReconcileCommitsBeforeLaterFailureAndBoundsWork(t *testing.T) {
	m := runnerManifest(t)
	ch, _ := m.Chain(8453)
	s := &runnerProtocolStore{}
	r := protocolChainReader(t, ch, func(method string, p json.RawMessage) (any, bool) {
		if method != "eth_getBlockByNumber" {
			return nil, false
		}
		var args []json.RawMessage
		json.Unmarshal(p, &args)
		var tag string
		json.Unmarshal(args[0], &tag)
		if tag == height(2) {
			return nil, true
		}
		return nil, false
	})
	c := Collector{Manifest: m, Store: s, Readers: map[uint64]*Reader{8453: r}, Archive: r.RPC.Archive, loaded: true, ReconcileLimit: 2}
	for n := uint64(1); n <= 3; n++ {
		c.captures = append(c.captures, Capture{CaptureId: ID(n), ChainId: 8453, Canonical: true, Committed: true, Finality: "head", Revision: 1, FromBlock: Ptr(n), ToBlock: Ptr(n), FromHash: Ptr(strings.Repeat("h", 32)), ToHash: Ptr(strings.Repeat("h", 32))})
	}
	if err := c.Reconcile(context.Background(), 8453); err == nil {
		t.Fatal("expected second header failure")
	}
	if len(s.revisions) != 1 || c.captures[0].Finality != "finalized" || c.captures[1].Finality != "head" {
		t.Fatalf("partial progress lost: %+v", s.revisions)
	}
	if !strings.Contains(s.revisions[0].Reason, "finality_proof=") {
		t.Fatal("missing finality proof")
	}
}
func TestCollectSplitDoesNotAmplifyTransportOrQuotaFailures(t *testing.T) {
	for _, err := range []error{errors.New("rpc_transport_timeout"), &ethereum.RPCError{Code: -32016, Message: "over rate limit"}, &ethereum.HTTPError{Status: 429}} {
		if splitLogError(err) {
			t.Fatal("transient failure recursively split", err)
		}
	}
	if !splitLogError(&ethereum.RPCError{Code: -32000, Message: "block range too large"}) {
		t.Fatal("bounded log error cannot split")
	}
}
func TestProbeDeadlineLoopIndependentOfMaintenance(t *testing.T) {
	m := runnerManifest(t)
	src, _ := m.Chain(8453)
	dst, _ := m.Chain(42161)
	s := &runnerProtocolStore{}
	c := Collector{Manifest: m, Store: s, Archive: ethereum.Archive{Dir: t.TempDir()}, Readers: map[uint64]*Reader{8453: protocolChainReader(t, src, nil), 42161: protocolChainReader(t, dst, nil)}}
	d := protocolDepositForRunner(m)
	w := &watcher{Orders: map[string]*pendingOrder{}, Cursors: map[uint64]uint64{}}
	w.add(d, "live", m)
	for i := range w.Tasks {
		w.Tasks[i].Planned = Now().Add(30 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	err := c.probeLoop(ctx, w, make(chan discoveredOrder))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(s.batches) == 0 {
		t.Fatal("timer did not dispatch")
	}
	p := s.batches[0].Probes[0]
	if p.RequestedAt.Sub(p.PlannedForAt) > 50*time.Millisecond {
		t.Fatalf("timer dispatch lag %s", p.RequestedAt.Sub(p.PlannedForAt))
	}
}
func protocolDepositForRunner(m Manifest) Deposit {
	src, _ := m.Chain(8453)
	dst, _ := m.Chain(42161)
	return Deposit{ChainId: 8453, SpokePool: src.SpokePool, BlockNumber: 100, BlockHash: strings.Repeat("h", 32), DestinationChainId: 42161, DepositId: mustQuantity("0x1"), RelayHash: strings.Repeat("r", 32), InputToken: WordAddress(src.USDC), OutputToken: WordAddress(dst.USDC), FillDeadline: uint32(Now().Unix() + 60), ExclusiveRelayer: strings.Repeat("\x00", 32), AvailableAt: Now()}
}
func mustQuantity(s string) *big.Int {
	n, e := Quantity(s)
	if e != nil {
		panic(fmt.Sprint(e))
	}
	return n
}
