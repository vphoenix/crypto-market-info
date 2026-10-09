package across

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestSourceQuotaBaseRequestLimitEscalatesAcrossSuccessAndRestart(t *testing.T) {
	dir := t.TempDir()
	q, err := NewSourceQuota(dir, "mainnet.base.org", time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	rate := &ethereum.RPCError{Code: -32011, Message: "request limit reached"}
	for level := int64(1); level <= 4; level++ {
		q.After(1, rate)
		state := priorityQuotaState(t, q, nil)
		if state[3] != level {
			t.Fatalf("lost escalation: %v", state)
		}
		if delay := time.Until(time.Unix(0, state[1])); delay < (time.Duration(1<<uint(level-1))*5*time.Second)-time.Second {
			t.Fatalf("missing backoff at level %d: %s", level, delay)
		}
		// Another method may succeed immediately after the previous cooldown.
		priorityQuotaState(t, q, func(s *[4]int64) { s[1] = time.Now().Add(-time.Second).UnixNano() })
		q.After(1, nil)
		if priorityQuotaState(t, q, nil)[3] != level {
			t.Fatal("cheap success reset provider escalation")
		}
		q, err = NewSourceQuota(dir, "mainnet.base.org", time.Millisecond, 3)
		if err != nil {
			t.Fatal(err)
		}
	}
	priorityQuotaState(t, q, func(s *[4]int64) { s[1] = time.Now().Add(-61 * time.Second).UnixNano() })
	q.After(1, nil)
	if priorityQuotaState(t, q, nil)[3] != 0 {
		t.Fatal("healthy source never reset backoff")
	}
}

func TestSourceQuotaImpossibleBudgetDoesNotWaitOrConsumeAdmission(t *testing.T) {
	q, _ := NewSourceQuota(t.TempDir(), "source", time.Millisecond, 3)
	q.After(1, &ethereum.RPCError{Code: -32011, Message: "request limit reached"})
	before := priorityQuotaState(t, q, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := q.Before(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("waited for an unusable provider cooldown")
	}
	if after := priorityQuotaState(t, q, nil); after != before {
		t.Fatal("cancelled admission changed shared quota")
	}
	if delay, err := q.Cooldown(ctx); err != nil || delay <= 0 {
		t.Fatal("cooldown was lost", delay, err)
	}
}

func TestWatchSourceCooldownAllowsPeerAndPreservesMissingCoverage(t *testing.T) {
	m := runnerManifest(t)
	base, _ := m.Chain(8453)
	arb, _ := m.Chain(42161)
	r := protocolChainReader(t, base, func(method string, _ json.RawMessage) (any, bool) {
		t.Fatalf("cooled source was queried: %s", method)
		return nil, false
	})
	q, _ := NewSourceQuota(t.TempDir(), "base", time.Millisecond, 3)
	q.After(1, &ethereum.RPCError{Code: -32011, Message: "request limit reached"})
	r.SourceQuota = q
	store := &runnerProtocolStore{}
	c := &Collector{Manifest: m, Store: store, Archive: r.RPC.Archive, Readers: map[uint64]*Reader{
		8453: r, 42161: protocolChainReader(t, arb, nil),
	}}
	if err := c.Watch(context.Background(), true, nil); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) == 0 {
		t.Fatal("peer chain did not commit")
	}
	for _, b := range store.batches {
		if b.Capture.ChainId != 42161 || b.Capture.CompletedTasks != 1 {
			t.Fatal("fabricated cooled-source coverage")
		}
	}
	if c.worker().Readers[8453].SourceQuota != q {
		t.Fatal("worker lost shared cooldown")
	}
}

func TestExpiredDiscoveryBudgetPersistsFailedCaptureWithoutAdvancing(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	r := protocolChainReader(t, chain, nil)
	store := &priorityProbeStore{writes: make(chan Batch, 8)}
	c := &Collector{Manifest: m, Store: store, Archive: r.RPC.Archive, Readers: map[uint64]*Reader{8453: r}}
	network, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.collectSplit(context.Background(), 8453, 100, 110, "live", "head", network)
	if err == nil {
		t.Fatal("expired network budget accepted")
	}
	if len(store.batches) != 1 {
		t.Fatal("missing durable failure", len(store.batches))
	}
	b := store.batches[0]
	if b.Capture.Status != "error" || !b.Capture.Committed || b.Capture.CompletedTasks != 0 {
		t.Fatal("false successful coverage")
	}
	if next := firstUncovered(100, 110, logIntervals(c.captures, 8453)); next != 100 {
		t.Fatal("failed range advanced", next)
	}
}

func TestWatchOnceSourcePacingDoesNotInventDeadline(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	m.Chains = []ChainConfig{chain}
	r := protocolChainReader(t, chain, nil)
	q, _ := NewSourceQuota(t.TempDir(), "source", time.Millisecond, 3)
	r.SourceQuota = q
	r.RPC.BeforeRequest, r.RPC.AfterRequest = q.Before, q.After
	store := &runnerProtocolStore{}
	c := &Collector{Manifest: m, Store: store, Archive: r.RPC.Archive, Readers: map[uint64]*Reader{8453: r}}
	if err := q.Before(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Watch(context.Background(), true, nil); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 1 || store.batches[0].Capture.CompletedTasks != 1 {
		t.Fatal("one-shot pacing lost coverage")
	}
}

func TestSourceQuotaDiscoveryYieldsWhenCooldownStartsMidCapture(t *testing.T) {
	q, _ := NewSourceQuota(t.TempDir(), "source", time.Millisecond, 3)
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), discoveryQuotaKey{}, true), time.Minute)
	defer cancel()
	if err := q.Before(ctx, 1); err != nil {
		t.Fatal(err)
	}
	q.After(1, &ethereum.RPCError{Code: -32011, Message: "request limit reached"})
	before := priorityQuotaState(t, q, nil)
	start := time.Now()
	if err := q.Before(ctx, 1); err == nil || err.Error() != "rpc_source_cooldown" {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("discovery waited through cooldown")
	}
	if after := priorityQuotaState(t, q, nil); after != before {
		t.Fatal("discovery changed provider cooldown")
	}
}

func TestWatchRawGapBudgetShrinksWhileRateAndArchiveRejectionsDoNot(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want uint64
	}{
		{"operation_budget", errors.New("rpc_operation_budget_exhausted"), 968},
		{"deadline", context.DeadlineExceeded, 968},
		{"rate_limit", &ethereum.RPCError{Code: -32011, Message: "request limit reached"}, 936},
		{"archive_denied", &ethereum.HTTPError{Status: 403}, 936},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := runnerManifest(t)
			chain, _ := m.Chain(8453)
			m.Chains = []ChainConfig{chain}
			var from uint64
			r := protocolChainReader(t, chain, func(method string, p json.RawMessage) (any, bool) {
				if method != "eth_getLogs" {
					return nil, false
				}
				var args []map[string]string
				if err := json.Unmarshal(p, &args); err != nil {
					t.Fatal(err)
				}
				from, _ = q64(args[0]["fromBlock"])
				return []any{}, true
			})
			r.RPC.BeforeRequest = func(context.Context, int) error { return tc.err }
			store := &coreStore{caps: []Capture{rawRecoveryCapture(8453, 100, 109), rawRecoveryCapture(8453, 1000, 1000)}}
			c := Collector{Manifest: m, Store: store, Readers: map[uint64]*Reader{8453: r}, Archive: r.RPC.Archive}
			if err := c.RepairRawGapStep(context.Background()); err == nil {
				t.Fatal("failed source appeared successful")
			}
			if next := firstUncovered(100, 1000, historyIntervals(c.captures, 8453, false)); next != 110 {
				t.Fatal("failure covered hole", next)
			}
			r.RPC.BeforeRequest = nil
			if err := c.RepairRawGapStep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if from != tc.want {
				t.Fatalf("retry from=%d want=%d", from, tc.want)
			}
		})
	}
}
