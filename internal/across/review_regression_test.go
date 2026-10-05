package across

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestReviewProbeWorkerCanReachPriorityGateWithBackgroundSlotsBlocked(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	reader := protocolChainReader(t, chain, nil)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	reader.RPC.BeforeRequest = func(ctx context.Context, _ int) error {
		if urgent, _ := ctx.Value(probePriorityKey{}).(bool); urgent {
			return nil
		}
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = reader.Header(ctx, "latest") }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("background requests did not enter quota wait")
		}
	}
	collector := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: reader}}
	worker := collector.worker()
	probeCtx, probeCancel := context.WithTimeout(probeContext(ctx), 100*time.Millisecond)
	defer probeCancel()
	if _, err := worker.Readers[8453].Header(probeCtx, "latest"); err != nil {
		t.Fatalf("background quota waiters starved probe worker: %v", err)
	}
	if len(worker.Readers[8453].Members) != 1 || len(reader.Members) != 0 {
		t.Fatal("workers shared or contaminated evidence buffers")
	}
	unblock()
	wg.Wait()
	if len(reader.Members) != 2 || len(worker.Readers[8453].Members) != 1 {
		t.Fatal("worker evidence changed after background completion")
	}
}

type reviewRevisionStore struct {
	runnerProtocolStore
	cancelNetwork context.CancelFunc
}

func (s *reviewRevisionStore) WriteAcrossRevision(ctx context.Context, cap Capture) error {
	s.cancelNetwork()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runnerProtocolStore.WriteAcrossRevision(ctx, cap)
}

func TestReviewReconcilePersistenceOutlivesNetworkBudgetAndArchivesProof(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	reader := protocolChainReader(t, chain, nil)
	networkCtx, networkCancel := context.WithCancel(context.Background())
	defer networkCancel()
	store := &reviewRevisionStore{cancelNetwork: networkCancel}
	cap := Capture{CaptureId: ID("finality-review"), ChainId: 8453, Canonical: true, Committed: true, Finality: "head", Revision: 1, FromBlock: Ptr(uint64(1)), ToBlock: Ptr(uint64(1)), FromHash: Ptr(strings.Repeat("h", 32)), ToHash: Ptr(strings.Repeat("h", 32))}
	c := Collector{Manifest: m, Store: store, Readers: map[uint64]*Reader{8453: reader}, Archive: reader.RPC.Archive, loaded: true, captures: []Capture{cap}, ReconcileLimit: 1}
	if err := c.Reconcile(context.Background(), 8453, networkCtx); err != nil {
		t.Fatalf("network budget cancelled durable revision: %v", err)
	}
	if len(store.revisions) != 1 || store.revisions[0].Finality != "finalized" {
		t.Fatal("missing finality progress", store.revisions)
	}
	text := strings.TrimPrefix(store.revisions[0].Reason, "finality_proof=")
	binary, err := ParseHex(text, 32)
	if err != nil {
		t.Fatal(err)
	}
	var hash dex.Hash
	copy(hash[:], binary)
	raw, err := c.Archive.Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	var proof struct{ FinalizedHead, FromHeader, ToHeader string }
	if err = json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{proof.FinalizedHead, proof.FromHeader, proof.ToHeader} {
		binary, err = ParseHex(ref, 32)
		if err != nil {
			t.Fatal("missing finality RPC reference", err)
		}
		copy(hash[:], binary)
		if _, err = c.Archive.Get(hash); err != nil {
			t.Fatal("finality RPC evidence unavailable", err)
		}
	}
}

func reviewSplitReader(t *testing.T, chain ChainConfig) *Reader {
	r := protocolChainReader(t, chain, func(method string, params json.RawMessage) (any, bool) {
		switch method {
		case "eth_getBlockByNumber":
			var args []json.RawMessage
			_ = json.Unmarshal(params, &args)
			var tag string
			_ = json.Unmarshal(args[0], &tag)
			n := uint64(103)
			if strings.HasPrefix(tag, "0x") {
				n, _ = q64(tag)
			}
			return protocolHeader(n, strings.Repeat("h", 32), Now()), true
		case "eth_getLogs":
			var args []map[string]string
			_ = json.Unmarshal(params, &args)
			from, _ := q64(args[0]["fromBlock"])
			to, _ := q64(args[0]["toBlock"])
			if from != to {
				return []any{map[string]any{}}, true
			}
			if from == 100 {
				return []any{}, true
			}
			return nil, true
		}
		return nil, false
	})
	r.MaxLogs = 1
	return r
}

func TestReviewSplitPreservesNestedCommittedPrefixAfterFailure(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	reader := reviewSplitReader(t, chain)
	store := &runnerProtocolStore{}
	c := Collector{Manifest: m, Store: store, Readers: map[uint64]*Reader{8453: reader}, Archive: reader.RPC.Archive}
	batches, err := c.collectSplit(context.Background(), 8453, 100, 103, "live", "head")
	if err == nil || len(batches) != 1 || *batches[0].Capture.FromBlock != 100 || *batches[0].Capture.ToBlock != 100 || batches[0].Capture.CompletedTasks != 1 {
		t.Fatalf("committed nested prefix lost: batches=%d err=%v", len(batches), err)
	}
	if next := firstUncovered(100, 103, logIntervals(c.captures, 8453)); next != 101 {
		t.Fatal("bad prefix cursor", next)
	}
}

func TestReviewWatchOnceUsesSuccessfulPrefixEvenWhenLaterShardFails(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	m.Chains = []ChainConfig{chain}
	reader := reviewSplitReader(t, chain)
	b := batchLoadFixture("already-scanned")
	b.Capture.ManifestHash = m.Hash
	b.Capture.FromBlock = Ptr(uint64(99))
	b.Capture.ToBlock = Ptr(uint64(99))
	b.Capture.FromHash = Ptr(strings.Repeat("h", 32))
	b.Capture.ToHash = Ptr(strings.Repeat("h", 32))
	b.Capture.ExpectedTasks = 1
	b.Capture.CompletedTasks = 1
	Seal(&b)
	store := &runnerProtocolStore{batches: []Batch{b}}
	c := Collector{Manifest: m, Store: store, Readers: map[uint64]*Reader{8453: reader}, Archive: reader.RPC.Archive}
	if err := c.Watch(context.Background(), true, nil); err != nil {
		t.Fatalf("successful prefix counted as total failure: %v", err)
	}
	if next := firstUncovered(99, 103, logIntervals(c.captures, 8453)); next != 101 {
		t.Fatal("watch discarded successful prefix", next)
	}
}

type reviewWorkerFailureStore struct {
	reads   atomic.Int32
	failure error
}

func (s *reviewWorkerFailureStore) AcrossCaptures(context.Context, string) ([]Capture, error) {
	if s.reads.Add(1) > 1 {
		return nil, s.failure
	}
	return nil, nil
}
func (s *reviewWorkerFailureStore) AcrossBatch(context.Context, Capture) (Batch, error) {
	return Batch{}, errors.New("unexpected fixture batch load")
}
func (s *reviewWorkerFailureStore) WriteAcrossBatch(context.Context, Batch) error {
	return errors.New("unexpected fixture write")
}
func (s *reviewWorkerFailureStore) WriteAcrossRevision(context.Context, Capture) error {
	return errors.New("unexpected fixture revision")
}

func TestReviewWatchPreservesWorkerFailureDuringDiscoveryCancellation(t *testing.T) {
	m := runnerManifest(t)
	failure := errors.New("fixture_maintenance_database_failure")
	store := &reviewWorkerFailureStore{failure: failure}
	c := Collector{Manifest: m, Store: store, Readers: map[uint64]*Reader{}, Archive: ethereum.Archive{Dir: t.TempDir()}}
	for _, chain := range m.Chains {
		reader := protocolChainReader(t, chain, nil)
		reader.RPC.HTTP.Transport = protocolTransport(func(request *http.Request) (*http.Response, error) {
			<-request.Context().Done()
			return nil, request.Context().Err()
		})
		c.Readers[chain.ChainID] = reader
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Watch(ctx, false, nil); !errors.Is(err, failure) {
		t.Fatalf("worker failure hidden by normal cancellation: got %v, want %v", err, failure)
	}
}
