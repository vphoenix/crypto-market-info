package across

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// Return a real JSON-RPC rate error while keeping the reader's identity/header
// fixtures intact. This exercises protocol classification and failure captures.
func historyLimitLogs(r *Reader, calls *int) {
	underlying := r.RPC.HTTP.Transport
	r.RPC.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(raw))
		var members []struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if err = json.Unmarshal(raw, &members); err != nil {
			return nil, err
		}
		for _, member := range members {
			if member.Method == "eth_getLogs" {
				*calls++
				body, _ := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": member.ID, "error": map[string]any{"code": -32016, "message": "over rate limit"}}})
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
			}
		}
		return underlying.RoundTrip(req)
	})
}

func TestAcrossHistoryRateLimitedChainDoesNotBlockPeerOrSplit(t *testing.T) {
	m := runnerManifest(t)
	base, _ := m.Chain(8453)
	arb, _ := m.Chain(42161)
	baseReader := protocolChainReader(t, base, nil)
	arbReader := protocolChainReader(t, arb, nil)
	requests := 0
	historyLimitLogs(baseReader, &requests)
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: baseReader, 42161: arbReader}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peerAdvanced := false
	err := c.History(ctx, []HistoryRange{{8453, 1, 100}, {42161, 1, 100}}, func(s string) {
		if strings.Contains(s, "history chain=42161 blocks=1..64") {
			peerAdvanced = true
			cancel()
		}
	})
	if !peerAdvanced || !errors.Is(err, context.Canceled) {
		t.Fatalf("peer was blocked: advanced=%t err=%v", peerAdvanced, err)
	}
	if requests != 1 {
		t.Fatalf("rate error recursively split or immediately retried: %d", requests)
	}
	for _, cap := range store.caps {
		if cap.ChainId == 8453 && cap.CompletedTasks != 0 {
			t.Fatal("rate error became scanned coverage")
		}
	}
}

func TestAcrossHistoryRepairsRawPartialWithoutMutatingOldCapture(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	unknown := true
	r := protocolChainReader(t, chain, func(method string, _ json.RawMessage) (any, bool) {
		if method == "eth_getStorageAt" && unknown {
			return Hex(WordAddress(strings.Repeat("j", 20))), true
		}
		return nil, false
	})
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	old, err := c.CollectRange(context.Background(), 8453, 1, 1, "backfill", "finalized")
	if err != nil || old.Capture.Status != "partial" {
		t.Fatalf("missing partial: %v %+v", err, old.Capture)
	}
	oldEncoding := ID(old.Capture)
	unknown = false
	if err = c.History(context.Background(), []HistoryRange{{8453, 1, 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if len(store.caps) != 2 {
		t.Fatalf("raw window was skipped instead of repaired: %d", len(store.caps))
	}
	if ID(store.batches[old.Capture.CaptureId].Capture) != oldEncoding {
		t.Fatal("repair rewrote old evidence")
	}
	newCap := store.caps[1]
	if newCap.Status != "complete" || newCap.CaptureId == old.Capture.CaptureId || newCap.CaptureMode != "backfill" {
		t.Fatalf("bad repair: %+v", newCap)
	}
}

func TestAcrossHistoryChecksFinalizedCheckpointEvenWhenRawWindowCovered(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	conflict := false
	r := protocolChainReader(t, chain, func(method string, params json.RawMessage) (any, bool) {
		if method != "eth_getBlockByNumber" || !conflict {
			return nil, false
		}
		var args []json.RawMessage
		json.Unmarshal(params, &args)
		var tag string
		json.Unmarshal(args[0], &tag)
		if tag == height(1) {
			return protocolHeader(1, strings.Repeat("x", 32), Now().Truncate(time.Second)), true
		}
		return nil, false
	})
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	if _, err := c.CollectRange(context.Background(), 8453, 1, 1, "backfill", "finalized"); err != nil {
		t.Fatal(err)
	}
	conflict = true
	err := c.History(context.Background(), []HistoryRange{{8453, 1, 1}}, nil)
	if err == nil || !strings.Contains(err.Error(), "finalized_hash_conflict") {
		t.Fatalf("covered raw window bypassed checkpoint: %v", err)
	}
	if len(store.caps) != 1 {
		t.Fatal("conflicting checkpoint produced new chain facts")
	}
}

func TestAcrossPartialRepairPermanentUnknownIsBoundedAndUsesSmallRanges(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	r := protocolChainReader(t, chain, func(method string, _ json.RawMessage) (any, bool) {
		if method == "eth_getStorageAt" {
			return Hex(WordAddress(strings.Repeat("j", 20))), true
		}
		return nil, false
	})
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	if _, err := c.CollectRange(context.Background(), 8453, 1, 129, "backfill", "head"); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := c.RepairPartialStep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.caps) != 4 {
		t.Fatalf("permanent ABI unknown must have exactly one attempt per 64-block chunk: %d", len(store.caps))
	}
	for _, cap := range store.caps[1:] {
		if cap.Status != "partial" || *cap.ToBlock-*cap.FromBlock+1 > 64 {
			t.Fatalf("bad repair bounds/status: %+v", cap)
		}
	}
}

func TestAcrossPartialRepairUserAbandonedDoesNotRequestRPC(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(42161)
	requests := 0
	r := protocolChainReader(t, chain, func(_ string, _ json.RawMessage) (any, bool) {
		requests++
		return nil, false
	})
	cap := Capture{CaptureId: ID("abandoned"), ChainId: 42161, CaptureKind: "logs", Status: "partial", Reason: "implementation_unknown: rpc_remote_error(code=-32000); " + repairAbandonedMarker, CompletedTasks: 1, Canonical: true, Committed: true, FromBlock: Ptr(uint64(1)), ToBlock: Ptr(uint64(64)), Revision: 2}
	for range 2 { // Fresh collector state models a process restart.
		c := Collector{Manifest: m, Readers: map[uint64]*Reader{42161: r}, captures: []Capture{cap}, loaded: true}
		if err := c.RepairPartialStep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if requests != 0 || ID(c.captures[0]) != ID(cap) {
			t.Fatal("abandoned gap requested RPC or changed its incomplete capture")
		}
	}
	// An unrelated new partial still follows the existing repair path.
	store := &coreStore{}
	other := cap
	other.CaptureId = ID("new-partial")
	other.FromBlock, other.ToBlock = Ptr(uint64(100)), Ptr(uint64(100))
	other.Reason = "implementation_unknown: rpc_transport_timeout"
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{42161: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}, captures: []Capture{cap, other}, loaded: true}
	if err := c.RepairPartialStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests == 0 || len(store.caps) != 1 || *store.caps[0].FromBlock != 100 {
		t.Fatal("abandoning old gaps disabled repair of unrelated new data")
	}
}

func historyReceiptFixture(t *testing.T, m Manifest, label, kind, tx string, complete bool) Batch {
	t.Helper()
	b, a := coreBatch(t)
	b.Capture.ManifestHash = m.Hash
	b.Capture.CaptureId = ID(label)
	b.Capture.CaptureKind = kind
	b.Deposits = nil
	h := *b.Capture.FromHash
	at := b.Capture.StartedAt
	ch, _ := m.Chain(8453)
	if kind == "logs" {
		b.Fills = []Fill{{CaptureId: b.Capture.CaptureId, ChainId: 8453, SpokePool: ch.SpokePool, BlockNumber: 10, BlockHash: h, BlockTime: at, TxHash: tx, DepositId: big.NewInt(1), InputToken: h, OutputToken: WordAddress(ch.USDC), InputAmountRaw: big.NewInt(100), OutputAmountRaw: big.NewInt(99), Depositor: h, Recipient: h, ExclusiveRelayer: h, MessageHash: h, RepaymentAddress: h, UpdatedRecipient: h, UpdatedMessageHash: h, UpdatedOutputAmountRaw: big.NewInt(99), AvailableAt: at, PayloadHash: h}}
	} else {
		row := TxReceipt{CaptureId: b.Capture.CaptureId, ChainId: 8453, BlockNumber: 10, BlockHash: h, BlockTime: at, TxHash: tx, Sender: strings.Repeat("s", 20), CalldataHash: h, TxValueWei: big.NewInt(0), EffectiveGasPriceWei: big.NewInt(1), ExecutionFeeWei: big.NewInt(1), FeeComplete: complete, RequestedAt: at, AvailableAt: at, TransactionPayloadHash: h, ReceiptPayloadHash: h}
		if complete {
			row.TotalFeeWei = big.NewInt(1)
		}
		b.Receipts = []TxReceipt{row}
	}
	if err := ArchiveBatch(a, &b, []EvidenceHeader{{ChainID: 8453, Number: 10, Hash: Hex(h), Time: at}}, nil); err != nil {
		t.Fatal(err)
	}
	return b
}

type historyBulkStore struct {
	coreStore
	bulkCalls int
	selected  []Capture
}

func (s *historyBulkStore) AcrossBatch(context.Context, Capture) (Batch, error) {
	return Batch{}, errors.New("unexpected_N_plus_1_read")
}
func (s *historyBulkStore) AcrossBatches(ctx context.Context, caps []Capture) (BatchLoad, error) {
	s.bulkCalls++
	s.selected = append([]Capture(nil), caps...)
	out := BatchLoad{Batches: map[string]Batch{}, Errors: map[string]error{}}
	for _, cap := range caps {
		b, err := s.coreStore.AcrossBatch(ctx, cap)
		if err != nil {
			out.Errors[cap.CaptureId] = err
		} else {
			out.Batches[cap.CaptureId] = b
		}
	}
	return out, nil
}

func TestAcrossRepairReceiptsLatestCanonicalBulkAndUnknownOncePerRun(t *testing.T) {
	m := runnerManifest(t)
	store := &historyBulkStore{}
	txA, txB, txC := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)
	for _, b := range []Batch{historyReceiptFixture(t, m, "fillA", "logs", txA, false), historyReceiptFixture(t, m, "fillA-duplicate", "logs", txA, false), historyReceiptFixture(t, m, "unknownA", "receipts", txA, false), historyReceiptFixture(t, m, "fillB", "logs", txB, false), historyReceiptFixture(t, m, "unknownB", "receipts", txB, false)} {
		if err := store.WriteAcrossBatch(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	orphan := historyReceiptFixture(t, m, "orphan", "logs", txC, false)
	store.WriteAcrossBatch(context.Background(), orphan)
	revision := orphan.Capture
	revision.Revision++
	revision.Canonical = false
	revision.Finality = "orphaned"
	store.caps = append(store.caps, revision)
	c := Collector{Manifest: m, Store: store}
	if err := c.PrepareRepair(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.bulkCalls != 1 || len(c.receiptQueue) != 2 {
		t.Fatalf("startup not bulk/deduplicated: calls=%d tasks=%d", store.bulkCalls, len(c.receiptQueue))
	}
	for _, cap := range store.selected {
		if cap.CaptureId == orphan.Capture.CaptureId {
			t.Fatal("older canonical revision was reintroduced")
		}
	}
	keyA := ID(receiptTask{8453, *orphan.Capture.FromHash, txA})
	delete(c.receiptQueue, keyA) // Successful fetch still had unknown fees.
	if err := c.RefreshRepairReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.receiptQueue) != 1 {
		t.Fatal("successful unknown receipt was requeued or pending startup work lost")
	}
	store.WriteAcrossBatch(context.Background(), historyReceiptFixture(t, m, "completeB", "receipts", txB, true))
	store.WriteAcrossBatch(context.Background(), historyReceiptFixture(t, m, "newC", "logs", txC, false))
	if err := c.RefreshRepairReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	keyC := ID(receiptTask{8453, *orphan.Capture.FromHash, txC})
	if len(c.receiptQueue) != 1 || c.receiptQueue[keyC].TxHash != txC {
		t.Fatal("complete receipt did not win or newly missing receipt was lost")
	}
}

func TestAcrossPartialRepairBudgetFailurePersistsWithParentContext(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	unknown := true
	r := protocolChainReader(t, chain, func(method string, _ json.RawMessage) (any, bool) {
		if method == "eth_getStorageAt" && unknown {
			return Hex(WordAddress(strings.Repeat("j", 20))), true
		}
		return nil, false
	})
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	if _, err := c.CollectRange(context.Background(), 8453, 1, 1, "backfill", "head"); err != nil {
		t.Fatal(err)
	}
	budget, cancel := context.WithCancel(context.Background())
	cancel()
	unknown = false
	if err := c.RepairPartialStep(context.Background(), budget); err != nil {
		t.Fatal("RPC budget became persistence failure", err)
	}
	if len(store.caps) != 2 || store.caps[1].Status != "error" || !store.caps[1].Committed {
		t.Fatal("failed bounded attempt evidence lost")
	}
}

func TestAcrossRepairReceiptFailuresRotateAndBackOffInsteadOfStarving(t *testing.T) {
	m := runnerManifest(t)
	store := &coreStore{}
	c := Collector{Manifest: m, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}, Readers: map[uint64]*Reader{}, receiptQueue: map[string]receiptTask{}}
	requests := map[string]int{}
	for _, chain := range m.Chains {
		c.Readers[chain.ChainID] = mockReader(t, chain.ChainID, func(method string, params json.RawMessage) any {
			if method != "eth_getTransactionByHash" && method != "eth_getTransactionReceipt" {
				t.Fatal("unexpected receipt RPC", method)
			}
			var args []string
			if err := json.Unmarshal(params, &args); err != nil {
				t.Fatal(err)
			}
			requests[args[0]]++
			return nil // Provider cannot obtain this transaction; task stays pending.
		})
	}
	for i, chain := range []uint64{8453, 8453, 42161} {
		task := receiptTask{chain, strings.Repeat("h", 32), strings.Repeat(string(rune('a'+i)), 32)}
		c.receiptQueue[ID(task)] = task
	}
	for range 4 {
		if err := c.RepairReceiptStep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 3 || len(store.caps) != 3 || len(c.receiptQueue) != 3 {
		t.Fatalf("a failure starved other receipts or retried without backoff: requests=%v captures=%d queue=%d", requests, len(store.caps), len(c.receiptQueue))
	}
	for _, count := range requests {
		if count != 2 {
			t.Fatal("failed receipt retried immediately", requests)
		}
	}
	for key := range c.receiptQueue {
		c.partialRepair.receiptAttemptAt[key] = time.Now().Add(-2 * time.Minute)
		break
	}
	if err := c.RepairReceiptStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.caps) != 4 {
		t.Fatal("eligible receipt was never retried")
	}
}

func TestAcrossPartialRepairRetainsTemporaryEndImplementationFailure(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	endHash := strings.Repeat("e", 32)
	r := protocolChainReader(t, chain, func(method string, params json.RawMessage) (any, bool) {
		if method == "eth_getBlockByNumber" {
			var args []json.RawMessage
			if err := json.Unmarshal(params, &args); err != nil {
				t.Fatal(err)
			}
			var tag string
			if err := json.Unmarshal(args[0], &tag); err != nil {
				t.Fatal(err)
			}
			if tag == "0x2" {
				return protocolHeader(2, endHash, Now()), true
			}
		}
		return nil, false
	})
	temporary := true
	underlying := r.RPC.HTTP.Transport
	r.RPC.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(raw))
		var members []struct {
			ID     int             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err = json.Unmarshal(raw, &members); err != nil {
			return nil, err
		}
		for _, member := range members {
			if temporary && member.Method == "eth_getStorageAt" && strings.Contains(string(member.Params), Hex(endHash)) {
				body, _ := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": member.ID, "error": map[string]any{"code": -32016, "message": "over rate limit"}}})
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
			}
		}
		return underlying.RoundTrip(req)
	})
	store := &coreStore{}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	old, err := c.CollectRange(context.Background(), 8453, 1, 2, "backfill", "head")
	if err != nil || !partialRetryable(old.Capture) {
		t.Fatalf("end RPC failure lost classification: err=%v reason=%s", err, old.Capture.Reason)
	}
	if err = c.RepairPartialStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = c.RepairPartialStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.caps) != 2 {
		t.Fatal("temporary partial ignored cooldown or was marked permanent")
	}
	temporary = false
	for key := range c.partialRepair.retryAt {
		c.partialRepair.retryAt[key] = time.Now().Add(-time.Second)
	}
	if err = c.RepairPartialStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.caps) != 3 || store.caps[2].Status != "complete" {
		t.Fatal("temporary end failure was never repaired after recovery")
	}
}
