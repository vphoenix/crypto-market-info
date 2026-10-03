package lst

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func gasTestHash(n byte) string { return strings.Repeat(string(n), 32) }
func gasTestBatch(t *testing.T, day time.Time) Batch {
	t.Helper()
	at := Now().Add(-time.Minute)
	b := Batch{Capture: Capture{CaptureId: uuid.New(), ManifestHash: gasTestHash(9), CaptureKind: "logs", CaptureMode: "backfill", SourceId: "ethereum:1:lido:withdrawal-queue", StartedAt: at, AvailableAt: at, ChainId: Ptr(uint64(1)), FromBlock: Ptr(uint64(100)), ToBlock: Ptr(uint64(120)), FromBlockHash: Ptr(gasTestHash(100)), ToBlockHash: Ptr(gasTestHash(120)), FromBlockTime: Ptr(day.Add(-12 * time.Second)), ToBlockTime: Ptr(day.Add(24 * time.Hour)), Finality: "finalized", Canonical: true, Revision: 1, Status: "complete", EvidenceRootHash: gasTestHash(8)}}
	for i, n := range []byte{4, 1, 1, 2, 3} {
		b.Requests = append(b.Requests, WithdrawalRequest{CaptureId: b.Capture.CaptureId, ChainId: 1, QueueAddress: strings.Repeat("q", 20), BlockNumber: 110, BlockHash: gasTestHash(110), BlockTime: day.Add(time.Hour), TransactionHash: gasTestHash(n), TransactionIndex: uint32(i), LogIndex: uint32(i), AbiVersion: "test", RequestId: big.NewInt(int64(i + 1)), Sender: strings.Repeat("s", 20), InitialOwner: strings.Repeat("o", 20), AmountStethWei: big.NewInt(100), AmountSharesRaw: big.NewInt(100), EventPayloadHash: gasTestHash(6), AvailableAt: at, GasSampleClass: "missing"})
	}
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	return b
}

func TestGasSelectionTwoDistinctTransactionsPerKindPerCompleteDay(t *testing.T) {
	today := Now().Truncate(24 * time.Hour)
	day := today.Add(-24 * time.Hour)
	b := gasTestBatch(t, day)
	for i, n := range []byte{7, 5, 5, 6} {
		b.Claims = append(b.Claims, WithdrawalClaim{BlockTime: day.Add(2 * time.Hour), BlockHash: gasTestHash(110), TransactionHash: gasTestHash(n), LogIndex: uint32(i)})
	}
	samples, e := gasSamples([]Batch{b, b}, today.Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if len(samples) != 4 {
		t.Fatalf("wanted 4 distinct request/claim transactions, got %d", len(samples))
	}
	for i, n := range []byte{1, 2, 5, 6} {
		if samples[i].hash != gasTestHash(n) {
			t.Fatal("selection is not lexicographic top two", i)
		}
	}
	// An already successful receipt removes that transaction from the work list;
	// it does not promote the third-ranked transaction into the sample.
	b.Requests[1].ReceiptStatus = Ptr(uint8(1))
	b.Requests[1].GasUsed = Ptr(uint64(100))
	b.Requests[1].EffectiveGasPriceWei = big.NewInt(3)
	b.Requests[1].ReceiptPayloadHash = Ptr(gasTestHash(33))
	b.Requests[1].ReceiptAvailableAt = Ptr(Now())
	samples, e = gasSamples([]Batch{b}, today.Add(time.Hour))
	if e != nil || len(samples) != 3 || samples[0].hash != gasTestHash(2) {
		t.Fatal("successful receipt refetched or third tx promoted", len(samples), e)
	}
}

func TestGasSelectionWaitsForWholeDayAndRejectsHeightGap(t *testing.T) {
	today := Now().Truncate(24 * time.Hour)
	day := today.Add(-24 * time.Hour)
	b := gasTestBatch(t, day)
	b.Capture.ToBlockTime = Ptr(day.Add(23 * time.Hour))
	if s, e := gasSamples([]Batch{b}, today.Add(time.Hour)); e != nil || len(s) != 0 {
		t.Fatal("incomplete day sampled", len(s), e)
	}
	left := b
	left.Capture.ToBlock = Ptr(uint64(109))
	left.Capture.ToBlockHash = Ptr(gasTestHash(109))
	left.Capture.ToBlockTime = Ptr(day.Add(12 * time.Hour))
	right := b
	right.Capture.FromBlock = Ptr(uint64(111))
	right.Capture.FromBlockHash = Ptr(gasTestHash(111))
	right.Capture.FromBlockTime = Ptr(day.Add(12*time.Hour + 24*time.Second))
	right.Capture.ToBlockTime = Ptr(today)
	if s, e := gasSamples([]Batch{left, right}, today.Add(time.Hour)); e != nil || len(s) != 0 {
		t.Fatal("height gap treated as full coverage", len(s), e)
	}
	right.Capture.FromBlock = Ptr(uint64(110))
	right.Capture.FromBlockHash = Ptr(gasTestHash(110))
	right.Capture.FromBlockTime = Ptr(day.Add(12*time.Hour + 12*time.Second))
	if s, e := gasSamples([]Batch{left, right}, today.Add(time.Hour)); e != nil || len(s) != 2 {
		t.Fatal("adjacent complete ranges not merged", len(s), e)
	}
	current := gasTestBatch(t, today)
	if s, e := gasSamples([]Batch{current}, today.Add(time.Hour)); e != nil || len(s) != 0 {
		t.Fatal("current UTC day sampled", len(s), e)
	}
}

type gasMemoryStore struct {
	batches []Batch
	writes  int
}

func (s *gasMemoryStore) WriteLST(_ context.Context, b Batch) error {
	s.batches = append(s.batches, b)
	s.writes++
	return nil
}
func (s *gasMemoryStore) WriteLSTRevision(context.Context, Capture) error {
	return errors.New("unexpected_revision")
}
func (s *gasMemoryStore) LSTCaptures(context.Context, string) ([]Capture, error) {
	out := []Capture{}
	for _, b := range s.batches {
		out = append(out, b.Capture)
	}
	return out, nil
}
func (s *gasMemoryStore) LSTBatch(_ context.Context, c Capture) (Batch, error) {
	for _, b := range s.batches {
		if b.Capture.CaptureId == c.CaptureId {
			b.Capture = c
			return b, nil
		}
	}
	return Batch{}, errors.New("missing_capture")
}

func TestGasPassBoundedReceiptEvidenceNewCaptureAndNoRefetch(t *testing.T) {
	today := Now().Truncate(24 * time.Hour)
	b := gasTestBatch(t, today.Add(-24*time.Hour))
	store := &gasMemoryStore{batches: []Batch{b}}
	queue := b.Requests[0].QueueAddress
	calls := 0
	var methods []string
	tr, clock := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		raw, _ := io.ReadAll(req.Body)
		var call struct {
			Id     uint64
			Method string
			Params []string
		}
		if e := json.Unmarshal(raw, &call); e != nil {
			t.Fatal(e)
		}
		methods = append(methods, call.Method)
		hash := call.Params[0]
		var result any
		if call.Method == "eth_getTransactionReceipt" {
			result = map[string]any{"transactionHash": hash, "blockHash": Hex(gasTestHash(110)), "blockNumber": "0x6e", "from": Hex(strings.Repeat("s", 20)), "to": Hex(queue), "status": "0x1", "gasUsed": "0x186a0", "effectiveGasPrice": "0x77359400", "logs": []map[string]any{{"address": Hex(queue), "topics": []string{requestedTopic}}}}
		} else if call.Method == "eth_getTransactionByHash" {
			result = map[string]any{"hash": hash, "blockHash": Hex(gasTestHash(110)), "to": Hex(queue), "input": Hex(string(crypto.Keccak256([]byte("requestWithdrawals(uint256[],address)"))[:4]))}
		} else {
			t.Fatal("unexpected method", call.Method)
		}
		out, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
		return transportReply(200, string(out)), nil
	})
	// Keep transport's artificial timestamps before the real collector clock.
	clock.at = Now().Add(-time.Hour)
	c := &Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: Manifest{Hash: b.Capture.ManifestHash, Addresses: map[string]string{"queue": Hex(queue)}}, Store: store, Archive: ethereum.Archive{Dir: filepath.Join(t.TempDir(), "archive")}, StateDir: t.TempDir()}
	if e := c.GasPass(context.Background(), 1); e != nil {
		t.Fatal(e)
	}
	if calls != 2 || store.writes != 1 || len(store.batches) != 2 {
		t.Fatal("transaction budget/new capture failed", calls, store.writes)
	}
	enriched := store.batches[1]
	if enriched.Capture.CaptureId == b.Capture.CaptureId || enriched.Capture.CaptureMode != "restart" || len(enriched.Requests) != len(b.Requests) {
		t.Fatal("not a new full immutable capture")
	}
	for i, r := range enriched.Requests {
		if r.EventPayloadHash != b.Requests[i].EventPayloadHash || r.CaptureId != enriched.Capture.CaptureId {
			t.Fatal("original fact evidence changed")
		}
		if r.TransactionHash == gasTestHash(1) && r.GasUsed == nil {
			t.Fatal("same transaction event missing shared receipt")
		}
	}
	for _, r := range store.batches[0].Requests {
		if r.GasUsed != nil {
			t.Fatal("original capture mutated")
		}
	}
	if e := c.GasPass(context.Background(), 10); e != nil {
		t.Fatal(e)
	}
	if calls != 4 || store.writes != 2 {
		t.Fatal("already sampled tx refetched", calls, store.writes)
	}
	if e := c.GasPass(context.Background(), 10); e != nil {
		t.Fatal(e)
	}
	if calls != 4 || store.writes != 2 {
		t.Fatal("third ranked tx promoted/refetched", calls, store.writes)
	}
}

func TestGasPassMissingReceiptDoesNotCommitOrAdvanceCoverage(t *testing.T) {
	day := Now().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	b := gasTestBatch(t, day)
	store := &gasMemoryStore{batches: []Batch{b}}
	calls := 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		raw, _ := io.ReadAll(req.Body)
		var v struct{ Id uint64 }
		json.Unmarshal(raw, &v)
		out, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": v.Id, "result": nil})
		return transportReply(200, string(out)), nil
	})
	c := &Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: Manifest{Hash: b.Capture.ManifestHash}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}, StateDir: t.TempDir()}
	if e := c.GasPass(context.Background(), 1); e != nil {
		t.Fatal(e)
	}
	if calls != 1 || store.writes != 0 || len(store.batches) != 1 {
		t.Fatal("failed receipt fabricated a capture or retried")
	}
	b.Capture.ToBlockTime = Ptr(day.Add(23 * time.Hour))
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	store.batches = []Batch{b}
	if e := c.GasPass(context.Background(), 10); e != nil {
		t.Fatal(e)
	}
	if calls != 1 {
		t.Fatal("incomplete day reached RPC")
	}
}
