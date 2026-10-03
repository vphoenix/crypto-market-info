package lst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestRPCHistoricalHeaderWithoutLondonBaseFee(t *testing.T) {
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		var call struct {
			Id uint64 `json:"id"`
		}
		_ = json.NewDecoder(req.Body).Decode(&call)
		return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x1","hash":"0x%s","parentHash":"0x%s","timestamp":"0x55ba4224"}}`, call.Id, strings.Repeat("11", 32), strings.Repeat("22", 32))), nil
	})
	r := RPC{Transport: tr, URL: "https://rpc.example"}
	b, e := r.Header(context.Background(), "0x1")
	if e != nil || b.BaseFee != nil || b.Number != 1 {
		t.Fatal(b, e)
	}
	if len(b.Response.PayloadHash) != 32 {
		t.Fatal("RPC model evidence must be raw32")
	}
}
func TestProtocolFailureKeepsKnownAnchorAndEvidence(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		n++
		if n == 2 {
			return nil, io.ErrUnexpectedEOF
		}
		var call struct {
			Id uint64 `json:"id"`
		}
		_ = json.NewDecoder(req.Body).Decode(&call)
		return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%024s%s"}`, call.Id, "", strings.TrimPrefix(m.Addresses["steth_impl"], "0x"))), nil
	})
	r := RPC{Transport: tr, URL: "https://rpc.example"}
	h := Block{Number: 100, Hash: strings.Repeat("h", 32), Time: Now()}
	p, responses, e := r.Protocol(context.Background(), m, h)
	if e == nil || p.BlockHash == nil || *p.BlockHash != h.Hash || p.BlockNumber == nil || *p.BlockNumber != 100 || len(p.PayloadHashes) != 1 || len(responses) != 2 || p.RequestedAt == nil || p.StateStatus == "ok" {
		t.Fatalf("partial protocol lost source evidence: %v", e)
	}
}
func TestDecodeLidoSingleFinalizationAndRemovedLog(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	at := time.Unix(1790000000, 0).UTC()
	h := strings.Repeat("h", 32)
	b := Block{Number: 100, Hash: h, Time: at}
	event := chainLog{Address: m.Addresses["queue"], Topics: []string{finalizedTopic, fmt.Sprintf("0x%064x", 1), fmt.Sprintf("0x%064x", 1)}, Data: fmt.Sprintf("0x%064x%064x%064x", 100, 90, at.Unix()), BlockNumber: "0x64", BlockHash: Hex(h), TransactionHash: Hex(strings.Repeat("t", 32)), TransactionIndex: "0x0", LogIndex: "0x1"}
	batch := Batch{Capture: Capture{CaptureId: uuid.New()}}
	e = decodeEvent(event, b, m, &batch, Response{PayloadHash: strings.Repeat("p", 32), AvailableAt: at})
	if e != nil || len(batch.Finalizations) != 1 || batch.Finalizations[0].FromRequestId.Int64() != 1 || batch.Finalizations[0].ToRequestId.Int64() != 1 {
		t.Fatal(batch, e)
	}
	event.Removed = true
	if decodeEvent(event, b, m, &batch, Response{}) == nil {
		t.Fatal("removed event accepted")
	}
	event.Removed = false
	event.Topics[1] = fmt.Sprintf("0x%064x", 0)
	if decodeEvent(event, b, m, &batch, Response{}) == nil {
		t.Fatal("zero source firstId accepted")
	}
}
func TestMarketHeadFailurePersistsEightUnknownMembers(t *testing.T) {
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { return nil, io.ErrUnexpectedEOF })
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: m, Archive: ethereum.Archive{Dir: t.TempDir()}}
	c.Metadata.Instrument.ID = 1
	b, e := c.Market(context.Background(), nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(b.Quotes) != 8 || b.Capture.Status != "partial" || b.Capture.Canonical || b.Capture.FromBlockHash != nil {
		t.Fatal("failed head invented current data")
	}
	if e = Validate(b); e != nil {
		t.Fatal(e)
	}
	for _, q := range b.Quotes {
		if q.BuyStatus == "ok" || q.ChainPayloadHashes != nil || q.NominalRedeemEthWei != nil {
			t.Fatal("old quote reused")
		}
	}
}
func TestFrozenPendingGobPreservesBytesAndRetryIdentity(t *testing.T) {
	b := lstBatchFixture()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(t.TempDir(), "pending.gob")
	if e := writeGob(file, b); e != nil {
		t.Fatal(e)
	}
	var recovered Batch
	if e := readGob(file, &recovered); e != nil {
		t.Fatal(e)
	}
	if CanonicalHash(recovered) != CanonicalHash(b) {
		t.Fatal("binary bytes/NULL/time changed across restart")
	}
	if e := Validate(recovered); e != nil {
		t.Fatal(e)
	}
}
func TestOriginalWithdrawalAmountsAndFreshness(t *testing.T) {
	if n, e := requestParts(big.NewInt(101), big.NewInt(2), big.NewInt(100)); e != nil || n != 2 {
		t.Fatal(n, e)
	}
	if _, e := requestParts(big.NewInt(1), big.NewInt(2), big.NewInt(100)); e == nil {
		t.Fatal("below minimum accepted")
	}
	now := Now()
	old := now.Add(-20 * time.Second)
	q := Quote{HedgeDepthEventTime: &old, HedgeDepthAvailableAt: &old, MarkSourceTime: &old, MarkAvailableAt: &old}
	if quoteFresh(q, ProtocolState{StateStatus: "ok"}, Block{Time: old}, now.Add(-25*time.Second), now) {
		t.Fatal("old at comparison accepted because fresh at own receipt")
	}
}

type runnerMemoryStore struct {
	batches map[uuid.UUID]Batch
	fail    bool
}

func (s *runnerMemoryStore) WriteLST(_ context.Context, b Batch) error {
	if s.fail {
		return errors.New("db_temporarily_unavailable")
	}
	if s.batches == nil {
		s.batches = map[uuid.UUID]Batch{}
	}
	s.batches[b.Capture.CaptureId] = b
	return nil
}
func (s *runnerMemoryStore) WriteLSTRevision(_ context.Context, c Capture) error {
	b := s.batches[c.CaptureId]
	b.Capture = c
	s.batches[c.CaptureId] = b
	return nil
}
func (s *runnerMemoryStore) LSTCaptures(_ context.Context, _ string) ([]Capture, error) {
	v := []Capture{}
	for _, b := range s.batches {
		v = append(v, b.Capture)
	}
	return v, nil
}
func (s *runnerMemoryStore) LSTBatch(_ context.Context, c Capture) (Batch, error) {
	return s.batches[c.CaptureId], nil
}
func TestCommitRetriesFrozenBatchWithoutRefetch(t *testing.T) {
	b := lstBatchFixture()
	_ = b.Seal()
	store := &runnerMemoryStore{fail: true}
	c := Collector{Store: store, StateDir: t.TempDir()}
	if c.Commit(context.Background(), b) == nil {
		t.Fatal("expected database failure")
	}
	store.fail = false
	if e := c.FlushPending(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(store.batches) != 1 || CanonicalHash(store.batches[b.Capture.CaptureId]) != CanonicalHash(b) {
		t.Fatal("retry changed capture")
	}
}

func TestBudgetSchedulingAvoidsStarvationAndKeepsDailySeed(t *testing.T) {
	start := time.Date(2026, 10, 2, 11, 0, 0, 0, time.UTC)
	seen := map[int]bool{}
	for i := 0; i < 4; i++ {
		order := entryBudgetOrder(start.Add(time.Duration(i) * time.Minute))
		seen[order[0]] = true
		each := map[int]bool{}
		for _, j := range order {
			each[j] = true
		}
		if len(each) != 4 {
			t.Fatal("missing budget member")
		}
	}
	if len(seen) != 4 {
		t.Fatal("one budget never receives a leading slot")
	}
	for i := 0; i < 5; i++ {
		at := time.Date(2026, 10, 2, 12, i, 0, 0, time.UTC)
		if entryBudgetOrder(at)[0] != 2 {
			t.Fatal("daily 100k seed not prioritized")
		}
	}
}
