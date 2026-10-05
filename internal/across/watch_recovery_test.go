package across

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestWatchOutageDoesNotDelayHeadOrChangeOneShot(t *testing.T) {
	for _, tc := range []struct {
		cursor, head, limit uint64
		once                bool
		from, to            uint64
		scan                bool
	}{
		{0, 1000, 512, false, 1000, 1000, true},
		{100, 1000, 512, false, 1000, 1000, true},
		{100, 1000, 512, true, 101, 612, true},
		{100, 612, 512, false, 101, 612, true},
		{1000, 1000, 512, false, 0, 0, false},
		{1000, 1003, 512, false, 1001, 1003, true},
	} {
		from, to, scan := watchLogWindow(tc.cursor, tc.head, tc.limit, tc.once)
		if from != tc.from || to != tc.to || scan != tc.scan {
			t.Fatalf("%+v got %d..%d scan=%t", tc, from, to, scan)
		}
	}
}

func rawRecoveryCapture(chain, from, to uint64) Capture {
	return Capture{CaptureId: ID([3]uint64{chain, from, to}), ChainId: chain, CaptureKind: "logs", Canonical: true, Committed: true, Status: "complete", CompletedTasks: 1, FromBlock: Ptr(from), ToBlock: Ptr(to), Revision: 1}
}

func TestWatchRawGapRecoveryKeepsOriginalStartAndResumes(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	requested := [][2]uint64{}
	r := protocolChainReader(t, chain, func(method string, p json.RawMessage) (any, bool) {
		if method != "eth_getLogs" {
			return nil, false
		}
		var args []map[string]string
		if err := json.Unmarshal(p, &args); err != nil {
			t.Fatal(err)
		}
		from, _ := q64(args[0]["fromBlock"])
		to, _ := q64(args[0]["toBlock"])
		requested = append(requested, [2]uint64{from, to})
		return []any{}, true
	})
	store := &coreStore{caps: []Capture{rawRecoveryCapture(8453, 100, 109), rawRecoveryCapture(8453, 1000, 1000)}}
	makeCollector := func() *Collector {
		return &Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	}
	c := makeCollector()
	if err := c.RepairRawGapStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A new process reconstructs the remaining gap from committed captures.
	c = makeCollector()
	if err := c.RepairRawGapStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(requested) != 2 || requested[0] != [2]uint64{110, 173} || requested[1] != [2]uint64{174, 237} {
		t.Fatalf("unexpected recovery ranges: %v", requested)
	}
	for _, cap := range store.caps[2:] {
		if cap.CaptureMode != "catchup" || cap.Finality != "head" || cap.Status != "complete" || !cap.Committed {
			t.Fatalf("incorrect recovery semantics: %+v", cap)
		}
	}
}

func TestWatchRawGapDoesNotRetryAbandonedDecodeCoverage(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(42161)
	r := protocolChainReader(t, chain, func(method string, _ json.RawMessage) (any, bool) {
		t.Fatalf("covered stream requested RPC %s", method)
		return nil, false
	})
	cap := rawRecoveryCapture(42161, 100, 1000)
	cap.Status, cap.Reason = "partial", "implementation_unknown; "+repairAbandonedMarker
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{42161: r}, Store: &coreStore{caps: []Capture{cap}}}
	if err := c.RepairRawGapStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.captures) != 1 || !strings.Contains(c.captures[0].Reason, repairAbandonedMarker) {
		t.Fatal("abandoned capture changed")
	}
}

func TestWatchRawGapFailedSourceRetainsHoleAndAllowsPeer(t *testing.T) {
	m := runnerManifest(t)
	readers := map[uint64]*Reader{}
	store := &coreStore{}
	for _, chain := range m.Chains {
		readers[chain.ChainID] = protocolChainReader(t, chain, nil)
		store.caps = append(store.caps, rawRecoveryCapture(chain.ChainID, 100, 109), rawRecoveryCapture(chain.ChainID, 1000, 1000))
	}
	r := readers[8453]
	underlying := r.RPC.HTTP.Transport
	r.RPC.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(raw))
		var calls []struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal(raw, &calls); err != nil {
			return nil, err
		}
		for _, call := range calls {
			if call.Method == "eth_getLogs" {
				body, _ := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": call.ID, "error": map[string]any{"code": -32000, "message": "unavailable"}}})
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
			}
		}
		return underlying.RoundTrip(req)
	})
	c := Collector{Manifest: m, Readers: readers, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	if err := c.RepairRawGapStep(context.Background()); err == nil {
		t.Fatal("failed log query appeared successful")
	}
	if err := c.RepairRawGapStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if next := firstUncovered(100, 1000, historyIntervals(c.captures, 8453, false)); next != 110 {
		t.Fatalf("failed query erased the gap: next=%d", next)
	}
	if next := firstUncovered(100, 1000, historyIntervals(c.captures, 42161, false)); next != 174 {
		t.Fatalf("peer did not progress: next=%d", next)
	}
}
