package lst

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func mvp9Header(number uint64, at time.Time) map[string]any {
	return map[string]any{"number": fmt.Sprintf("0x%x", number), "hash": fmt.Sprintf("0x%064x", number), "parentHash": fmt.Sprintf("0x%064x", number-1), "timestamp": fmt.Sprintf("0x%x", at.Unix()), "logsBloom": Hex(strings.Repeat("\x00", 256)), "transactions": []string{}}
}

func mvp9OldCursor(t *testing.T, c *Collector) (string, []byte) {
	t.Helper()
	file := filepath.Join(c.StateDir, "live-logs.gob")
	p := logProgress{Manifest: c.Manifest.Hash, Start: 50, Next: 60, RangeSize: 512, CoverageStartedAt: Now().Add(-time.Hour)}
	if e := writeGob(file, p); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	return file, raw
}

func TestMVP9ExplicitCursorRestartsAndRecoversOnlyContinuousNewCoverage(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	at := Now().Add(-time.Hour).Truncate(time.Second)
	calls := 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if e := json.NewDecoder(req.Body).Decode(&call); e != nil || call.Method != "eth_getBlockByNumber" || len(call.Params) != 2 || string(call.Params[0]) != `"0x64"` {
			t.Fatal("recovery replanned the fixed start or fetched old history", call, e)
		}
		raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": mvp9Header(100, at)})
		if e != nil {
			t.Fatal(e)
		}
		return transportReply(200, string(raw)), nil
	})
	store := &runnerMemoryStore{batches: map[uuid.UUID]Batch{}}
	add := func(from, to uint64, status string, canonical, committed bool, finality string) {
		id := uuid.New()
		store.batches[id] = Batch{Capture: Capture{CaptureId: id, ManifestHash: m.Hash, CaptureKind: "logs", CaptureMode: "live", Revision: 1, Status: status, Canonical: canonical, Committed: committed, Finality: finality, FromBlock: Ptr(from), ToBlock: Ptr(to), FromBlockTime: Ptr(at)}}
	}
	add(50, 59, "complete", true, true, "finalized")
	add(100, 103, "complete", true, true, "finalized")
	add(104, 107, "complete", true, true, "finalized")
	add(109, 120, "complete", true, true, "finalized")
	// None of these records can fill the deliberately missing block 108.
	add(108, 120, "failed", true, true, "finalized")
	add(108, 120, "complete", false, true, "finalized")
	add(108, 120, "complete", true, false, "finalized")
	add(108, 120, "complete", true, true, "head")
	c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://primary.example"}, Store: store, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
	oldFile, oldBytes := mvp9OldCursor(t, &c)
	if c.liveLogBlocks() != 8 || c.logInterval() != time.Minute || c.liveLogCursorPath() == oldFile {
		t.Fatal("new segment inherited old range schedule or cursor")
	}
	first, e := c.logProgress(context.Background())
	if e != nil || first.Start != 100 || first.Next != 108 || first.RangeSize != 8 || !first.CoverageStartedAt.Equal(at) || calls != 1 {
		t.Fatal("new segment crossed a gap or borrowed earliest historical start", first, calls, e)
	}
	restarted, e := c.logProgress(context.Background())
	if e != nil || restarted.Start != 100 || restarted.Next != 108 || calls != 1 {
		t.Fatal("restart refetched or reset a persisted segment", restarted, calls, e)
	}
	if e := os.Remove(c.liveLogCursorPath()); e != nil {
		t.Fatal(e)
	}
	recovered, e := c.logProgress(context.Background())
	if e != nil || recovered.Start != 100 || recovered.Next != 108 || calls != 2 {
		t.Fatal("lost cursor could not recover actual contiguous commits", recovered, calls, e)
	}
	add(108, 108, "complete", true, true, "finalized")
	bridged, e := c.logProgress(context.Background())
	if e != nil || bridged.Next != 121 || calls != 2 {
		t.Fatal("a real committed bridge did not restore continuity", bridged, calls, e)
	}
	// A new segment must not let the legacy cursor jump over 60..99 either.
	legacy := c
	legacy.LiveLogsFromBlock = 0
	old, e := legacy.logProgress(context.Background())
	if e != nil || old.Start != 50 || old.Next != 60 || legacy.liveLogBlocks() != 512 || legacy.logInterval() != 5*time.Minute || calls != 2 {
		t.Fatal("new committed facts bypassed the old gap", old, calls, e)
	}
	current, e := os.ReadFile(oldFile)
	if e != nil || !bytes.Equal(oldBytes, current) {
		t.Fatal("new cursor/recovery modified old cursor bytes", e)
	}
	ranges := reportMergeRanges([]reportRange{{50, 59}, {100, 103}, {104, 107}, {108, 108}, {109, 120}})
	if reportCovered(ranges, 59, 100) || reportCovered(ranges, 50, 120) || !reportCovered(ranges, 100, 120) {
		t.Fatal("report converted two real segments into coverage across the old gap", ranges)
	}
}

func TestMVP9BadPersistedSegmentAndWrongStartHeaderStopWithoutReset(t *testing.T) {
	for _, name := range []string{"wrong_start", "next_before_start", "other_manifest", "wrong_header_height"} {
		t.Run(name, func(t *testing.T) {
			m, e := LoadManifest("../../config/lst-lido-ethereum.json")
			if e != nil {
				t.Fatal(e)
			}
			calls := 0
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					ID uint64 `json:"id"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": mvp9Header(101, Now().Add(-time.Hour))})
				return transportReply(200, string(raw)), nil
			})
			c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://primary.example"}, Store: &runnerMemoryStore{}, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
			oldFile, oldBytes := mvp9OldCursor(t, &c)
			var frozen []byte
			if name != "wrong_header_height" {
				p := logProgress{Manifest: m.Hash, Start: 100, Next: 108, RangeSize: 8}
				switch name {
				case "wrong_start":
					p.Start = 99
				case "next_before_start":
					p.Next = 99
				case "other_manifest":
					p.Manifest = strings.Repeat("x", 32)
				}
				if e := writeGob(c.liveLogCursorPath(), p); e != nil {
					t.Fatal(e)
				}
				frozen, _ = os.ReadFile(c.liveLogCursorPath())
			}
			_, e = c.logProgress(context.Background())
			if e == nil || name == "wrong_header_height" && (calls != 1 || e.Error() != "live_cursor_start_header_mismatch") || name != "wrong_header_height" && calls != 0 {
				t.Fatal("invalid segment was repaired silently or caused source reads", name, calls, e)
			}
			current, _ := os.ReadFile(oldFile)
			if !bytes.Equal(current, oldBytes) {
				t.Fatal("failure changed the old cursor")
			}
			current, readErr := os.ReadFile(c.liveLogCursorPath())
			if name == "wrong_header_height" {
				if !os.IsNotExist(readErr) {
					t.Fatal("bad start header persisted a fake segment")
				}
			} else if readErr != nil || !bytes.Equal(current, frozen) {
				t.Fatal("bad persisted cursor was overwritten", readErr)
			}
		})
	}
}

func TestMVP9HistoricalIdentityUsesLogRPCAndPrimaryAnchorsRemainRequired(t *testing.T) {
	for _, name := range []string{"valid", "log_source_anchor_conflict", "historical_state_unavailable", "unknown_implementation", "event_anchor_conflict", "primary_canonical_conflict"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m, e := LoadManifest("../../config/lst-lido-ethereum.json")
			if e != nil {
				t.Fatal(e)
			}
			at := Now().Add(-time.Hour).Truncate(time.Second)
			amount := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 100), big.NewInt(3))
			event := chainLog{Address: m.Addresses["queue"], Topics: []string{requestedTopic, fmt.Sprintf("0x%064x", 17), fmt.Sprintf("0x%064x", 2), fmt.Sprintf("0x%064x", 3)}, Data: fmt.Sprintf("0x%064x%064x", amount, big.NewInt(99)), BlockNumber: "0x65", BlockHash: fmt.Sprintf("0x%064x", 101), TransactionHash: fmt.Sprintf("0x%064x", 200), TransactionIndex: "0x0", LogIndex: "0x0"}
			calls := map[string]int{}
			var historicalBlocks []string
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				key := req.URL.Host + "/" + call.Method
				calls[key]++
				var result any
				switch call.Method {
				case "eth_getBlockByNumber":
					var tag string
					if json.Unmarshal(call.Params[0], &tag) != nil {
						t.Fatal("header tag")
					}
					n := uint64(110)
					if tag != "finalized" {
						var e error
						n, e = q64(tag)
						if e != nil {
							t.Fatal(e)
						}
					}
					if req.URL.Host == "logs.example" && n != 102 {
						t.Fatal("log source replaced primary chain headers", tag)
					}
					h := mvp9Header(n, at.Add(time.Duration(n-100)*12*time.Second))
					if name == "log_source_anchor_conflict" && req.URL.Host == "logs.example" || name == "primary_canonical_conflict" && req.URL.Host == "primary.example" && calls[key] == 5 {
						h["hash"] = fmt.Sprintf("0x%064x", 999)
					}
					result = h
				case "eth_call":
					if req.URL.Host != "logs.example" {
						t.Fatal("historical queue state still used pruned primary", req.URL.Host)
					}
					var target map[string]string
					var ref struct {
						BlockHash        string `json:"blockHash"`
						RequireCanonical bool   `json:"requireCanonical"`
					}
					if len(call.Params) != 2 || json.Unmarshal(call.Params[0], &target) != nil || json.Unmarshal(call.Params[1], &ref) != nil || !ref.RequireCanonical || target["to"] != Hex(m.Address("queue")) || target["data"] != Hex(string(crypto.Keccak256([]byte("proxy__getImplementation()"))[:4])) {
						t.Fatal("historical identity lost exact contract/position", target, ref)
					}
					historicalBlocks = append(historicalBlocks, ref.BlockHash)
					if name == "historical_state_unavailable" {
						return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"historical state unavailable"}}`, call.ID)), nil
					}
					result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
					if name == "unknown_implementation" {
						result = fmt.Sprintf("0x%064x", 0)
					}
				case "eth_getLogs":
					if req.URL.Host != "logs.example" {
						t.Fatal("logs fetched from wrong source")
					}
					var filter map[string]string
					if len(call.Params) != 1 || json.Unmarshal(call.Params[0], &filter) != nil || filter["address"] != m.Addresses["queue"] || filter["fromBlock"] != "0x64" || filter["toBlock"] != "0x66" {
						t.Fatal("logs range/filter changed", filter)
					}
					v := event
					if name == "event_anchor_conflict" {
						v.BlockHash = fmt.Sprintf("0x%064x", 999)
					}
					result = []chainLog{v}
				default:
					t.Fatal("unexpected source method", call.Method)
				}
				raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
				if e != nil {
					t.Fatal(e)
				}
				return transportReply(200, string(raw)), nil
			})
			tr.cfg.Clock, tr.started, tr.cfg.RPCRequestsPerMinute = realClock{}, Now().Add(-2*time.Minute), 20
			tr.MarkInitialized()
			store := &runnerMemoryStore{}
			c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://primary.example"}, LogRPC: &RPC{Transport: tr, URL: "https://logs.example"}, LogMode: "range", LiveLogsFromBlock: 100, Store: store, StateDir: t.TempDir(), Archive: tr.archive}
			oldFile, oldBytes := mvp9OldCursor(t, &c)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			b, fetchErr := c.Logs(ctx, 100, 102, "live")
			if e := Validate(b); e != nil {
				t.Fatal(e)
			}
			if e := c.Commit(ctx, b); e != nil {
				t.Fatal(e)
			}
			p := logProgress{Manifest: m.Hash, Start: 100, Next: 100, RangeSize: 8}
			p.result(&c, 102, fetchErr)
			if e := writeGob(c.liveLogCursorPath(), p); e != nil {
				t.Fatal(e)
			}
			if name != "valid" {
				if fetchErr == nil || b.Capture.Status != "failed" || b.Capture.Canonical || p.Next != 100 || len(b.Requests)+len(b.Claims)+len(b.Finalizations) != 0 {
					t.Fatal("source/identity conflict produced complete coverage", name, b.Capture, p, fetchErr)
				}
			} else {
				if fetchErr != nil || b.Capture.Status != "complete" || !b.Capture.Canonical || b.Capture.Finality != "finalized" || p.Next != 103 || len(b.Requests) != 1 || b.Requests[0].AmountStethWei.Cmp(amount) != 0 || calls["primary.example/eth_getBlockByNumber"] != 5 || calls["logs.example/eth_getBlockByNumber"] != 1 || calls["logs.example/eth_call"] != 2 || calls["logs.example/eth_getLogs"] != 1 {
					t.Fatal("separate source did not retain primary anchors and exact event", b.Capture, calls, p, fetchErr)
				}
				if len(historicalBlocks) != 2 || historicalBlocks[0] != fmt.Sprintf("0x%064x", 100) || historicalBlocks[1] != fmt.Sprintf("0x%064x", 102) {
					t.Fatal("identity checked another historical position", historicalBlocks)
				}
				var hash dex.Hash
				copy(hash[:], b.Requests[0].EventPayloadHash)
				raw, e := tr.archive.Get(hash)
				var proof struct{ Host string }
				if e != nil || json.Unmarshal(raw, &proof) != nil || proof.Host != "logs.example" {
					t.Fatal("typed event proof borrowed primary source", proof, e)
				}
			}
			current, e := os.ReadFile(oldFile)
			if e != nil || !bytes.Equal(current, oldBytes) {
				t.Fatal("new source commit changed old cursor", e)
			}
			if calls["primary.example/eth_call"] != 0 || calls["primary.example/eth_getLogs"] != 0 {
				t.Fatal("source failure triggered hidden fallback", calls)
			}
		})
	}
}

func TestMVP9WatchWritesNewCursorAndPreservesOldGap(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	at := Now().Add(-time.Hour).Truncate(time.Second)
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "fapi.example" {
			if req.URL.Path != "/fapi/v1/fundingRate" {
				t.Fatal("unexpected CEX task")
			}
			return transportReply(200, `[]`), nil
		}
		var call struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
			t.Fatal(e)
		}
		var result any
		switch call.Method {
		case "eth_getBlockByNumber":
			var tag string
			_ = json.Unmarshal(call.Params[0], &tag)
			if tag == "latest" {
				return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"source unavailable"}}`, call.ID)), nil
			}
			n := uint64(110)
			if tag != "finalized" {
				var e error
				n, e = q64(tag)
				if e != nil {
					t.Fatal(e)
				}
			}
			if n < 100 || req.URL.Host == "logs.example" && n != 107 {
				t.Fatal("watch read the old gap or replaced primary header", req.URL.Host, n)
			}
			result = mvp9Header(n, at.Add(time.Duration(n-100)*12*time.Second))
		case "eth_call":
			if req.URL.Host != "logs.example" {
				t.Fatal("watch identity used primary")
			}
			result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
		case "eth_getLogs":
			if req.URL.Host != "logs.example" {
				t.Fatal("watch logs used primary")
			}
			var filter map[string]string
			_ = json.Unmarshal(call.Params[0], &filter)
			if filter["fromBlock"] != "0x64" || filter["toBlock"] != "0x6b" {
				t.Fatal("watch requested outside its fixed new segment", filter)
			}
			result = []chainLog{}
		default:
			t.Fatal("unexpected watch method", call.Method)
		}
		raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
		if e != nil {
			t.Fatal(e)
		}
		return transportReply(200, string(raw)), nil
	})
	tr.cfg.Clock, tr.started, tr.cfg.RPCRequestsPerMinute = realClock{}, Now().Add(-2*time.Minute), 20
	tr.MarkInitialized()
	cex, e := NewCEX(tr, "https://fapi.example")
	if e != nil {
		t.Fatal(e)
	}
	store := &runnerMemoryStore{}
	c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://primary.example", ProtocolMulticall: true}, LogRPC: &RPC{Transport: tr, URL: "https://logs.example"}, CEX: cex, Metadata: cexTestMetadata(), Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "range", LiveLogsFromBlock: 100, MarketInterval: 2 * time.Minute, EntryRoutesPerRound: 1}
	oldFile, frozen := mvp9OldCursor(t, &c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var progress []string
	e = c.Watch(ctx, false, func(message string) {
		progress = append(progress, message)
		if strings.HasPrefix(message, "http_stats=") {
			cancel()
		}
	})
	if !errors.Is(e, context.Canceled) {
		t.Fatal("watch did not complete its first maintenance round", e, progress)
	}
	var p logProgress
	if e := readGob(c.liveLogCursorPath(), &p); e != nil || p.Start != 100 || p.Next != 108 {
		t.Fatal("watch did not persist its actual new range", p, e, progress)
	}
	current, e := os.ReadFile(oldFile)
	if e != nil || !bytes.Equal(current, frozen) {
		t.Fatal("watch saved the new segment into the old cursor", e)
	}
	logs := 0
	for _, b := range store.batches {
		if e := Validate(b); e != nil {
			t.Fatal(e)
		}
		if b.Capture.CaptureKind == "logs" {
			logs++
			if b.Capture.Status != "complete" || !b.Capture.Canonical || *b.Capture.FromBlock != 100 || *b.Capture.ToBlock != 107 {
				t.Fatal("watch fabricated gap coverage", b.Capture)
			}
		}
	}
	if logs != 1 || reportCovered(reportMergeRanges([]reportRange{{50, 59}, {100, 107}}), 59, 100) {
		t.Fatal("unexpected log range or false report continuity", logs)
	}
}

type mvp9ContextStore struct {
	runnerMemoryStore
	prefix []Capture
}

func (s *mvp9ContextStore) LSTCaptures(ctx context.Context, manifest string) ([]Capture, error) {
	caps, err := s.runnerMemoryStore.LSTCaptures(ctx, manifest)
	return append(append([]Capture(nil), s.prefix...), caps...), err
}

func (s *mvp9ContextStore) WriteLST(ctx context.Context, b Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runnerMemoryStore.WriteLST(ctx, b)
}

func TestMVP9OldGapCommitsBeforeAdvanceAndUsesParentWriteContext(t *testing.T) {
	for _, name := range []string{"success", "source_failure", "commit_failure", "fetch_canceled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m, e := LoadManifest("../../config/lst-lido-ethereum.json")
			if e != nil {
				t.Fatal(e)
			}
			fetchCtx, cancelFetch := context.WithCancel(context.Background())
			defer cancelFetch()
			at := Now().Add(-time.Hour).Truncate(time.Second)
			calls := 0
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				var result any
				switch call.Method {
				case "eth_getBlockByNumber":
					var tag string
					_ = json.Unmarshal(call.Params[0], &tag)
					n := uint64(110)
					if tag != "finalized" {
						var e error
						n, e = q64(tag)
						if e != nil || n != 60 && n != 91 {
							t.Fatal("old helper crossed its 32-block piece", tag, e)
						}
					}
					result = mvp9Header(n, at.Add(time.Duration(int64(n)-100)*12*time.Second))
				case "eth_call":
					if req.URL.Host != "logs.example" {
						t.Fatal("old gap used the pruned main source")
					}
					if name == "source_failure" {
						return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"historical state unavailable"}}`, call.ID)), nil
					}
					if name == "fetch_canceled" {
						cancelFetch()
						return nil, context.Canceled
					}
					result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
				case "eth_getLogs":
					var filter map[string]string
					_ = json.Unmarshal(call.Params[0], &filter)
					if req.URL.Host != "logs.example" || filter["fromBlock"] != "0x3c" || filter["toBlock"] != "0x5b" {
						t.Fatal("old gap crossed its actual fixed piece", filter)
					}
					result = []chainLog{}
				default:
					t.Fatal("unexpected old task", call.Method)
				}
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
				return transportReply(200, string(raw)), nil
			})
			tr.cfg.Clock, tr.started, tr.cfg.RPCRequestsPerMinute = realClock{}, Now().Add(-2*time.Minute), 20
			tr.MarkInitialized()
			store := &mvp9ContextStore{
				runnerMemoryStore: runnerMemoryStore{fail: name == "commit_failure"},
				prefix:            []Capture{{CaptureId: uuid.New(), ManifestHash: m.Hash, CaptureKind: "logs", CaptureMode: "live", Status: "complete", Canonical: true, Committed: true, Finality: "finalized", FromBlock: Ptr(uint64(50)), ToBlock: Ptr(uint64(59)), FromBlockTime: Ptr(at)}},
			}
			c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://primary.example"}, LogRPC: &RPC{Transport: tr, URL: "https://logs.example"}, Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "range", LiveLogsFromBlock: 100}
			oldFile, oldBytes := mvp9OldCursor(t, &c)
			newCursor := logProgress{Manifest: m.Hash, Start: 100, Next: 111, RangeSize: 8}
			if e := writeGob(c.liveLogCursorPath(), newCursor); e != nil {
				t.Fatal(e)
			}
			newBytes, _ := os.ReadFile(c.liveLogCursorPath())
			parent, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			end, e := c.RPC.Header(parent, "finalized")
			if e != nil {
				t.Fatal(e)
			}
			e = c.catchUpOldLogGap(fetchCtx, parent, end, func(string) {})
			var old logProgress
			if readErr := readGob(oldFile, &old); readErr != nil {
				t.Fatal(readErr)
			}
			if name == "commit_failure" {
				if e == nil || old.Next != 60 || len(store.batches) != 0 {
					t.Fatal("failed write advanced old coverage", old, e)
				}
				current, _ := os.ReadFile(oldFile)
				if !bytes.Equal(current, oldBytes) {
					t.Fatal("precommit changed the old cursor")
				}
				var pending Batch
				if e := readGob(filepath.Join(c.StateDir, "pending-batch.gob"), &pending); e != nil {
					t.Fatal("failed write did not preserve exact pending batch", e)
				}
				if e := Validate(pending); e != nil || pending.Capture.Status != "complete" {
					t.Fatal("pending changed captured facts", e)
				}
				frozenID, frozenDigest, beforeCalls := pending.Capture.CaptureId, pending.Capture.FactDigest, calls
				store.fail = false
				if e := c.FlushPending(parent); e != nil || calls != beforeCalls || store.batches[frozenID].Capture.FactDigest != frozenDigest {
					t.Fatal("pending retry refetched or altered facts", e)
				}
				legacy := c
				legacy.LiveLogsFromBlock = 0
				p, e := legacy.logProgress(parent)
				if e != nil || p.Next != 92 || calls != beforeCalls {
					t.Fatal("committed batch could not recover pre-fsync old cursor", p, e)
				}
			} else {
				wantNext, wantStatus := uint64(92), "complete"
				if name != "success" {
					wantNext, wantStatus = 60, "failed"
				}
				if e != nil || old.Next != wantNext || old.RangeSize > 32 || len(store.batches) != 1 {
					t.Fatal("old helper hid source failure or moved before commit", old, len(store.batches), e)
				}
				for _, b := range store.batches {
					if e := Validate(b); e != nil || b.Capture.Status != wantStatus || wantStatus == "failed" && (b.Capture.Canonical || len(b.Requests)+len(b.Claims)+len(b.Finalizations) != 0) {
						t.Fatal("old helper changed failed source into coverage", b.Capture, e)
					}
				}
			}
			current, _ := os.ReadFile(c.liveLogCursorPath())
			if !bytes.Equal(current, newBytes) {
				t.Fatal("old catchup wrote or reset the current segment")
			}
		})
	}
}

func TestMVP9OldGapRoomDoesNotSendOrAlterProviderReservations(t *testing.T) {
	calls := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("no HTTP expected while planning")
	})
	tr.cfg.RPCRequestsPerMinute = 20
	c := Collector{RPC: &RPC{Transport: tr, URL: "https://primary.example"}, LogRPC: &RPC{Transport: tr, URL: "https://logs.example"}}
	setSlots := func(endpoint string, available int) {
		g, e := tr.gate("rpc", endpoint)
		if e != nil {
			t.Fatal(e)
		}
		g.mu.Lock()
		g.state.Recent = nil
		for range 20 - available {
			g.state.Recent = append(g.state.Recent, sentRequest{At: clock.Now(), Class: "normal"})
		}
		g.mu.Unlock()
	}
	for _, tc := range []struct {
		remaining     time.Duration
		primary, logs int
		want          bool
	}{{34 * time.Second, 20, 20, false}, {40 * time.Second, 3, 20, false}, {40 * time.Second, 20, 5, false}, {40 * time.Second, 4, 6, true}} {
		setSlots(c.RPC.URL, tc.primary)
		setSlots(c.LogRPC.URL, tc.logs)
		ctx, stop := context.WithTimeout(context.Background(), tc.remaining)
		room, e := c.oldLogGapHasRoom(ctx)
		stop()
		p, _ := tr.AvailableRPCSlots(c.RPC.URL)
		l, _ := tr.AvailableRPCSlots(c.LogRPC.URL)
		if e != nil || room != tc.want || p != tc.primary || l != tc.logs || calls != 0 {
			t.Fatal("planner changed quota or ignored time/host budget", tc, room, p, l, e)
		}
	}
}

func TestMVP9LogBudgetEightInheritsOldReservationsAndUsesRollingFiveMinutes(t *testing.T) {
	var clock *transportFakeClock
	var sends []time.Time
	tr, fake := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sends = append(sends, clock.Now())
		return transportReply(200, `[]`), nil
	})
	clock = fake
	clock.at = Now().Truncate(time.Microsecond)
	tr.started, tr.cfg.RPCRequestsPerMinute = clock.Now().Add(-2*time.Minute), 20
	tr.MarkInitialized()
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]}`)
	for range 4 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://logs.example", body, "logs"); e != nil {
			t.Fatal(e)
		}
	}
	g, e := tr.gate("rpc", "https://logs.example")
	if e != nil {
		t.Fatal(e)
	}
	g.mu.Lock()
	frozen := append([]sentRequest(nil), g.state.Recent...)
	g.mu.Unlock()
	if len(frozen) != 4 {
		t.Fatal("default reservations not persisted", frozen)
	}
	cfg := tr.cfg
	cfg.LogRequestsPerFiveMinutes = 8
	restart, e := NewTransport(cfg)
	if e != nil {
		t.Fatal(e)
	}
	restart.MarkInitialized()
	for range 4 {
		if _, e := restart.Do(context.Background(), "rpc", "POST", "https://logs.example", body, "logs"); e != nil {
			t.Fatal(e)
		}
	}
	newGate, e := restart.gate("rpc", "https://logs.example")
	if e != nil {
		t.Fatal(e)
	}
	newGate.mu.Lock()
	current := append([]sentRequest(nil), newGate.state.Recent...)
	newGate.mu.Unlock()
	oldJSON, _ := json.Marshal(frozen)
	retainedJSON, _ := json.Marshal(current[:min(len(current), 4)])
	if len(current) != 8 || len(sends) != 8 || !bytes.Equal(oldJSON, retainedJSON) || sends[7].Sub(sends[0]) >= 5*time.Minute {
		t.Fatal("explicit cap reset old history or retained the four-request budget", current, len(sends))
	}
	before := clock.Now()
	ctx, cancel := context.WithDeadline(context.Background(), before.Add(20*time.Second))
	res, e := restart.Do(ctx, "rpc", "POST", "https://logs.example", body, "logs")
	cancel()
	var wait *gateWaitError
	if !errors.As(e, &wait) || wait.Cause != "local_gate_budget_exhausted" || len(sends) != 8 || !clock.Now().Equal(before) || !res.RequestedAt.IsZero() || res.PayloadHash != "" {
		t.Fatal("ninth log request bypassed the rolling cap or became a false HTTP attempt", res, e, len(sends))
	}
	newGate.mu.Lock()
	blockedJSON, _ := json.Marshal(newGate.state.Recent)
	newGate.mu.Unlock()
	priorJSON, _ := json.Marshal(current)
	if !bytes.Equal(blockedJSON, priorJSON) {
		t.Fatal("blocked request rewrote reservations")
	}
	if e := clock.Sleep(context.Background(), sends[0].Add(5*time.Minute).Sub(clock.Now())); e != nil {
		t.Fatal(e)
	}
	if _, e := restart.Do(context.Background(), "rpc", "POST", "https://logs.example", body, "logs"); e != nil || len(sends) != 9 || sends[8].Sub(sends[0]) != 5*time.Minute {
		t.Fatal("five-minute expiry did not release exactly the oldest reservation", sends, e)
	}
	for i, at := range sends {
		within := 0
		for _, previous := range sends[:i+1] {
			if previous.After(at.Add(-5 * time.Minute)) {
				within++
			}
		}
		if within > 8 || i > 0 && at.Sub(sends[i-1]) < 10*time.Second {
			t.Fatal("rolling count or existing minimum getLogs gap changed", within, sends)
		}
	}
}

func TestMVP9LogBudgetInvalidConfigAndBackfillDefault(t *testing.T) {
	for _, value := range []int{-1, 31} {
		if _, e := NewTransport(TransportConfig{StateDir: t.TempDir(), ArchiveDir: t.TempDir(), LogRequestsPerFiveMinutes: value}); e == nil || e.Error() != "transport_log_five_minute_limit_invalid" {
			t.Fatal("invalid log budget accepted", value, e)
		}
	}
	var clock *transportFakeClock
	var sends []time.Time
	tr, fake := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sends = append(sends, clock.Now())
		return transportReply(200, `[]`), nil
	})
	clock = fake
	tr.cfg.Backfill = true
	tr.MarkInitialized()
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]}`)
	for range 9 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://logs.example", body, "logs"); e != nil {
			t.Fatal(e)
		}
	}
	if sends[8].Sub(sends[0]) >= 5*time.Minute {
		t.Fatal("zero config applied the new live eight-request cap to backfill")
	}
}
