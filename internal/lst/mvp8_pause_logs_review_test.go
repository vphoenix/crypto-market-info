package lst

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type mvp8MaintenanceStore struct {
	runnerMemoryStore
	oldMarket                 uuid.UUID
	gasInventoryAfterFinality bool
}

func (s *mvp8MaintenanceStore) LSTCaptures(ctx context.Context, manifest string) ([]Capture, error) {
	// After the quiet round's finality revision, the gas task must still inspect
	// existing coverage. It need not send a receipt request with no full UTC day.
	if b, ok := s.batches[s.oldMarket]; ok && b.Capture.Finality == "finalized" {
		s.gasInventoryAfterFinality = true
	}
	return s.runnerMemoryStore.LSTCaptures(ctx, manifest)
}

func TestMVP8PausedWatchPreservesCursorAndOtherMaintenance(t *testing.T) {
	for _, pause := range []bool{true, false} {
		t.Run(fmt.Sprintf("pause_%t", pause), func(t *testing.T) {
			t.Parallel()
			m, err := LoadManifest("../../config/lst-lido-ethereum.json")
			if err != nil {
				t.Fatal(err)
			}
			calls, fundingCalls, historicalCalls := 0, 0, 0
			var requestedTags []string
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "fapi.example" {
					if req.URL.Path != "/fapi/v1/fundingRate" {
						t.Fatal("unexpected Binance request", req.URL.Path)
					}
					fundingCalls++
					return transportReply(200, `[]`), nil
				}
				calls++
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				if call.Method != "eth_getBlockByNumber" {
					t.Fatal("paused watch sent a log/receipt/state request", call.Method)
				}
				var tag string
				if json.Unmarshal(call.Params[0], &tag) != nil {
					t.Fatal("header tag")
				}
				requestedTags = append(requestedTags, tag)
				if tag == "latest" {
					// A valid RPC source error yields an honest unknown market batch,
					// then permits testing the actual first maintenance pass promptly.
					return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"source unavailable"}}`, call.ID)), nil
				}
				n := uint64(110)
				if tag != "finalized" {
					var e error
					n, e = q64(tag)
					if e != nil {
						t.Fatal(e)
					}
					if n != 99 {
						historicalCalls++
						if pause {
							t.Fatal("paused watch read a gap block", n)
						}
					}
				}
				result := map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": fmt.Sprintf("0x%064x", n), "parentHash": fmt.Sprintf("0x%064x", n-1), "timestamp": fmt.Sprintf("0x%x", Now().Add(-time.Hour).Truncate(time.Second).Unix()+int64(n)*12), "logsBloom": Hex(strings.Repeat("\x00", 256)), "transactions": []string{}}
				raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
				if e != nil {
					t.Fatal(e)
				}
				return transportReply(200, string(raw)), nil
			})
			tr.cfg.Clock, tr.started, tr.cfg.RPCRequestsPerMinute = realClock{}, Now().Add(-2*time.Minute), 20
			tr.MarkInitialized()
			cex, err := NewCEX(tr, "https://fapi.example")
			if err != nil {
				t.Fatal(err)
			}
			oldID := uuid.New()
			oldHash := mustMVP7Hash(t, 99)
			store := &mvp8MaintenanceStore{runnerMemoryStore: runnerMemoryStore{batches: map[uuid.UUID]Batch{oldID: {Capture: Capture{CaptureId: oldID, ManifestHash: m.Hash, CaptureKind: "market", CaptureMode: "live", StartedAt: Now().Add(-time.Hour), Status: "partial", Committed: true, Canonical: true, Finality: "head", Revision: 1, ToBlock: Ptr(uint64(99)), ToBlockHash: &oldHash}}}}, oldMarket: oldID}
			c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example", ProtocolMulticall: true}, CEX: cex, Manifest: m, Metadata: cexTestMetadata(), Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "receipts", MarketInterval: 2 * time.Minute, EntryRoutesPerRound: 1, PauseLiveLogs: pause}
			cursorPath := filepath.Join(c.StateDir, "live-logs.gob")
			frozen := logProgress{Manifest: m.Hash, Start: 100, Next: 100, RangeSize: 8, CoverageStartedAt: Now().Add(-time.Hour)}
			if e := writeGob(cursorPath, frozen); e != nil {
				t.Fatal(e)
			}
			original, e := os.ReadFile(cursorPath)
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			var progress []string
			e = c.Watch(ctx, false, func(message string) {
				progress = append(progress, message)
				if strings.HasPrefix(message, "http_stats=") {
					cancel()
				}
			})
			if !errors.Is(e, context.Canceled) {
				t.Fatal("one-round cancellation lost", e, progress)
			}
			if fundingCalls != 1 || store.batches[oldID].Capture.Finality != "finalized" || store.batches[oldID].Capture.Revision != 2 {
				t.Fatal("pausing logs also paused funding/finality", fundingCalls, store.batches[oldID].Capture, requestedTags)
			}
			markets, funds, logs := 0, 0, 0
			for id, b := range store.batches {
				if id == oldID {
					continue
				}
				if e := Validate(b); e != nil {
					t.Fatal(e)
				}
				switch b.Capture.CaptureKind {
				case "market":
					markets++
					if len(b.Quotes) != 8 || b.Capture.Status != "partial" || b.Capture.Canonical {
						t.Fatal("source error forged current quotes", b.Capture, len(b.Quotes))
					}
				case "funding":
					funds++
					if b.Capture.Status != "complete" {
						t.Fatal("empty public funding response was hidden", b.Capture)
					}
				case "logs":
					logs++
				}
			}
			if markets != 1 || funds != 1 {
				t.Fatal("current observations missing", markets, funds)
			}
			current, e := os.ReadFile(cursorPath)
			if e != nil {
				t.Fatal(e)
			}
			if pause {
				if !bytes.Equal(original, current) || historicalCalls != 0 || logs != 0 || calls != 3 || !store.gasInventoryAfterFinality {
					t.Fatal("pause changed coverage or skipped other maintenance", calls, historicalCalls, logs, store.gasInventoryAfterFinality, progress)
				}
			} else {
				var saved logProgress
				if e := readGob(cursorPath, &saved); e != nil {
					t.Fatal(e)
				}
				if historicalCalls == 0 || logs == 0 || saved.Next <= 100 || bytes.Equal(original, current) {
					t.Fatal("default Watch no longer scans logs", calls, historicalCalls, logs, saved, progress)
				}
			}
		})
	}
}
