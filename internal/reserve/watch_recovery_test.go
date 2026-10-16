package reserve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type recoveryStore struct {
	memoryStore
	batches []Batch
	cancel  context.CancelFunc
	failure error
}

func (s *recoveryStore) WriteReserveBatch(_ context.Context, b Batch) error {
	if s.failure != nil {
		return s.failure
	}
	if err := Validate(b); err != nil {
		return err
	}
	s.batches = append(s.batches, b)
	s.caps = append(s.caps, b.Capture)
	if b.Capture.CaptureKind == "snapshot" {
		s.cancel()
	}
	return nil
}

func (s *recoveryStore) WriteReserveRevision(ctx context.Context, c Capture) error {
	for n := range s.caps {
		if s.caps[n].BatchId == c.BatchId {
			s.caps[n] = c
		}
	}
	return s.memoryStore.WriteReserveRevision(ctx, c)
}

// Exercise the real Watch control flow, including fresh finality tags and
// canonical revisions. No failed/null or orphan snapshot may reach the store.
func TestWatchRecoversAbsentBlockAndMidCaptureReorg(t *testing.T) {
	for _, fault := range []string{"absent_header", "logs_reorg", "snapshot_reorg", "malformed_header", "storage_failure"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			old, fresh := dex.Digest([]byte("old")), dex.Digest([]byte("fresh"))
			switched, injected, gotLogs := false, false, false
			checks, finalityRefreshes := 0, 0
			r := testReader(t, func(method string, params []json.RawMessage) (any, error) {
				switch method {
				case "eth_chainId":
					return "0x1", nil
				case "eth_getCode":
					return "0x01", nil
				case "eth_getLogs":
					gotLogs = true
					if fault == "logs_reorg" && !injected {
						injected, switched = true, true
					}
					return []any{}, nil
				case "eth_getBlockByNumber":
					var tag string
					_ = json.Unmarshal(params[0], &tag)
					number, hash := "0xa", old
					if tag == "safe" || tag == "finalized" {
						finalityRefreshes++
						number = map[string]string{"safe": "0x8", "finalized": "0x7"}[tag]
						hash = dex.Digest([]byte(tag))
					} else if tag == "0xa" {
						if !injected && (fault == "absent_header" || fault == "malformed_header") {
							injected = true
							if fault == "absent_header" {
								return nil, nil
							}
							number = ""
						}
						if gotLogs {
							checks++
							if fault == "snapshot_reorg" && checks == 2 && !injected {
								injected, switched = true, true
							}
						}
					}
					if switched && (tag == "latest" || tag == "0xa") {
						hash = fresh
					}
					return map[string]string{"number": number, "hash": hash.String(), "parentHash": dex.Digest([]byte("parent")).String(), "timestamp": "0x64", "baseFeePerGas": "0x1", "miner": testToken.String()}, nil
				default:
					return nil, fmt.Errorf("unexpected method %s", method)
				}
			})
			r.RPC.Archive.HashOnly = true
			code, err := CodeHash(ethereum.Result{Raw: json.RawMessage(`"0x01"`)})
			if err != nil {
				t.Fatal(err)
			}
			m := Manifest{Hash: dex.Digest([]byte("manifest")), ImplementationCodeHash: code, QuoterCodeHash: code}
			store := &recoveryStore{cancel: cancel}
			storageFailure := errors.New("database integrity failure")
			if fault == "storage_failure" {
				store.failure = storageFailure
			}
			c := Collector{RPC: r.RPC, Store: store, Manifest: m, StartBlock: 10}
			var messages []string
			err = c.Watch(ctx, false, func(s string) { messages = append(messages, s) })
			if fault == "malformed_header" || fault == "storage_failure" {
				if err == nil || errors.Is(err, context.Canceled) || len(store.batches) != 0 {
					t.Fatalf("persistent failure swallowed: %v %+v", err, store.batches)
				}
				if fault == "storage_failure" && !errors.Is(err, storageFailure) {
					t.Fatal("storage error changed", err)
				}
				return
			}
			if !errors.Is(err, context.Canceled) || !injected {
				t.Fatalf("watch failed to recover: %v %v", err, messages)
			}
			want := old
			if switched {
				want = fresh
				if finalityRefreshes < 4 {
					t.Fatal("reorg retry reused old finality tags", finalityRefreshes)
				}
			}
			last := store.batches[len(store.batches)-1]
			if last.Capture.CaptureKind != "snapshot" || last.Capture.ToHash != Hash(want) {
				t.Fatal("fresh snapshot not committed", last.Capture)
			}
			for _, b := range store.batches {
				if b.Capture.CaptureKind == "snapshot" && b.Capture.ToHash != Hash(want) {
					t.Fatal("orphan snapshot published")
				}
			}
			if fault == "snapshot_reorg" && (len(store.revisions) != 1 || store.revisions[0].Canonical || c.Rewind != 10) {
				t.Fatal("previous logs not invalidated and rewound", store.revisions, c.Rewind)
			}
			if !strings.Contains(strings.Join(messages, "\n"), "retry:") {
				t.Fatal("recovery not observable", messages)
			}
		})
	}
}
