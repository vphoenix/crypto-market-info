package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type runnerProtocolStore struct {
	batches   []Batch
	revisions []Capture
}

func (s *runnerProtocolStore) AcrossCaptures(context.Context, string) ([]Capture, error) {
	o := []Capture{}
	for _, b := range s.batches {
		o = append(o, b.Capture)
	}
	return o, nil
}
func (s *runnerProtocolStore) AcrossBatch(_ context.Context, c Capture) (Batch, error) {
	for _, b := range s.batches {
		if b.Capture.CaptureId == c.CaptureId {
			return b, nil
		}
	}
	return Batch{}, errors.New("missing_test_batch")
}
func (s *runnerProtocolStore) WriteAcrossBatch(_ context.Context, b Batch) error {
	s.batches = append(s.batches, b)
	return nil
}
func (s *runnerProtocolStore) WriteAcrossRevision(_ context.Context, c Capture) error {
	s.revisions = append(s.revisions, c)
	return nil
}
func protocolHeader(n uint64, h string, at time.Time) map[string]any {
	return map[string]any{"number": height(n), "hash": Hex(h), "parentHash": Hex(strings.Repeat("p", 32)), "timestamp": fmt.Sprintf("0x%x", at.Unix())}
}
func protocolChainReader(t *testing.T, c ChainConfig, override func(string, json.RawMessage) (any, bool)) *Reader {
	t.Helper()
	at := Now().Truncate(time.Second)
	r := mockReader(t, c.ChainID, func(method string, p json.RawMessage) any {
		if override != nil {
			if out, ok := override(method, p); ok {
				return out
			}
		}
		switch method {
		case "eth_getBlockByNumber":
			var args []json.RawMessage
			json.Unmarshal(p, &args)
			var tag string
			json.Unmarshal(args[0], &tag)
			n := uint64(100)
			if strings.HasPrefix(tag, "0x") {
				n, _ = q64(tag)
			}
			return protocolHeader(n, strings.Repeat("h", 32), at)
		case "eth_chainId":
			return height(c.ChainID)
		case "eth_getStorageAt":
			return Hex(WordAddress(strings.Repeat("i", 20)))
		case "eth_getCode":
			return "0x0102"
		case "eth_getLogs":
			return []any{}
		case "eth_call":
			var args []json.RawMessage
			json.Unmarshal(p, &args)
			var call map[string]string
			json.Unmarshal(args[0], &call)
			for name, m := range ABI.Methods {
				if strings.HasPrefix(call["data"], "0x"+fmt.Sprintf("%x", m.ID)) {
					var v any
					switch name {
					case "chainId":
						v = new(big.Int).SetUint64(c.ChainID)
					case "decimals":
						v = uint8(6)
					case "symbol":
						v = "USDC"
					case "fillStatuses":
						v = big.NewInt(2)
					case "pausedFills":
						v = false
					case "getCurrentTime":
						v = big.NewInt(at.Unix())
					default:
						t.Fatal(name)
					}
					b, e := m.Outputs.Pack(v)
					if e != nil {
						t.Fatal(e)
					}
					return Hex(string(b))
				}
			}
			t.Fatal(call)
		}
		t.Fatal(method)
		return nil
	})
	c.Implementations = []Implementation{{Address: strings.Repeat("i", 20), CodeHash: string(crypto.Keccak256([]byte{1, 2})), ABIRevision: ABIRevision}}
	r.Chain = c
	return r
}
func runnerManifest(t *testing.T) Manifest {
	t.Helper()
	m, e := LoadManifest("../../config/across-research.json")
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func TestRunnerProtocolCanonicalFailureDoesNotCloseOrder(t *testing.T) {
	m := runnerManifest(t)
	source, _ := m.Chain(8453)
	dest, _ := m.Chain(42161)
	store := &runnerProtocolStore{}
	sourceReader := protocolChainReader(t, source, nil)
	headers := 0
	targetReader := protocolChainReader(t, dest, func(method string, p json.RawMessage) (any, bool) {
		if method == "eth_getBlockByNumber" {
			headers++
			if headers > 1 {
				return protocolHeader(100, strings.Repeat("o", 32), Now()), true
			}
		}
		return nil, false
	})
	d := Deposit{ChainId: 8453, SpokePool: source.SpokePool, BlockNumber: 100, BlockHash: strings.Repeat("h", 32), DestinationChainId: 42161, DepositId: big.NewInt(1), RelayHash: strings.Repeat("r", 32), InputToken: WordAddress(source.USDC), OutputToken: WordAddress(dest.USDC), FillDeadline: uint32(Now().Unix() + 60), ExclusiveRelayer: strings.Repeat("\x00", 32), AvailableAt: Now()}
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: sourceReader, 42161: targetReader}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	w := &watcher{Orders: map[string]*pendingOrder{}, Cursors: map[uint64]uint64{}}
	key := orderKey(d)
	w.Orders[key] = &pendingOrder{Deposit: d, Origin: "live"}
	w.Tasks = []probeTask{{Key: key, Planned: Now().Add(-time.Second), Kind: "baseline"}}
	if e := c.runTasks(context.Background(), w); e != nil {
		t.Fatal(e)
	}
	if _, ok := w.Orders[key]; !ok {
		t.Fatal("partial filled status incorrectly terminated order")
	}
	if len(store.batches) != 1 {
		t.Fatal("missing probe evidence")
	}
	p := store.batches[0].Probes[0]
	if p.ProbeStatus != "rpc_error" || p.FillStatus == nil || *p.FillStatus != 2 || ProbeOpen(d, p) {
		t.Fatalf("bad failed canonical probe: %+v", p)
	}
}
func TestRunnerProtocolReorgInvalidatesEveryTypeAndAttempt(t *testing.T) {
	m := runnerManifest(t)
	ch, _ := m.Chain(8453)
	store := &runnerProtocolStore{}
	reader := protocolChainReader(t, ch, func(method string, p json.RawMessage) (any, bool) {
		if method == "eth_getBlockByNumber" {
			var args []json.RawMessage
			json.Unmarshal(p, &args)
			var tag string
			json.Unmarshal(args[0], &tag)
			if tag == "finalized" {
				return protocolHeader(99, strings.Repeat("f", 32), Now()), true
			}
			return protocolHeader(100, strings.Repeat("n", 32), Now()), true
		}
		return nil, false
	})
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: reader}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}, loaded: true}
	for _, kind := range []string{"logs", "receipts", "probes"} {
		for _, status := range []string{"complete", "partial", "error"} {
			c.captures = append(c.captures, Capture{CaptureId: ID(kind + status), ChainId: 8453, CaptureKind: kind, Status: status, Canonical: true, Committed: true, Finality: "head", Revision: 1, FromBlock: Ptr(uint64(100)), ToBlock: Ptr(uint64(100)), FromHash: Ptr(strings.Repeat("o", 32)), ToHash: Ptr(strings.Repeat("o", 32))})
		}
	}
	if e := c.Reconcile(context.Background(), 8453); e != nil {
		t.Fatal(e)
	}
	if len(store.revisions) != 9 {
		t.Fatalf("only %d attempts invalidated", len(store.revisions))
	}
	for _, v := range store.revisions {
		if v.Canonical || v.Finality != "orphaned" || v.Revision != 2 {
			t.Fatal("old attempt remains canonical")
		}
	}
}
func TestRunnerProtocolZeroLogsAreCoverageButNullIsFailure(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			m := runnerManifest(t)
			ch, _ := m.Chain(8453)
			store := &runnerProtocolStore{}
			r := protocolChainReader(t, ch, func(method string, p json.RawMessage) (any, bool) {
				if bad && method == "eth_getLogs" {
					return nil, true
				}
				return nil, false
			})
			c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: r}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
			b, e := c.CollectRange(context.Background(), 8453, 100, 100, "backfill", "finalized")
			if bad {
				if e == nil || b.Capture.Status != "error" || b.Capture.CompletedTasks != 0 {
					t.Fatal("null logs became successful coverage")
				}
			} else if e != nil || b.Capture.Status != "complete" || b.Capture.CompletedTasks != 1 {
				t.Fatalf("empty logs must be covered: %v %+v", e, b.Capture)
			}
			if len(store.batches) != 1 || !store.batches[0].Capture.Committed {
				t.Fatal("missing committed evidence")
			}
		})
	}
}
func TestRunnerProtocolOneChainFailureDoesNotStopOther(t *testing.T) {
	m := runnerManifest(t)
	a, _ := m.Chain(8453)
	b, _ := m.Chain(42161)
	store := &runnerProtocolStore{}
	failed := protocolChainReader(t, a, func(method string, p json.RawMessage) (any, bool) {
		if method == "eth_getBlockByNumber" {
			return nil, true
		}
		return nil, false
	})
	good := protocolChainReader(t, b, nil)
	c := Collector{Manifest: m, Readers: map[uint64]*Reader{8453: failed, 42161: good}, Store: store, Archive: ethereum.Archive{Dir: t.TempDir()}}
	if e := c.Watch(context.Background(), true, nil); e != nil {
		t.Fatal(e)
	}
	found := false
	for _, b := range store.batches {
		if b.Capture.ChainId == 42161 && b.Capture.CaptureKind == "logs" && b.Capture.CompletedTasks == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("healthy chain did not collect")
	}
}
