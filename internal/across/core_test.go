package across

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func coreBatch(t *testing.T) (Batch, ethereum.Archive) {
	t.Helper()
	at := time.Date(2026, 10, 2, 0, 0, 0, 123456000, time.UTC)
	h := strings.Repeat("\xff", 32)
	b := Batch{Capture: Capture{ManifestHash: h, CaptureId: ID("testcapture"), ChainId: 8453, CaptureKind: "logs", CaptureMode: "live", FromBlock: Ptr(uint64(10)), ToBlock: Ptr(uint64(10)), FromHash: &h, ToHash: &h, StartedAt: at, AvailableAt: at, SourceId: "public", Status: "complete", ExpectedTasks: 1, CompletedTasks: 1, Canonical: true, Finality: "head", Revision: 1}}
	d := Deposit{CaptureId: b.Capture.CaptureId, ChainId: 8453, SpokePool: strings.Repeat("\xfe", 20), BlockNumber: 10, BlockHash: h, BlockTime: at, TxHash: h, DestinationChainId: 42161, DepositId: new(big.Int).Lsh(big.NewInt(1), 200), Depositor: h, Recipient: h, ExclusiveRelayer: h, InputToken: h, OutputToken: h, InputAmountRaw: big.NewInt(2000), OutputAmountRaw: big.NewInt(1900), MessageHash: h, RelayHash: h, LiveReceivedAt: &at, AvailableAt: at, PayloadHash: h}
	b.Deposits = []Deposit{d}
	a := ethereum.Archive{Dir: t.TempDir()}
	if e := ArchiveBatch(a, &b, []EvidenceHeader{{ChainID: 8453, Number: 10, Hash: Hex(h), Time: at}}, nil); e != nil {
		t.Fatal(e)
	}
	return b, a
}
func TestAcrossCoreMembersBinaryAndEvidence(t *testing.T) {
	b, a := coreBatch(t)
	if e := Validate(b); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadCaptureEvidence(a, b.Capture); e != nil {
		t.Fatal(e)
	}
	if ID("\xff") == ID("\xfe") {
		t.Fatal("binary collision")
	}
	b.Deposits[0].OutputAmountRaw = big.NewInt(1800)
	if Validate(b) == nil {
		t.Fatal("mutated members accepted")
	}
}
func TestAcrossCoreEvidenceMalformedArraysFail(t *testing.T) {
	b, a := coreBatch(t)
	e, err := ReadCaptureEvidence(a, b.Capture)
	if err != nil {
		t.Fatal(err)
	}
	e.RowCounts = nil
	h, err := a.PutObject(e)
	if err != nil {
		t.Fatal(err)
	}
	b.Capture.EvidenceHash = string(h[:])
	if _, err = ReadCaptureEvidence(a, b.Capture); err == nil {
		t.Fatal("accepted malformed evidence")
	}
}
func TestAcrossOpenBoundariesAndFreshness(t *testing.T) {
	d := Deposit{FillDeadline: 100, ExclusivityDeadline: 90, ExclusiveRelayer: strings.Repeat("\x01", 32)}
	p := OrderProbe{ProbeStatus: "ok", FillStatus: Ptr(uint8(1)), PausedFills: Ptr(false), ContractTime: Ptr(uint64(90))}
	if ProbeOpen(d, p) {
		t.Fatal("exclusive deadline equality open")
	}
	p.ContractTime = Ptr(uint64(91))
	if !ProbeOpen(d, p) {
		t.Fatal("slow requested is not already filled")
	}
	p.ContractTime = Ptr(uint64(100))
	if !ProbeOpen(d, p) {
		t.Fatal("fill deadline equality excluded")
	}
	p.ContractTime = Ptr(uint64(101))
	if ProbeOpen(d, p) {
		t.Fatal("expired open")
	}
	p.ContractTime = Ptr(uint64(91))
	p.ProbeStatus = "rpc_error"
	if ProbeOpen(d, p) {
		t.Fatal("failed query considered open")
	}
	m := Manifest{FreshMillis: 5000, FutureMillis: 2000}
	at := Now()
	if !fresh(at, at.Add(-5*time.Second), m) || fresh(at, at.Add(-5*time.Second-time.Microsecond), m) || fresh(at, at.Add(2*time.Second+time.Microsecond), m) {
		t.Fatal("freshness boundary")
	}
}
func TestAcrossFollowupsAreActualScheduledTasks(t *testing.T) {
	base := Now()
	w := watcher{}
	w.follow("x", base)
	if len(w.Tasks) != 3 {
		t.Fatal("missing followups")
	}
	for i, d := range []uint32{2000, 5000, 10000} {
		if *w.Tasks[i].Delay != d || !w.Tasks[i].Planned.Equal(base.Add(time.Duration(d)*time.Millisecond)) {
			t.Fatal("wrong schedule")
		}
	}
}
func TestAcrossBBOExactStrictAndNeverRefreshCachedTime(t *testing.T) {
	raw := []byte(`{"symbol":"ETHUSDT","bidPrice":"2000.123456789012345678","askPrice":"2001.000000000000000001","bidQty":"10.00","askQty":"5.00"}`)
	b, e := ParseBBO(raw, "ETHUSDT")
	if e != nil {
		t.Fatal(e)
	}
	if b.Bid.String() != "2000.123456789012345678" {
		t.Fatal("lost decimal precision")
	}
	var v map[string]any
	json.Unmarshal(raw, &v)
	for _, bad := range []any{2.3, "NaN", "1e3", "-1", "0.1234567890123456789"} {
		v["bidPrice"] = bad
		r, _ := json.Marshal(v)
		if _, e := ParseBBO(r, "ETHUSDT"); e == nil {
			t.Fatal("accepted invalid decimal", bad)
		}
	}
	b.RequestedAt = Now().Add(-61 * time.Second)
	b.AvailableAt = b.RequestedAt
	b.PayloadHash = strings.Repeat("\x01", 32)
	prices := Prices{ETH: &b}
	p := OrderProbe{AvailableAt: Now()}
	prices.Attach(&p)
	if p.EthUsdtAsk != nil {
		t.Fatal("stale cachedprice accepted")
	}
}

type coreStore struct {
	caps    []Capture
	batches map[string]Batch
	fail    bool
}

func (s *coreStore) AcrossCaptures(context.Context, string) ([]Capture, error) { return s.caps, nil }
func (s *coreStore) AcrossBatch(_ context.Context, c Capture) (Batch, error) {
	b, ok := s.batches[c.CaptureId]
	if !ok {
		return b, errors.New("missing")
	}
	b.Capture = c
	return b, Validate(b)
}
func (s *coreStore) WriteAcrossBatch(_ context.Context, b Batch) error {
	if s.fail {
		return errors.New("simulated_write_failure")
	}
	if s.batches == nil {
		s.batches = map[string]Batch{}
	}
	s.batches[b.Capture.CaptureId] = b
	s.caps = append(s.caps, b.Capture)
	return nil
}
func (s *coreStore) WriteAcrossRevision(_ context.Context, c Capture) error {
	if s.fail {
		return errors.New("simulated_revision_failure")
	}
	for i, v := range s.caps {
		if v.CaptureId == c.CaptureId {
			s.caps[i] = c
			return nil
		}
	}
	return errors.New("missing_capture")
}

func TestAcrossFailedSourceProbeHasNoZeroEvidenceReference(t *testing.T) {
	m := runnerManifest(t)
	source, _ := m.Chain(8453)
	dest, _ := m.Chain(42161)
	b, _ := coreBatch(t)
	d := b.Deposits[0]
	d.ChainId = 8453
	d.DestinationChainId = 42161
	a := ethereum.Archive{Dir: t.TempDir()}
	store := &coreStore{}
	c := Collector{Manifest: m, Store: store, Archive: a, Readers: map[uint64]*Reader{8453: protocolChainReader(t, source, nil), 42161: protocolChainReader(t, dest, nil)}}
	network, cancel := context.WithCancel(context.Background())
	cancel()
	p, e := c.recordProbe(context.Background(), &pendingOrder{Deposit: d, Origin: "live"}, probeTask{Key: orderKey(d), Planned: Now()}, "", network)
	if e != nil {
		t.Fatal(e)
	}
	if p.ProbeStatus != "rpc_error" || len(store.caps) != 1 {
		t.Fatal("failure not preserved")
	}
	proof, e := ReadCaptureEvidence(a, store.caps[0])
	if e != nil {
		t.Fatal(e)
	}
	if len(proof.RPCPayloads) != 0 {
		t.Fatal("nonexistent raw evidence reference", proof.RPCPayloads)
	}
}
func TestAcrossCoverageUnionKeepsGapsAndAcceptsSplitReplay(t *testing.T) {
	rr := []blockInterval{{100, 104}, {105, 110}, {120, 130}}
	if firstUncovered(100, 130, rr) != 111 {
		t.Fatal("skipped unscanned gap")
	}
	if firstUncovered(100, 110, rr) != 111 {
		t.Fatal("split replay not treated as covered")
	}
	if firstUncovered(120, 125, rr) != 131 {
		t.Fatal("covering interval lost")
	}
}

func TestAcrossFailedRevisionDoesNotMutateInMemoryCanonicalState(t *testing.T) {
	m := runnerManifest(t)
	chain, _ := m.Chain(8453)
	b, a := coreBatch(t)
	store := &coreStore{caps: []Capture{b.Capture}, batches: map[string]Batch{b.Capture.CaptureId: b}, fail: true}
	c := Collector{Manifest: m, Store: store, Archive: a, Readers: map[uint64]*Reader{8453: protocolChainReader(t, chain, nil)}}
	e := c.Reconcile(context.Background(), 8453)
	if e == nil || !strings.Contains(e.Error(), "across_write_frozen_capture_") {
		t.Fatal("revision failure not fatal", e)
	}
	if len(c.captures) != 1 || !c.captures[0].Canonical || c.captures[0].Revision != 1 {
		t.Fatal("memory changed before DB commit")
	}
	store.fail = false
	if e = c.Reconcile(context.Background(), 8453); e != nil {
		t.Fatal(e)
	}
	if c.captures[0].Canonical || c.captures[0].Finality != "orphaned" || c.captures[0].Revision != 2 {
		t.Fatal("revision retry did not invalidate")
	}
}
