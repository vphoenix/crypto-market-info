package dexarb

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"testing"
	"time"
)

var testUSDC = dex.MustAddress("0x0000000000000000000000000000000000000001")
var testUSDT = dex.MustAddress("0x0000000000000000000000000000000000000002")

func reportFixture(n uint64, delta int64) (dex.Block, []dex.Quote) {
	a := dex.Anchor{ChainID: 1, Number: n, Hash: dex.ObjectHash(n), Manifest: dex.Digest([]byte("m")), Batch: dex.ObjectHash(n + 10000), Payload: dex.Digest([]byte("p")), Time: time.Unix(int64(n*12), 0).UTC()}
	b := dex.Block{Anchor: a, Parent: dex.ObjectHash(n - 1), Canonical: true, Committed: true, Finality: "finalized", Capture: "live", QuoteCoverage: "complete", BaseFee: big.NewInt(10), AvailableAt: a.Time.Add(time.Second)}
	qs := []dex.Quote{}
	for _, size := range []int64{100, 200} {
		q := dex.Quote{Anchor: a, Role: "strategy", Route: "r", Mode: "exact_in", TokenIn: testUSDC, TokenOut: testUSDC, Requested: big.NewInt(size), AmountIn: big.NewInt(size), AmountOut: big.NewInt(size + delta), Status: "ok"}
		q.SetID()
		qs = append(qs, q)
	}
	q := dex.Quote{Anchor: a, Role: "capital_entry", Mode: "exact_in", TokenIn: testUSDT, TokenOut: testUSDC, Requested: big.NewInt(1000), AmountIn: big.NewInt(1000), AmountOut: big.NewInt(999), Status: "ok"}
	q.SetID()
	qs = append(qs, q)
	b.ActualQuotes = 3
	b.QuoteMembers = dex.QuoteDigest(qs)
	return b, qs
}
func analyzeFixture(bs []dex.Block, qs map[dex.Hash][]dex.Quote) Report {
	return Analyze(bs, qs, []Route{{ID: "r"}}, []*big.Int{big.NewInt(100), big.NewInt(200)}, testUSDC, testUSDT, ReportOptions{CapitalUSDT: big.NewInt(1000)})
}
func TestWindowsDoNotCountBlocksSizesOrOrdinaryChanges(t *testing.T) {
	var bs []dex.Block
	qs := map[dex.Hash][]dex.Quote{}
	for n := uint64(1); n <= 101; n++ {
		profit := int64(n%7 + 1)
		if n == 1 {
			profit = -1
		}
		b, q := reportFixture(n, profit)
		bs = append(bs, b)
		qs[b.Batch] = q
	}
	report := analyzeFixture(bs, qs)
	if len(report.Windows) != 1 || report.Windows[0].Observations != 100 || !report.Windows[0].ConfirmedNew || report.Routes[0].ConfirmedWindows != 1 {
		t.Fatalf("repeated quotes inflated: %+v", report.Routes)
	}
	if report.Windows[0].Best.Gross.Int64() != 7 {
		t.Fatal("profits summed")
	}
}
func TestWindowsGapPartialBudgetAndRestartContinuity(t *testing.T) {
	var bs []dex.Block
	qs := map[dex.Hash][]dex.Quote{}
	for _, n := range []uint64{1, 2, 4, 5, 6} {
		profit := int64(1)
		if n == 1 || n == 5 {
			profit = -1
		}
		b, q := reportFixture(n, profit)
		bs = append(bs, b)
		qs[b.Batch] = q
	}
	r := analyzeFixture(bs, qs)
	if len(r.Windows) != 3 || r.Routes[0].ConfirmedWindows != 2 || !r.Windows[1].ContinuityUnknown {
		t.Fatalf("gap counted as independent opportunity: %+v", r.Routes)
	}
	b, q := reportFixture(7, 2)
	q[2].Requested = big.NewInt(9999)
	q[2].SetID()
	b.QuoteMembers = dex.QuoteDigest(q)
	bs = append(bs, b)
	qs[b.Batch] = q
	r = analyzeFixture(bs, qs)
	if r.Routes[0].CompleteBlocks != 5 {
		t.Fatal("missing capital reference counted complete")
	}
	// Missing fact rows despite complete marker must not create a candidate.
	b, q = reportFixture(8, 9)
	bs = append(bs, b)
	qs[b.Batch] = q[:2]
	r = analyzeFixture(bs, qs)
	if r.Routes[0].ConfirmedWindows != 2 {
		t.Fatal("incomplete member set counted")
	}
}

type costFake struct {
	requests []dex.SwapRequest
	hashes   []dex.Hash
	fail     bool
	checks   int
}

func (f *costFake) Canonical(context.Context, dex.Block) error { f.checks++; return nil }
func (f *costFake) Quotes(_ context.Context, h dex.Hash, rr []dex.SwapRequest) []dex.SwapResult {
	out := []dex.SwapResult{}
	for _, r := range rr {
		f.requests = append(f.requests, r)
		f.hashes = append(f.hashes, h)
		v := dex.SwapResult{Amount: big.NewInt(1000), Payload: dex.Digest([]byte("cost"))}
		if r.ExactOutput {
			v.Amount = big.NewInt(5)
		}
		if f.fail {
			v.Err = errors.New("historical_state_unavailable")
		}
		out = append(out, v)
	}
	return out
}
func TestCostsExactOutputReserveAndMissingHistory(t *testing.T) {
	b, q := reportFixture(1, 20)
	w := Window{Best: Candidate{Block: b, Quote: q[0], Gross: big.NewInt(20)}}
	s := CostScenario{GasUnits: 400000, PriorityWei: big.NewInt(1), OrderingUSDC: big.NewInt(2), OtherUSDC: big.NewInt(0), CapitalUSDT: big.NewInt(1000)}
	f := &costFake{}
	r := QuoteCosts(context.Background(), f, w, s, testUSDC, testUSDT, dex.Address{})
	if r.Status != "scenario_positive" || r.NetUSDC.Int64() != 13 {
		t.Fatal(r)
	}
	req := f.requests[1]
	if !req.ExactOutput || req.TokenIn != testUSDC || req.Amount.Int64() != 4_400_000 || f.checks+len(f.requests) > 4 {
		t.Fatal("wrong gas direction or amount", req)
	}
	for _, h := range f.hashes {
		if h != b.Hash {
			t.Fatal("different block")
		}
	}
	f = &costFake{fail: true}
	r = QuoteCosts(context.Background(), f, w, s, testUSDC, testUSDT, dex.Address{})
	if r.Status != "unknown" || len(f.requests) != 2 {
		t.Fatal("failed historical quote reused or retried at latest")
	}
	s.OrderingUSDC = big.NewInt(2000)
	r = QuoteCosts(context.Background(), &costFake{}, w, s, testUSDC, testUSDT, dex.Address{})
	if r.BudgetOK || r.Status != "unknown" {
		t.Fatal("gas/payment reserve ignored")
	}
}
func TestDecimalAtomsNeverRound(t *testing.T) {
	for _, s := range []string{"0.0000001", "NaN", "1e6", "-1", "1.2.3"} {
		if _, e := ParseAtoms(s, 6); e == nil {
			t.Fatal(s)
		}
	}
	n, e := ParseAtoms("1000000.123456", 6)
	if e != nil || Units(n, 6) != "1000000.123456" {
		t.Fatal(n, e)
	}
}
func TestCostsChooseSmallerSizeAfterReservingGas(t *testing.T) {
	b, q := reportFixture(1, 20)
	large := Candidate{Block: b, Quote: q[0], Gross: big.NewInt(50)}
	large.Quote.Requested = big.NewInt(1000)
	small := Candidate{Block: b, Quote: q[1], Gross: big.NewInt(20)}
	small.Quote.Requested = big.NewInt(500)
	w := Window{Best: large, Alternatives: []Candidate{large, small}}
	s := CostScenario{GasUnits: 400000, PriorityWei: big.NewInt(1), OrderingUSDC: new(big.Int), OtherUSDC: new(big.Int), CapitalUSDT: big.NewInt(1000)}
	r := QuoteCosts(context.Background(), &costFake{}, w, s, testUSDC, testUSDT, dex.Address{})
	if r.Status != "scenario_positive" || r.SelectedAmountUSDC.Int64() != 500 || r.NetUSDC.Int64() != 15 {
		t.Fatal("smaller funded candidate lost", r)
	}
}
func TestReportCostCacheUsesBlockAndExactParameters(t *testing.T) {
	rpc := &costFake{}
	cache := &CostCache{RPC: rpc}
	h := dex.Digest([]byte("block one"))
	req := dex.SwapRequest{TokenIn: testUSDC, TokenOut: testUSDT, Amount: big.NewInt(100), Fee: 100}
	for i := 0; i < 2; i++ {
		out := cache.Quotes(context.Background(), h, []dex.SwapRequest{req})
		if out[0].Amount == nil {
			t.Fatal("missing cached amount")
		}
	}
	if len(rpc.requests) != 1 {
		t.Fatal("same report request not cached")
	}
	changed := req
	changed.Amount = big.NewInt(101)
	cache.Quotes(context.Background(), h, []dex.SwapRequest{changed})
	cache.Quotes(context.Background(), dex.Digest([]byte("block two")), []dex.SwapRequest{req})
	if len(rpc.requests) != 3 {
		t.Fatal("cache reused different amount or block")
	}
	b := dex.Block{Anchor: dex.Anchor{Hash: h}}
	for i := 0; i < 2; i++ {
		if e := cache.Canonical(context.Background(), b); e != nil {
			t.Fatal(e)
		}
	}
	if rpc.checks != 2 {
		t.Fatal("mutable canonical membership cached")
	}
}
