package lst

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type reportMemoryStore struct {
	batches []Batch
	extra   []Capture
}

func (s *reportMemoryStore) WriteLST(context.Context, Batch) error {
	return errors.New("read_only_test_store")
}
func (s *reportMemoryStore) WriteLSTRevision(context.Context, Capture) error {
	return errors.New("read_only_test_store")
}
func (s *reportMemoryStore) LSTCaptures(context.Context, string) ([]Capture, error) {
	out := append([]Capture(nil), s.extra...)
	for _, b := range s.batches {
		out = append(out, b.Capture)
	}
	return out, nil
}
func (s *reportMemoryStore) LSTBatch(_ context.Context, c Capture) (Batch, error) {
	for _, b := range s.batches {
		if b.Capture.CaptureId == c.CaptureId {
			b.Capture = c
			return b, nil
		}
	}
	return Batch{}, errors.New("missing")
}
func reportReadCSV(t *testing.T, dir, name string) [][]string {
	t.Helper()
	f, e := os.Open(filepath.Join(dir, name))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	rows, e := csv.NewReader(f).ReadAll()
	if e != nil {
		t.Fatal(e)
	}
	return rows
}
func reportColumn(rows [][]string, name string) int {
	for i, s := range rows[0] {
		if s == name {
			return i
		}
	}
	panic(name)
}
func reportGoodMarket(t *testing.T) Batch {
	t.Helper()
	b := lstBatchFixture()
	now := b.Capture.StartedAt
	c := &b.Capture
	c.Finality = "finalized"
	c.Status = "complete"
	c.ChainId = Ptr(uint64(1))
	c.FromBlock = Ptr(uint64(10))
	c.ToBlock = Ptr(uint64(10))
	c.FromBlockHash = Ptr(c.ManifestHash)
	c.ToBlockHash = Ptr(c.ManifestHash)
	c.FromBlockTime = Ptr(now)
	c.ToBlockTime = Ptr(now)
	p := &b.Protocols[0]
	p.StateStatus = "ok"
	p.IdentityOk = true
	p.BlockNumber = Ptr(uint64(10))
	p.BlockHash = Ptr(c.ManifestHash)
	p.BlockTime = Ptr(now)
	p.RequestedAt = Ptr(now)
	p.ReceivedAt = Ptr(now)
	p.SourceAvailableAt = Ptr(now)
	p.PayloadHashes = []string{c.ManifestHash}
	q := &b.Quotes[0]
	q.IsFollowupSeed = true
	q.BuyStatus = "ok"
	q.ConversionStatus = "ok"
	q.ExitStatus = "ok"
	q.HedgeStatus = "ok"
	q.TimingStatus = "fresh"
	q.BuyWethOutWei = new(big.Int).Mul(big.NewInt(50), big.NewInt(1000000000000000000))
	q.BuyLstOutRaw = clone(q.BuyWethOutWei)
	q.RequestStethWei = clone(q.BuyLstOutRaw)
	q.RequestSharesRaw = clone(q.BuyLstOutRaw)
	q.RequestParts = Ptr(uint32(1))
	q.NominalRedeemEthWei = clone(q.BuyWethOutWei)
	q.EthExitInputWei = clone(q.NominalRedeemEthWei)
	q.EthExitUsdtOutRaw = big.NewInt(100200000000)
	q.ChainRequestedAt = Ptr(now)
	q.ChainReceivedAt = Ptr(now)
	q.ChainAvailableAt = Ptr(now)
	q.ChainPayloadHashes = []string{c.ManifestHash}
	q.HedgeQuantityLot = Ptr(int64(50000))
	q.HedgeEthWei = clone(q.NominalRedeemEthWei)
	q.UnhedgedEthResidualWei = big.NewInt(0)
	q.HedgeSellNotionalUsdtE8 = big.NewInt(15000000000000)
	q.HedgeBuyNotionalUsdtE8 = clone(q.HedgeSellNotionalUsdtE8)
	q.HedgeDepthPayloadHash = Ptr(c.ManifestHash)
	q.HedgeDepthRequestedAt = Ptr(now)
	q.HedgeDepthReceivedAt = Ptr(now)
	q.HedgeDepthAvailableAt = Ptr(now)
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	return b
}
func reportFollow(t *testing.T, entry Batch) Batch {
	t.Helper()
	b := reportGoodMarket(t)
	now := entry.Capture.StartedAt.Add(24 * time.Hour)
	c := &b.Capture
	c.StartedAt = now
	c.AvailableAt = now
	c.FromBlockTime = Ptr(now)
	c.ToBlockTime = Ptr(now)
	p := &b.Protocols[0]
	p.ObservedAt = now
	p.AvailableAt = now
	p.BlockTime = Ptr(now)
	p.RequestedAt = Ptr(now)
	p.ReceivedAt = Ptr(now)
	p.SourceAvailableAt = Ptr(now)
	q := &b.Quotes[0]
	q.QuoteId = CanonicalHash("followup")
	q.QuoteRole = "followup"
	q.ReferenceQuoteId = Ptr(entry.Quotes[0].QuoteId)
	q.IsFollowupSeed = false
	q.TargetDelaySeconds = Ptr(uint32(86400))
	q.PlannedForAt = Ptr(now)
	q.ObservedAt = now
	q.AvailableAt = now
	q.PurchaseBudgetUsdtRaw = nil
	q.BuyWethOutWei = nil
	q.BuyLstOutRaw = nil
	q.BuyStatus = "unknown"
	q.ConversionStatus = "unknown"
	q.ChainRequestedAt = Ptr(now)
	q.ChainReceivedAt = Ptr(now)
	q.ChainAvailableAt = Ptr(now)
	q.HedgeDepthRequestedAt = Ptr(now)
	q.HedgeDepthReceivedAt = Ptr(now)
	q.HedgeDepthAvailableAt = Ptr(now)
	q.EthExitUsdtOutRaw = big.NewInt(101500000000)
	q.HedgeBuyNotionalUsdtE8 = big.NewInt(15100000000000)
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	return b
}
func TestReportExactGrossAndUnknownCosts(t *testing.T) {
	entry := reportGoodMarket(t)
	follow := reportFollow(t, entry)
	s := &reportMemoryStore{batches: []Batch{entry, follow}}
	dir := t.TempDir()
	if e := Report(context.Background(), s, Manifest{Hash: entry.Capture.ManifestHash}, entry.Capture.StartedAt.Add(-time.Hour), follow.Capture.StartedAt.Add(time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	rows := reportReadCSV(t, dir, "quotes.csv")
	if rows[1][reportColumn(rows, "gross_discount_usdt")] != "200" || rows[1][reportColumn(rows, "quote_id")] != Hex(entry.Quotes[0].QuoteId) {
		t.Fatal("lost exact gross/binary encoding", rows)
	}
	scenarios := reportReadCSV(t, dir, "scenarios.csv")
	if len(scenarios) != 2 || scenarios[1][reportColumn(scenarios, "gross_cashflow_before_funding_and_costs_usdt")] != "500" {
		t.Fatal(scenarios)
	}
	for _, col := range []string{"net_profit_usdt", "annualized_return", "observed_partial_funding_cashflow_usdt"} {
		if scenarios[1][reportColumn(scenarios, col)] != "" {
			t.Fatal("unknown became zero or profit", col)
		}
	}
	if _, e := os.Stat(filepath.Join(dir, "summary.json")); e != nil {
		t.Fatal(e)
	}
	// A changed ETH amount cannot use the old fixed-quantity exit quote.
	follow.Quotes[0].EthExitInputWei = new(big.Int).Sub(follow.Quotes[0].EthExitInputWei, big.NewInt(1))
	if e := follow.Seal(); e != nil {
		t.Fatal(e)
	}
	s.batches[1] = follow
	if e := Report(context.Background(), s, Manifest{Hash: entry.Capture.ManifestHash}, entry.Capture.StartedAt.Add(-time.Hour), follow.Capture.StartedAt.Add(time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	scenarios = reportReadCSV(t, dir, "scenarios.csv")
	if scenarios[1][reportColumn(scenarios, "gross_cashflow_before_funding_and_costs_usdt")] != "" {
		t.Fatal("mismatched exposure used")
	}
}
func TestReportLatestRevisionBeforeCanonicalFiltering(t *testing.T) {
	b := reportGoodMarket(t)
	orphan := b.Capture
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	s := &reportMemoryStore{batches: []Batch{b}, extra: []Capture{orphan}}
	dir := t.TempDir()
	if e := Report(context.Background(), s, Manifest{Hash: b.Capture.ManifestHash}, b.Capture.StartedAt.Add(-time.Hour), b.Capture.StartedAt.Add(time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	rows := reportReadCSV(t, dir, "quotes.csv")
	if len(rows) != 1 {
		t.Fatal("old canonical revision revived")
	}
	coverage := reportReadCSV(t, dir, "coverage.csv")
	if len(coverage) != 2 || coverage[1][7] != "orphaned" {
		t.Fatal(coverage)
	}
}
func reportLogFixture(t *testing.T) Batch {
	t.Helper()
	b := lstBatchFixture()
	b.Protocols = nil
	b.Quotes = nil
	c := &b.Capture
	c.CaptureKind = "logs"
	c.Status = "complete"
	c.Finality = "finalized"
	c.ExpectedProtocolRows = 0
	c.ExpectedQuoteRows = 0
	c.ChainId = Ptr(uint64(1))
	c.FromBlock = Ptr(uint64(10))
	c.ToBlock = Ptr(uint64(15))
	c.FromBlockHash = Ptr(c.ManifestHash)
	c.ToBlockHash = Ptr(c.ManifestHash)
	c.FromBlockTime = Ptr(c.StartedAt)
	c.ToBlockTime = Ptr(c.StartedAt.Add(4 * time.Hour))
	c.AvailableAt = c.StartedAt.Add(5 * time.Hour)
	r := WithdrawalRequest{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: strings.Repeat("q", 20), BlockNumber: 10, BlockHash: c.ManifestHash, BlockTime: c.StartedAt, TransactionHash: CanonicalHash("batch request transaction"), AbiVersion: "fixture", RequestId: big.NewInt(1), Sender: strings.Repeat("s", 20), InitialOwner: strings.Repeat("o", 20), AmountStethWei: big.NewInt(1000), AmountSharesRaw: big.NewInt(999), EventPayloadHash: c.ManifestHash, AvailableAt: c.AvailableAt, GasUsed: Ptr(uint64(5)), EffectiveGasPriceWei: big.NewInt(10), GasSampleClass: "direct_batch"}
	r2 := r
	r2.RequestId = big.NewInt(2)
	r2.LogIndex = 1
	b.Requests = []WithdrawalRequest{r, r2}
	b.Finalizations = []WithdrawalFinalization{{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: r.QueueAddress, BlockNumber: 12, BlockHash: c.ManifestHash, BlockTime: c.StartedAt.Add(time.Hour), TransactionHash: CanonicalHash("final"), AbiVersion: "fixture", FromRequestId: big.NewInt(1), ToRequestId: big.NewInt(1), EthLockedWei: big.NewInt(999), SharesToBurnRaw: big.NewInt(999), EventTimestamp: c.StartedAt.Add(time.Hour), EventPayloadHash: c.ManifestHash, AvailableAt: c.AvailableAt}}
	claim := WithdrawalClaim{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: r.QueueAddress, BlockNumber: 13, BlockHash: c.ManifestHash, BlockTime: c.StartedAt.Add(2 * time.Hour), TransactionHash: CanonicalHash("claim"), AbiVersion: "fixture", RequestId: big.NewInt(1), Owner: r.InitialOwner, Receiver: r.InitialOwner, AmountEthWei: big.NewInt(999), EventPayloadHash: c.ManifestHash, AvailableAt: c.AvailableAt, GasSampleClass: "missing"}
	missing := claim
	missing.RequestId = big.NewInt(9)
	missing.LogIndex = 1
	b.Claims = []WithdrawalClaim{claim, missing}
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	return b
}
func TestReportWithdrawalBoundariesCensoringAndUniqueTransactionGas(t *testing.T) {
	b := reportLogFixture(t)
	s := &reportMemoryStore{batches: []Batch{b}}
	dir := t.TempDir()
	if e := Report(context.Background(), s, Manifest{Hash: b.Capture.ManifestHash}, b.Capture.StartedAt.Add(-time.Hour), b.Capture.StartedAt.Add(6*time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	rows := reportReadCSV(t, dir, "withdrawals.csv")
	if len(rows) != 4 {
		t.Fatal(rows)
	}
	if rows[1][9] != "3600" || rows[1][10] != "3600" || rows[1][11] != "7200" || rows[1][13] != "claimed" || rows[1][14] != "999/1000" {
		t.Fatal("waiting/claim segments wrong", rows[1])
	}
	if rows[2][13] != "right_censored_coverage_unknown" || rows[3][13] != "left_censored_request_missing" {
		t.Fatal("censoring lost", rows)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "summary.json"))
	if e != nil {
		t.Fatal(e)
	}
	var summary reportSummary
	if e = json.Unmarshal(raw, &summary); e != nil {
		t.Fatal(e)
	}
	if summary.GasSampleTransactions != 1 || summary.GasSampleTotalWei != "50" || summary.RequestCohort != 2 || summary.MatchedFinalizations != 1 {
		t.Fatal(summary)
	}
}
func TestReportFundingUsesActualMarkAndKeepsCoverageUnknown(t *testing.T) {
	entry := reportGoodMarket(t)
	follow := reportFollow(t, entry)
	fund := lstBatchFixture()
	fund.Protocols = nil
	fund.Quotes = nil
	c := &fund.Capture
	c.CaptureKind = "funding"
	c.Finality = "not_applicable"
	c.ExpectedProtocolRows = 0
	c.ExpectedQuoteRows = 0
	c.Status = "complete"
	c.WindowFromAt = Ptr(entry.Capture.StartedAt)
	c.WindowToAt = Ptr(follow.Capture.StartedAt)
	c.AvailableAt = follow.Capture.StartedAt
	f := FundingSettlement{CaptureId: c.CaptureId, InstrumentId: 1, FundingTime: entry.Capture.StartedAt.Add(time.Hour), FundingRate: decimal.RequireFromString("-0.0001"), SettlementMarkPriceTickE8: Ptr(int64(300000000000)), SourceId: c.SourceId, RequestedAt: follow.Capture.StartedAt, ReceivedAt: follow.Capture.StartedAt, AvailableAt: follow.Capture.StartedAt, SourcePayloadHash: c.ManifestHash}
	fund.Funding = []FundingSettlement{f}
	if e := fund.Seal(); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	if e := Report(context.Background(), &reportMemoryStore{batches: []Batch{entry, follow, fund}}, Manifest{Hash: c.ManifestHash}, entry.Capture.StartedAt.Add(-time.Hour), follow.Capture.StartedAt.Add(time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	rows := reportReadCSV(t, dir, "funding.csv")
	if rows[1][reportColumn(rows, "short_cashflow_per_eth_usdt")] != "-0.3" {
		t.Fatal(rows)
	}
	rows = reportReadCSV(t, dir, "scenarios.csv")
	if rows[1][reportColumn(rows, "observed_partial_funding_cashflow_usdt")] != "-15" || rows[1][reportColumn(rows, "net_profit_usdt")] != "" {
		t.Fatal("wrong mark/sign or incomplete cost certified", rows)
	}
}

func TestReportFinalizationIncludesBothSourceEndpoints(t *testing.T) {
	b := reportLogFixture(t)
	b.Finalizations[0].FromRequestId = big.NewInt(1)
	b.Finalizations[0].ToRequestId = big.NewInt(2)
	third := b.Requests[1]
	third.RequestId = big.NewInt(3)
	third.LogIndex = 2
	b.Requests = append(b.Requests, third)
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	if e := Report(context.Background(), &reportMemoryStore{batches: []Batch{b}}, Manifest{Hash: b.Capture.ManifestHash}, b.Capture.StartedAt.Add(-time.Hour), b.Capture.StartedAt.Add(6*time.Hour), dir); e != nil {
		t.Fatal(e)
	}
	rows := reportReadCSV(t, dir, "withdrawals.csv")
	status := map[string]string{}
	for _, row := range rows[1:] {
		status[row[2]] = row[13]
	}
	if status["1"] != "claimed" || status["2"] != "finalized_unclaimed" || status["3"] != "right_censored_coverage_unknown" {
		t.Fatal("source [from,to] endpoints not honored", status)
	}
}
