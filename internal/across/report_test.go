package across

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func reportTestManifest() Manifest {
	return Manifest{Hash: ID("report-test-manifest"), Chains: []ChainConfig{{ChainID: 8453, SpokePool: strings.Repeat("b", 20), USDC: strings.Repeat("u", 20)}, {ChainID: 42161, SpokePool: strings.Repeat("a", 20), USDC: strings.Repeat("v", 20)}}}
}
func reportTestDeposit(id int64) Deposit {
	m := reportTestManifest()
	at := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	d := Deposit{CaptureId: ID("capture"), ChainId: 8453, SpokePool: m.Chains[0].SpokePool, BlockNumber: 7, BlockHash: ID("block"), BlockTime: at, TxHash: ID(id), LogIndex: uint32(id), AbiRevision: "fixture", DestinationChainId: 42161, DepositId: big.NewInt(id), Depositor: strings.Repeat("d", 32), Recipient: strings.Repeat("r", 32), ExclusiveRelayer: strings.Repeat("\x00", 32), InputToken: reportToken32(m.Chains[0].USDC), OutputToken: reportToken32(m.Chains[1].USDC), InputAmountRaw: big.NewInt(100000000), OutputAmountRaw: big.NewInt(99000000), QuoteTimestamp: uint32(at.Unix() - 1), FillDeadline: uint32(at.Unix() + 60), MessageHash: strings.Repeat("\x00", 32), RelayHash: ID(id + 100), AvailableAt: at.Add(time.Second), PayloadHash: ID("payload")}
	return d
}
func reportTestFill(d Deposit) Fill {
	return Fill{CaptureId: ID("fill-capture"), ChainId: d.DestinationChainId, SpokePool: reportTestManifest().Chains[1].SpokePool, BlockNumber: 9, BlockHash: ID("destination-block"), BlockTime: d.BlockTime.Add(2 * time.Second), TxHash: ID("fill-tx"), AbiRevision: "fixture", OriginChainId: d.ChainId, DepositId: new(big.Int).Set(d.DepositId), InputToken: d.InputToken, OutputToken: d.OutputToken, InputAmountRaw: new(big.Int).Set(d.InputAmountRaw), OutputAmountRaw: new(big.Int).Set(d.OutputAmountRaw), Depositor: d.Depositor, Recipient: d.Recipient, ExclusiveRelayer: d.ExclusiveRelayer, FillDeadline: d.FillDeadline, ExclusivityDeadline: d.ExclusivityDeadline, MessageHash: d.MessageHash, RepaymentChainId: d.ChainId, RepaymentAddress: strings.Repeat("p", 32), UpdatedRecipient: d.Recipient, UpdatedMessageHash: d.MessageHash, UpdatedOutputAmountRaw: big.NewInt(97000000), FillType: 0, AvailableAt: d.AvailableAt.Add(2 * time.Second), PayloadHash: ID("fill-payload")}
}
func reportTestSummary() reportSummary {
	return reportSummary{Counts: map[string]uint64{}, Daily: map[string]*reportTotals{}, Directions: map[string]*reportTotals{}, RepaymentAddresses: map[string]uint64{}, OriginalTermsCeiling: "0.000000", ObservedFastCeiling: "0.000000", LiveOpenCeiling: "0.000000"}
}

func TestReportCeilingsUseUpdatedPaymentAndDoNotCancelPositiveOrders(t *testing.T) {
	d := reportTestDeposit(1)
	negative := reportTestDeposit(2)
	negative.OutputAmountRaw = big.NewInt(110000000)
	fill := reportTestFill(d)
	f := newReportFacts()
	if e := f.add(Batch{Capture: Capture{CaptureMode: "backfill"}, Deposits: []Deposit{d, negative}, Fills: []Fill{fill}}); e != nil {
		t.Fatal(e)
	}
	s := reportTestSummary()
	rows, e := reportOrders(&f, reportTestManifest(), d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s)
	if e != nil {
		t.Fatal(e)
	}
	if len(rows) != 2 || s.OriginalTermsCeiling != "1.000000" || s.ObservedFastCeiling != "3.000000" || s.Counts["matched_fast_fill_orders"] != 1 {
		t.Fatal(rows, s)
	}
	// An updated nonempty message is outside the ordinary empty-message scope.
	fill.UpdatedMessageHash = ID("nonempty")
	f.fills = map[string]Fill{"fill": fill}
	s = reportTestSummary()
	if _, e = reportOrders(&f, reportTestManifest(), d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s); e != nil {
		t.Fatal(e)
	}
	if s.ObservedFastCeiling != "0.000000" {
		t.Fatal("nonempty execution counted", s)
	}
	fill.UpdatedMessageHash = d.MessageHash
	fill.FillType = 2
	f.fills = map[string]Fill{"fill": fill}
	s = reportTestSummary()
	if _, e = reportOrders(&f, reportTestManifest(), d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s); e != nil {
		t.Fatal(e)
	}
	if s.ObservedFastCeiling != "0.000000" {
		t.Fatal("slow fill counted", s)
	}
}

func TestReportImmutableConflictAndEarliestCanonicalLiveVisibility(t *testing.T) {
	d := reportTestDeposit(1)
	f := newReportFacts()
	d.AvailableAt = d.BlockTime.Add(10 * time.Second)
	if e := f.add(Batch{Capture: Capture{CaptureMode: "backfill"}, Deposits: []Deposit{d}}); e != nil {
		t.Fatal(e)
	}
	if len(f.firstSeen) != 0 {
		t.Fatal("backfill invented live time")
	}
	live := d
	live.CaptureId = ID("independent-live")
	live.AbiRevision = "different decoder label"
	live.PayloadHash = ID("different RPC formatting")
	live.AvailableAt = d.BlockTime.Add(20 * time.Second)
	live.LiveReceivedAt = Ptr(live.AvailableAt.Add(-time.Millisecond))
	if e := f.add(Batch{Capture: Capture{CaptureMode: "live"}, Deposits: []Deposit{live}}); e != nil {
		t.Fatal(e)
	}
	second := live
	second.AvailableAt = live.AvailableAt.Add(time.Second)
	if e := f.add(Batch{Capture: Capture{CaptureMode: "live"}, Deposits: []Deposit{second}}); e != nil {
		t.Fatal(e)
	}
	if !f.firstSeen[reportOrderKey(d)].Equal(live.AvailableAt) {
		t.Fatal("backfill or later live replaced first seen")
	}
	bad := live
	bad.InputAmountRaw = new(big.Int).Add(bad.InputAmountRaw, big.NewInt(1))
	if e := f.add(Batch{Deposits: []Deposit{bad}}); e == nil {
		t.Fatal("on-chain content conflict accepted")
	}
	fill := reportTestFill(d)
	fill.InputAmountRaw = new(big.Int).Add(fill.InputAmountRaw, big.NewInt(1))
	if reportFillMatches(d, fill) {
		t.Fatal("same deposit id matched different full terms")
	}
}

func TestReportProbeExclusiveAndDeadlineBoundariesAndActualDelay(t *testing.T) {
	d := reportTestDeposit(1)
	d.ExclusiveRelayer = strings.Repeat("e", 32)
	d.ExclusivityDeadline = 100
	d.FillDeadline = 200
	p := OrderProbe{OriginChainId: d.ChainId, OriginSpokePool: d.SpokePool, OriginBlockHash: d.BlockHash, RelayHash: d.RelayHash, ChainId: d.DestinationChainId, DepositId: d.DepositId, ObservationOrigin: "live", ProbeStatus: "ok", ContractTime: Ptr(uint64(100)), FillStatus: Ptr(uint8(0)), PausedFills: Ptr(false), BlockHash: Ptr(ID("destination")), RequestedAt: d.AvailableAt, PlannedForAt: d.AvailableAt, AvailableAt: d.AvailableAt}
	if got := reportProbeState(d, p); got != "exclusive" {
		t.Fatal(got)
	}
	p.ContractTime = Ptr(uint64(101))
	p.FillStatus = Ptr(uint8(1))
	if got := reportProbeState(d, p); got != "open" {
		t.Fatal(got)
	}
	p.ContractTime = Ptr(uint64(200))
	if got := reportProbeState(d, p); got != "open" {
		t.Fatal(got)
	}
	p.ContractTime = Ptr(uint64(201))
	if got := reportProbeState(d, p); got != "expired" {
		t.Fatal(got)
	}
	p.ContractTime = Ptr(uint64(150))
	p.TargetDelayMs = Ptr(uint32(2000))
	p.AvailableAt = p.PlannedForAt.Add(time.Second + time.Microsecond)
	if got := reportProbeState(d, p); got != "unknown_late" {
		t.Fatal(got)
	}
	p.AvailableAt = p.PlannedForAt
	p.FillStatus = Ptr(uint8(2))
	if got := reportProbeState(d, p); got != "filled" {
		t.Fatal(got)
	}
	p.OriginBlockHash = ID("orphaned-origin")
	if got := reportProbeState(d, p); got != "unmatched_origin" {
		t.Fatal(got)
	}
}

func TestReportReceiptKnownFeeSupplementAndContradiction(t *testing.T) {
	r := TxReceipt{TxHash: ID("tx"), BlockHash: ID("block"), GasUsed: 3, ExecutionFeeWei: big.NewInt(6), TotalFeeWei: nil}
	v := r
	v.CaptureId = ID("new observation")
	v.ReceiptPayloadHash = ID("new formatting")
	v.TotalFeeWei = big.NewInt(8)
	v.L1DataFeeWei = big.NewInt(2)
	v.FeeComplete = true
	v.FeeRule = "known"
	got, e := mergeReportReceipt(r, v)
	if e != nil || got.TotalFeeWei.Cmp(big.NewInt(8)) != 0 || !got.FeeComplete {
		t.Fatal(got, e)
	}
	v.TotalFeeWei = big.NewInt(9)
	if _, e = mergeReportReceipt(got, v); e == nil {
		t.Fatal("conflicting known fee accepted")
	}
}

func TestReportFillIdentityAndWholeTransactionFeeAreExplicit(t *testing.T) {
	d1, d2 := reportTestDeposit(1), reportTestDeposit(2)
	f1, f2 := reportTestFill(d1), reportTestFill(d2)
	f2.LogIndex = 1
	facts := newReportFacts()
	if e := facts.add(Batch{Deposits: []Deposit{d1, d2}, Fills: []Fill{f1, f2}}); e != nil {
		t.Fatal(e)
	}
	receipt := TxReceipt{ChainId: f1.ChainId, BlockHash: f1.BlockHash, TxHash: f1.TxHash, Sender: strings.Repeat("s", 20), TotalFeeWei: big.NewInt(1234), FeeComplete: true}
	facts.receipts[reportReceiptKey(receipt.ChainId, receipt.BlockHash, receipt.TxHash)] = receipt
	s := reportTestSummary()
	rows, e := reportOrders(&facts, reportTestManifest(), d1.BlockTime.Add(-time.Hour), d1.BlockTime.Add(time.Hour), &s)
	if e != nil {
		t.Fatal(e)
	}
	columns := map[string]int{}
	for i, name := range reportOrderHeader {
		columns[name] = i
	}
	if len(rows) != 2 {
		t.Fatal(rows)
	}
	for _, row := range rows {
		if len(row) != len(reportOrderHeader) || row[columns["fill_chain_id"]] != "42161" || row[columns["fill_block_hash"]] != Hex(f1.BlockHash) || row[columns["fill_tx_hash"]] != Hex(f1.TxHash) || row[columns["fill_transaction_fee_wei_shared"]] != "1234" || row[columns["fill_transaction_fee_scope"]] != "whole_transaction_shared_do_not_sum_orders" {
			t.Fatal(row)
		}
	}
	if _, ok := columns["fill_fee_wei"]; ok {
		t.Fatal("ambiguous per-order fee column retained")
	}
}

func TestReportInventoryScenariosAreHypothesesAndKeepChainBalancesSeparate(t *testing.T) {
	r := reportInventoryScenarios()
	if r.CapitalUSDT != "250000.000000" || r.CapitalByChainUSDT["8453"] != "125000.000000" || r.CapitalByChainUSDT["42161"] != "125000.000000" || r.ObservedTurnaroundHours != nil || r.ActualDailyPaymentCapacityUSDC != nil || r.NetProfitUSDT != nil {
		t.Fatal(r)
	}
	wantHours := []uint32{3, 6, 12, 24}
	wantCapacity := []string{"2000000.000000", "1000000.000000", "500000.000000", "250000.000000"}
	wantChain := []string{"1000000.000000", "500000.000000", "250000.000000", "125000.000000"}
	if len(r.Scenarios) != 4 || len(r.Assumptions) == 0 || !strings.Contains(r.Basis, "hypothetical") {
		t.Fatal(r)
	}
	for i, s := range r.Scenarios {
		if s.AssumedTurnaroundHours != wantHours[i] || s.MaximumGrossDailyPaymentCapacityUSDC != wantCapacity[i] || s.MaximumGrossDailyPaymentCapacityByChainUSDC["8453"] != wantChain[i] || s.MaximumGrossDailyPaymentCapacityByChainUSDC["42161"] != wantChain[i] {
			t.Fatal(s)
		}
	}
}

func TestReportRefundVerifiesTransferWithoutInventingOrderMembership(t *testing.T) {
	a := ethereum.Archive{Dir: t.TempDir()}
	m := reportTestManifest()
	d := reportTestDeposit(1)
	recipient := strings.Repeat("r", 20)
	caller := strings.Repeat("c", 20)
	amount := big.NewInt(123000000)
	r := TxReceipt{ChainId: 8453, BlockHash: d.BlockHash, TxHash: ID("refund-tx"), BlockTime: d.BlockTime, Success: true}
	transfer := map[string]any{"address": Hex(m.Chains[0].USDC), "topics": []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", Hex(reportToken32(m.Chains[0].SpokePool)), Hex(reportToken32(recipient))}, "data": Hex(string(amount.FillBytes(make([]byte, 32)))), "logIndex": "0x5", "transactionHash": Hex(r.TxHash), "blockHash": Hex(r.BlockHash), "removed": false}
	response, _ := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"transactionHash": Hex(r.TxHash), "blockHash": Hex(r.BlockHash), "status": "0x1", "logs": []any{transfer}}}})
	h, e := a.PutObject(struct{ Response []byte }{response})
	if e != nil {
		t.Fatal(e)
	}
	r.ReceiptPayloadHash = string(h[:])
	refund := Refund{ChainId: 8453, SpokePool: m.Chains[0].SpokePool, BlockHash: r.BlockHash, TxHash: r.TxHash, BlockTime: d.BlockTime, LogIndex: 10, Token: m.Chains[0].USDC, EventKind: "deferred_claim", Caller: caller, RefundAddresses: []string{recipient}, RefundAmountsRaw: []*big.Int{amount}}
	f := newReportFacts()
	f.refunds["claim"] = refund
	f.receipts[reportReceiptKey(r.ChainId, r.BlockHash, r.TxHash)] = r
	s := reportTestSummary()
	rows := reportRefundRows(&f, a, m, d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s)
	if len(rows) != 1 || rows[0][13] != "verified_transfer" || rows[0][9] != Hex(caller) || rows[0][10] != Hex(recipient) || rows[0][16] != "attribution_unknown" || rows[0][17] != "other_origins_or_user_refunds_possible" {
		t.Fatal(rows)
	}
	// Same payment cannot be used for two same-sized refund entries.
	duplicate := refund
	duplicate.LogIndex++
	f.refunds["duplicate"] = duplicate
	s = reportTestSummary()
	rows = reportRefundRows(&f, a, m, d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s)
	for _, row := range rows {
		if row[13] == "verified_transfer" {
			t.Fatal("one transfer allocated twice", rows)
		}
	}
	delete(f.refunds, "duplicate")
	refund.RefundAddresses = []string{strings.Repeat("x", 20)}
	refund.DeferredRefunds = Ptr(true)
	f.refunds["claim"] = refund
	s = reportTestSummary()
	rows = reportRefundRows(&f, a, m, d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), &s)
	if rows[0][13] != "deferred_or_unmatched" || rows[0][12] != "" {
		t.Fatal("deferred transfer invented", rows)
	}
}

type reportTestStore struct {
	caps    []Capture
	batches map[string]Batch
}

func (s reportTestStore) AcrossCaptures(context.Context, string) ([]Capture, error) {
	return s.caps, nil
}
func (s reportTestStore) AcrossBatch(_ context.Context, c Capture) (Batch, error) {
	b := s.batches[c.CaptureId]
	b.Capture = c
	return b, Validate(b)
}
func (s reportTestStore) WriteAcrossBatch(context.Context, Batch) error      { return nil }
func (s reportTestStore) WriteAcrossRevision(context.Context, Capture) error { return nil }

func TestBuildReportLatestOrphanCannotResurrectAndMissingEvidenceStaysGap(t *testing.T) {
	d := reportTestDeposit(1)
	m := reportTestManifest()
	archive := ethereum.Archive{Dir: t.TempDir()}
	at := d.AvailableAt
	b := Batch{Capture: Capture{ManifestHash: m.Hash, CaptureId: d.CaptureId, ChainId: d.ChainId, CaptureKind: "logs", CaptureMode: "live", FromBlock: &d.BlockNumber, ToBlock: &d.BlockNumber, FromHash: &d.BlockHash, ToHash: &d.BlockHash, StartedAt: at, AvailableAt: at, SourceId: "fixture", Status: "complete", Canonical: true, Finality: "head"}, Deposits: []Deposit{d}}
	if e := ArchiveBatch(archive, &b, []EvidenceHeader{{ChainID: d.ChainId, Number: d.BlockNumber, Hash: Hex(d.BlockHash), Time: d.BlockTime}}, nil); e != nil {
		t.Fatal(e)
	}
	orphan := b.Capture
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	missing := b
	missing.Capture.CaptureId = ID("missing-evidence")
	missing.Capture.EvidenceHash = ID("nonexistent")
	missing.Deposits = nil
	Seal(&missing)
	store := reportTestStore{caps: []Capture{orphan, b.Capture, missing.Capture}, batches: map[string]Batch{b.Capture.CaptureId: b, missing.Capture.CaptureId: missing}}
	out := t.TempDir()
	if e := BuildReport(context.Background(), store, m, archive, d.BlockTime.Add(-time.Hour), d.BlockTime.Add(time.Hour), out); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(out, "summary.json"))
	if e != nil {
		t.Fatal(e)
	}
	var summary reportSummary
	if e = json.Unmarshal(raw, &summary); e != nil {
		t.Fatal(e)
	}
	if summary.Counts["unique_deposits"] != 0 || summary.Counts["missing_capture_evidence"] != 1 || summary.NetProfitUSDT != nil || summary.NegativeConclusionAllowed {
		t.Fatal(string(raw))
	}
	if !strings.Contains(summary.CoverageScope, "ALL saved captures") || !strings.Contains(summary.OrderScope, "deposit cohort") || !strings.Contains(summary.ReceiptAndProbeScope, "probe requested_at") || len(summary.InventorySensitivity.Scenarios) != 4 {
		t.Fatal("report scope or assumed scenarios missing", string(raw))
	}
	f, e := os.Open(filepath.Join(out, "orders.csv"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	rows, e := csv.NewReader(f).ReadAll()
	if e != nil || len(rows) != 1 {
		t.Fatal(rows, e)
	}
}
