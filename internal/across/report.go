package across

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// Report does not connect to an RPC or use current prices to value history. Its
// ceilings are user fee space, before LP fees, execution, inventory and rivalry.
type reportSummary struct {
	ManifestHash                  string                       `json:"manifest_hash"`
	From                          time.Time                    `json:"from_utc"`
	To                            time.Time                    `json:"to_utc"`
	GeneratedAt                   time.Time                    `json:"generated_at_utc"`
	Scope                         string                       `json:"scope"`
	CoverageScope                 string                       `json:"coverage_scope"`
	OrderScope                    string                       `json:"order_scope"`
	ReceiptAndProbeScope          string                       `json:"receipt_and_probe_counts_scope"`
	Counts                        map[string]uint64            `json:"counts"`
	OriginalTermsCeiling          string                       `json:"original_terms_fee_space_usdc"`
	ObservedFastCeiling           string                       `json:"observed_fast_fill_fee_space_usdc"`
	LiveOpenCeiling               string                       `json:"live_observed_open_original_terms_fee_space_usdc"`
	Daily                         map[string]*reportTotals     `json:"utc_days"`
	Directions                    map[string]*reportTotals     `json:"directions"`
	RepaymentAddresses            map[string]uint64            `json:"fast_fills_by_repayment_address"`
	TransactionFeesWei            map[string]string            `json:"unique_observed_transaction_fees_wei_by_chain"`
	InventorySensitivity          reportInventorySensitivity   `json:"inventory_sensitivity"`
	NetProfitUSDT                 *string                      `json:"net_profit_usdt"`
	NegativeConclusionAllowed     bool                         `json:"negative_conclusion_allowed"`
	Limitations                   []string                     `json:"limitations"`
	Performance                   reportPerformance            `json:"performance"`
	LogCoverage                   map[string]reportLogCoverage `json:"log_coverage_all_saved_captures"`
	EligibleCaptureFinalityPolicy string                       `json:"eligible_capture_finality_policy"`
	EligibleCaptureFinalityCounts map[string]uint64            `json:"eligible_capture_finality_counts"`
	RawResponsePolicy             string                       `json:"raw_response_policy"`
}

type reportPerformance struct {
	CaptureMetadataLoadMicros     int64          `json:"capture_metadata_load_microseconds"`
	FactLoad                      BatchLoadStats `json:"fact_load"`
	ArchiveValidationMicros       int64          `json:"archive_validation_microseconds"`
	ArchiveWorkers                uint32         `json:"archive_workers"`
	ArchiveChunkCaptures          uint32         `json:"archive_chunk_captures"`
	CaptureEvidenceChecks         uint64         `json:"capture_evidence_checks"`
	UniqueRPCPayloadChecks        uint64         `json:"unique_rpc_payload_checks"`
	TotalBeforeSummaryWriteMicros int64          `json:"total_before_summary_write_microseconds"`
}

type reportLogRange struct {
	First uint64    `json:"from_block"`
	Last  uint64    `json:"to_block"`
	From  time.Time `json:"from_block_time_utc"`
	To    time.Time `json:"to_block_time_utc"`
}

type reportLogCoverage struct {
	Raw           []reportLogRange `json:"raw_scanned_contiguous_ranges"`
	Decoded       []reportLogRange `json:"fully_decoded_contiguous_ranges"`
	RawBlocks     uint64           `json:"raw_scanned_blocks"`
	DecodedBlocks uint64           `json:"fully_decoded_blocks"`
}

func reportMergeLogRanges(ranges []reportLogRange) []reportLogRange {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].First < ranges[j].First })
	out := []reportLogRange{}
	for _, r := range ranges {
		if len(out) == 0 || r.First > out[len(out)-1].Last && r.First-out[len(out)-1].Last > 1 {
			out = append(out, r)
			continue
		}
		last := &out[len(out)-1]
		if r.Last > last.Last {
			last.Last = r.Last
			last.To = r.To
		}
	}
	return out
}

type reportInventorySensitivity struct {
	Basis                          string                    `json:"basis"`
	CapitalUSDT                    string                    `json:"inventory_budget_usdt"`
	CapitalByChainUSDT             map[string]string         `json:"inventory_budget_by_chain_usdt"`
	AssumedUSDCUSDT                string                    `json:"assumed_usdc_usdt"`
	Assumptions                    []string                  `json:"assumptions"`
	Scenarios                      []reportInventoryScenario `json:"scenarios"`
	ObservedTurnaroundHours        *string                   `json:"observed_turnaround_hours"`
	ActualDailyPaymentCapacityUSDC *string                   `json:"actual_daily_payment_capacity_usdc"`
	NetProfitUSDT                  *string                   `json:"net_profit_usdt"`
}

type reportInventoryScenario struct {
	AssumedTurnaroundHours                      uint32            `json:"assumed_turnaround_hours"`
	MaximumGrossDailyPaymentCapacityUSDC        string            `json:"maximum_gross_daily_payment_capacity_usdc"`
	MaximumGrossDailyPaymentCapacityByChainUSDC map[string]string `json:"maximum_gross_daily_payment_capacity_by_chain_usdc"`
}

// This is declared capital sensitivity, not observed receipt turnover. Each
// chain can reuse principal only after the assumed return interval has elapsed.
func reportInventoryScenarios() reportInventorySensitivity {
	r := reportInventorySensitivity{Basis: "hypothetical fully utilized inventory; not observed capacity or profit", CapitalUSDT: "250000.000000", CapitalByChainUSDT: map[string]string{"8453": "125000.000000", "42161": "125000.000000"}, AssumedUSDCUSDT: "1.000000", Assumptions: []string{"assume USDC/USDT parity solely for this sensitivity; no executable conversion quote", "reserve 125000 USDT-equivalent of USDC on each chain; balances are separate", "each payment's full principal becomes available on the same chain after the assumed interval; return path and timing are unverified", "assume sufficient balanced eligible orders and full utilization; ignore setup, idle time, gas reserves and inventory restoration costs", "pending refunds are not spendable before the assumed return; capacity equals inventory times 24 divided by turnaround hours"}}
	for _, hours := range []uint32{3, 6, 12, 24} {
		perChain := new(big.Int).Quo(big.NewInt(125000*24), new(big.Int).SetUint64(uint64(hours)))
		total := new(big.Int).Mul(new(big.Int).Set(perChain), big.NewInt(2))
		r.Scenarios = append(r.Scenarios, reportInventoryScenario{AssumedTurnaroundHours: hours, MaximumGrossDailyPaymentCapacityUSDC: total.String() + ".000000", MaximumGrossDailyPaymentCapacityByChainUSDC: map[string]string{"8453": perChain.String() + ".000000", "42161": perChain.String() + ".000000"}})
	}
	return r
}

type reportTotals struct {
	Orders               uint64 `json:"orders"`
	InputUSDC            string `json:"input_usdc"`
	OriginalTermsCeiling string `json:"original_terms_fee_space_usdc"`
	ObservedFastCeiling  string `json:"observed_fast_fill_fee_space_usdc"`
	LiveOpenCeiling      string `json:"live_open_original_terms_fee_space_usdc"`
}
type reportFacts struct {
	deposits  map[string]Deposit
	fills     map[string]Fill
	updates   map[string]DepositUpdate
	refunds   map[string]Refund
	receipts  map[string]TxReceipt
	probes    map[string]OrderProbe
	transfers map[string]ReceiptTransfers
	firstSeen map[string]time.Time
}

func newReportFacts() reportFacts {
	return reportFacts{map[string]Deposit{}, map[string]Fill{}, map[string]DepositUpdate{}, map[string]Refund{}, map[string]TxReceipt{}, map[string]OrderProbe{}, map[string]ReceiptTransfers{}, map[string]time.Time{}}
}

// Event comparisons deliberately omit observation metadata, including payload
// formatting and ABI labels. An independent observation is not a chain conflict.
func reportProtocolID(value any) string {
	x := reflect.New(reflect.TypeOf(value)).Elem()
	x.Set(reflect.ValueOf(value))
	for _, name := range []string{"CaptureId", "AbiRevision", "LiveReceivedAt", "AvailableAt", "PayloadHash", "RequestedAt", "TransactionPayloadHash", "ReceiptPayloadHash"} {
		if f := x.FieldByName(name); f.IsValid() {
			f.SetZero()
		}
	}
	if _, ok := value.(TxReceipt); ok {
		for _, name := range []string{"L1DataFeeWei", "OperatorFeeWei", "GasUsedForL1", "TotalFeeWei", "FeeRule", "FeeComplete", "Reason"} {
			x.FieldByName(name).SetZero()
		}
	}
	return ID(x.Interface())
}
func reportEventKey(chain uint64, spoke, block, tx string, index uint32) string {
	return fmt.Sprintf("%d/%s/%s/%s/%d", chain, Hex(spoke), Hex(block), Hex(tx), index)
}
func reportReceiptKey(chain uint64, block, tx string) string {
	return fmt.Sprintf("%d/%s/%s", chain, Hex(block), Hex(tx))
}
func reportOrderKey(d Deposit) string {
	return fmt.Sprintf("%d/%s/%s", d.ChainId, Hex(d.SpokePool), Hex(d.RelayHash))
}
func reportPairKey(chain uint64, id *big.Int) string { return fmt.Sprintf("%d/%s", chain, id.String()) }
func reportPut[T any](m map[string]T, key string, v T) error {
	if old, ok := m[key]; ok {
		if reportProtocolID(old) != reportProtocolID(v) {
			return fmt.Errorf("conflicting_protocol_fact: %s", key)
		}
		return nil
	}
	m[key] = v
	return nil
}
func mergeReportReceipt(old, v TxReceipt) (TxReceipt, error) {
	if reportProtocolID(old) != reportProtocolID(v) {
		return old, errors.New("conflicting_receipt_core")
	}
	merge := func(a **big.Int, b *big.Int) error {
		if *a != nil && b != nil && (*a).Cmp(b) != 0 {
			return errors.New("conflicting_receipt_fee")
		}
		if *a == nil {
			*a = b
		}
		return nil
	}
	for _, p := range []struct {
		a **big.Int
		b *big.Int
	}{{&old.L1DataFeeWei, v.L1DataFeeWei}, {&old.OperatorFeeWei, v.OperatorFeeWei}, {&old.TotalFeeWei, v.TotalFeeWei}} {
		if e := merge(p.a, p.b); e != nil {
			return old, e
		}
	}
	if old.GasUsedForL1 != nil && v.GasUsedForL1 != nil && *old.GasUsedForL1 != *v.GasUsedForL1 {
		return old, errors.New("conflicting_receipt_l1_gas")
	}
	if old.GasUsedForL1 == nil {
		old.GasUsedForL1 = v.GasUsedForL1
	}
	if v.FeeComplete {
		old.FeeComplete = true
		old.FeeRule = v.FeeRule
		old.Reason = v.Reason
		old.ReceiptPayloadHash = v.ReceiptPayloadHash
	}
	return old, nil
}
func (f *reportFacts) add(b Batch) error {
	for _, d := range b.Deposits {
		key := reportEventKey(d.ChainId, d.SpokePool, d.BlockHash, d.TxHash, d.LogIndex)
		if e := reportPut(f.deposits, key, d); e != nil {
			return e
		}
		if b.Capture.CaptureMode == "live" && d.LiveReceivedAt != nil {
			k := reportOrderKey(d)
			if t, ok := f.firstSeen[k]; !ok || d.AvailableAt.Before(t) {
				f.firstSeen[k] = d.AvailableAt
			}
		}
	}
	for _, v := range b.Fills {
		if e := reportPut(f.fills, reportEventKey(v.ChainId, v.SpokePool, v.BlockHash, v.TxHash, v.LogIndex), v); e != nil {
			return e
		}
	}
	for _, v := range b.Updates {
		if e := reportPut(f.updates, reportEventKey(v.ChainId, v.SpokePool, v.BlockHash, v.TxHash, v.LogIndex), v); e != nil {
			return e
		}
	}
	for _, v := range b.Refunds {
		if e := reportPut(f.refunds, reportEventKey(v.ChainId, v.SpokePool, v.BlockHash, v.TxHash, v.LogIndex), v); e != nil {
			return e
		}
	}
	for _, v := range b.Probes {
		if e := reportPut(f.probes, Hex(v.ProbeId), v); e != nil {
			return e
		}
	}
	for _, v := range b.Receipts {
		k := reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)
		if old, ok := f.receipts[k]; ok {
			merged, e := mergeReportReceipt(old, v)
			if e != nil {
				return e
			}
			f.receipts[k] = merged
		} else {
			f.receipts[k] = v
		}
	}
	for _, v := range b.Transfers {
		k := reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash) + "/" + Hex(v.Token)
		if old, exists := f.transfers[k]; exists && reportProtocolID(old) != reportProtocolID(v) {
			return errors.New("conflicting_receipt_transfers")
		}
		f.transfers[k] = v
	}

	return nil
}

func reportTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func reportAmount(n *big.Int) string {
	if n == nil {
		return ""
	}
	return decimal.NewFromBigInt(n, -6).StringFixed(6)
}
func reportPositiveSpread(in, out *big.Int) *big.Int {
	n := new(big.Int).Sub(in, out)
	if n.Sign() < 0 {
		n.SetInt64(0)
	}
	return n
}
func reportAddAmount(s string, n *big.Int) string {
	if s == "" {
		s = "0"
	}
	a, _ := decimal.NewFromString(s)
	return a.Add(decimal.NewFromBigInt(n, -6)).StringFixed(6)
}
func reportNullable[T any](v *T) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(*v)
}
func reportToken32(v string) string { return strings.Repeat("\x00", 12) + v }
func reportRoute(d Deposit, m Manifest) string {
	var origin, dest *ChainConfig
	for i := range m.Chains {
		c := &m.Chains[i]
		if c.ChainID == d.ChainId {
			origin = c
		}
		if c.ChainID == d.DestinationChainId {
			dest = c
		}
	}
	if origin == nil || dest == nil || origin.ChainID == dest.ChainID {
		return "excluded_route"
	}
	if d.SpokePool != origin.SpokePool || d.InputToken != reportToken32(origin.USDC) || d.OutputToken != reportToken32(dest.USDC) {
		return "excluded_token_or_spoke"
	}
	if d.Message != "" || d.MessageHash != strings.Repeat("\x00", 32) {
		return "excluded_message"
	}
	return "eligible_original_empty_terms"
}
func reportFillMatches(d Deposit, f Fill) bool {
	return d.ChainId == f.OriginChainId && d.DestinationChainId == f.ChainId && d.DepositId.Cmp(f.DepositId) == 0 && d.InputToken == f.InputToken && d.OutputToken == f.OutputToken && d.InputAmountRaw.Cmp(f.InputAmountRaw) == 0 && d.OutputAmountRaw.Cmp(f.OutputAmountRaw) == 0 && d.Depositor == f.Depositor && d.Recipient == f.Recipient && d.ExclusiveRelayer == f.ExclusiveRelayer && d.FillDeadline == f.FillDeadline && d.ExclusivityDeadline == f.ExclusivityDeadline && d.MessageHash == f.MessageHash
}

// A successful source-chain observation alone never proves destination openness.
func reportProbeState(d Deposit, p OrderProbe) string {
	if p.OriginChainId != d.ChainId || p.OriginSpokePool != d.SpokePool || p.OriginBlockHash != d.BlockHash || p.RelayHash != d.RelayHash || p.ChainId != d.DestinationChainId || p.DepositId.Cmp(d.DepositId) != 0 {
		return "unmatched_origin"
	}
	if p.ObservationOrigin != "live" {
		return "unknown_" + p.ObservationOrigin
	}
	if p.ProbeStatus != "ok" {
		return "unknown_" + p.ProbeStatus
	}
	if p.ContractTime == nil || p.FillStatus == nil || p.PausedFills == nil || p.BlockHash == nil {
		return "unknown_partial"
	}
	if p.AvailableAt.Before(p.RequestedAt) || p.AvailableAt.Before(p.PlannedForAt) {
		return "unknown_timing"
	}
	if p.TargetDelayMs != nil && p.AvailableAt.After(p.PlannedForAt.Add(time.Second)) {
		return "unknown_late"
	}
	if *p.FillStatus == 2 {
		return "filled"
	}
	if *p.FillStatus > 2 {
		return "unknown_fill_status"
	}
	if *p.PausedFills {
		return "paused"
	}
	if *p.ContractTime > uint64(d.FillDeadline) {
		return "expired"
	}
	if d.ExclusiveRelayer != strings.Repeat("\x00", 32) && *p.ContractTime <= uint64(d.ExclusivityDeadline) {
		return "exclusive"
	}
	return "open"
}

func writeReportCSV(path string, header []string, rows [][]string) error {
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	w := csv.NewWriter(f)
	if e = w.Write(header); e == nil {
		e = w.WriteAll(rows)
	}
	w.Flush()
	if e == nil {
		e = w.Error()
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}

// BuildReport reads immutable saved evidence only. Missing member sets or source
// evidence remain visible coverage failures; neither is silently treated as zero.
func BuildReport(ctx context.Context, store Store, manifest Manifest, archive ethereum.Archive, from, to time.Time, out string) (resultErr error) {
	started := time.Now()
	if from.IsZero() || !to.After(from) {
		return errors.New("invalid_report_interval")
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	progressFile, err := os.OpenFile(filepath.Join(out, "phase-progress.jsonl"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var progressErr error
	progress := func(event LoadProgress) {
		entry := struct {
			At                  time.Time `json:"at_utc"`
			ReportElapsedMicros int64     `json:"report_elapsed_microseconds"`
			LoadProgress
		}{Now(), time.Since(started).Microseconds(), event}
		raw, err := json.Marshal(entry)
		if err != nil {
			progressErr = err
			return
		}
		raw = append(raw, '\n')
		if _, err = progressFile.Write(raw); err != nil {
			progressErr = err
		}
		_, _ = os.Stderr.Write(raw)
	}
	defer func() {
		phase := "report_complete"
		if resultErr != nil {
			phase = "report_failed"
		}
		progress(LoadProgress{Phase: phase, ElapsedMicros: time.Since(started).Microseconds()})
		if err := progressFile.Close(); progressErr == nil {
			progressErr = err
		}
		if resultErr == nil {
			resultErr = progressErr
		}
	}()
	ctx = WithLoadProgress(ctx, progress)
	progress(LoadProgress{Phase: "capture_metadata_started"})
	caps, e := store.AcrossCaptures(ctx, manifest.Hash)
	if e != nil {
		return e
	}
	metadataMicros := time.Since(started).Microseconds()
	progress(LoadProgress{Phase: "capture_metadata_finished", Completed: uint64(len(caps)), ElapsedMicros: metadataMicros})
	// Defend callers/fakes as well as FINAL SQL: latest first, filters second.
	latest, e := latestReadCaptures(caps)
	if e != nil {
		return e
	}
	selected := []Capture{}
	for _, c := range latest {
		if c.Canonical && c.Committed {
			selected = append(selected, c)
		}
	}
	loaded, e := LoadBatches(ctx, store, selected)
	if e != nil {
		return e
	}
	progress(LoadProgress{Phase: "fact_load_finished", Completed: uint64(len(selected)), ElapsedMicros: loaded.Stats.TotalMicros})
	facts := newReportFacts()
	coverage := [][]string{}
	rawRanges := map[uint64][]reportLogRange{}
	decodedRanges := map[uint64][]reportLogRange{}
	archiveValidator := newReportArchiveValidator(archive)
	s := reportSummary{ManifestHash: Hex(manifest.Hash), From: from.UTC(), To: to.UTC(), GeneratedAt: Now(), Scope: "Base-Arbitrum native USDC; original empty-message terms; public polling observations", Counts: map[string]uint64{}, Daily: map[string]*reportTotals{}, Directions: map[string]*reportTotals{}, RepaymentAddresses: map[string]uint64{}, TransactionFeesWei: map[string]string{}, OriginalTermsCeiling: "0.000000", ObservedFastCeiling: "0.000000", LiveOpenCeiling: "0.000000", Limitations: []string{"fee space is before LP, transaction, failure, inventory and operating costs; no verified net profit", "all relayer refunds remain per-order attribution_unknown; no observed capital turnover", "absence of a matched fill remains fill_unknown: destination coverage and prefill history are not proven by an absent event", "unknown ABI, unmatched fills, nonempty messages and updated execution terms prevent a global negative Across conclusion", "CEX prices are reference observations only; historical receipts are not repriced using current quotes", "two-chain capture excludes other origins and user-refund membership; refund transfers are not route income"}}
	s.RawResponsePolicy = "not_retained"
	s.Limitations = append(s.Limitations, "source responses are parsed in memory and not archived; source hashes, database member digests and compact capture anchors remain available")
	s.CoverageScope = "coverage.csv and capture/evidence/unknown-event/log-gap counts use ALL saved captures for this manifest, including outside [from_utc,to_utc); latest revisions are selected before filtering"
	s.OrderScope = "orders.csv, order counts and fee-space totals select deposit block_time in [from_utc,to_utc); matching fills, updates and probes use all saved evidence, including after to_utc; this is a deposit cohort, not an as-of replay"
	s.ReceiptAndProbeScope = "unique receipt fees and receipt counts select receipt block_time in [from_utc,to_utc); probe status/price counts select probe requested_at in that window; refund rows and unmatched-fill counts select their event block_time in that window"
	s.InventorySensitivity = reportInventoryScenarios()
	s.Limitations = append(s.Limitations, "orders.csv fill_transaction_fee_wei_shared is the full shared transaction fee; do not sum this column across orders; summary receipt fees deduplicate chain/block_hash/tx_hash")
	s.Performance.CaptureMetadataLoadMicros = metadataMicros
	s.Performance.FactLoad = loaded.Stats
	s.Performance.ArchiveWorkers = reportArchiveWorkers
	s.Performance.ArchiveChunkCaptures = reportArchiveChunkSize
	s.LogCoverage = map[string]reportLogCoverage{}
	s.EligibleCaptureFinalityPolicy = "canonical_committed_including_head"
	s.EligibleCaptureFinalityCounts = map[string]uint64{}
	s.Limitations = append(s.Limitations, "raw scanned continuity only proves complete log retrieval; fully decoded continuity also requires complete known-ABI interpretation; neither proves coverage before the first stored range or after the last")
	s.Limitations = append(s.Limitations, "accepted evidence includes canonical committed head and safe captures after member/archive verification; these are not finalized and remain subject to reorganization; no finalized profitability conclusion is produced")
	progress(LoadProgress{Phase: "archive_validation_started", Total: uint64(len(latest))})
	var archiveResults []reportArchiveResult
	for captureIndex, c := range latest {
		if captureIndex%reportArchiveChunkSize == 0 {
			archiveStarted := time.Now()
			archiveResults, e = archiveValidator.check(ctx, latest[captureIndex:min(captureIndex+reportArchiveChunkSize, len(latest))], loaded.Errors)
			s.Performance.ArchiveValidationMicros += time.Since(archiveStarted).Microseconds()
			if e != nil {
				return e
			}
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		reason := c.Reason
		fromTime, toTime := "", ""
		rawComplete, decodedComplete := false, false
		use := c.Canonical && c.Committed
		if !use {
			s.Counts["excluded_orphan_or_uncommitted_captures"]++
		}
		if use {
			b := loaded.Batches[c.CaptureId]
			err := loaded.Errors[c.CaptureId]
			delete(loaded.Batches, c.CaptureId)
			if err != nil {
				use = false
				reason = "incomplete_members: " + err.Error()
				s.Counts["incomplete_capture_members"]++
			} else {
				s.Performance.CaptureEvidenceChecks++
				verified := archiveResults[captureIndex%reportArchiveChunkSize]
				evidence := verified.evidence
				if verified.reason != "" {
					use = false
					reason = verified.reason
					if reason == "missing_or_invalid_capture_evidence" {
						s.Counts["missing_capture_evidence"]++
					} else {
						s.Counts["missing_rpc_evidence"]++
					}
				}
				if use {
					if evidence.FromTime != nil {
						fromTime = reportTime(*evidence.FromTime)
					}
					if evidence.ToTime != nil {
						toTime = reportTime(*evidence.ToTime)
					}
					if c.CaptureKind == "logs" && c.CompletedTasks == 1 && c.FromBlock != nil && evidence.FromTime != nil && evidence.ToTime != nil {
						rawComplete = true
						rawRanges[c.ChainId] = append(rawRanges[c.ChainId], reportLogRange{*c.FromBlock, *c.ToBlock, *evidence.FromTime, *evidence.ToTime})
						if c.Status == "complete" && c.UnknownEventCount == 0 {
							decodedComplete = true
							decodedRanges[c.ChainId] = append(decodedRanges[c.ChainId], reportLogRange{*c.FromBlock, *c.ToBlock, *evidence.FromTime, *evidence.ToTime})
						}
					}
					if err = facts.add(b); err != nil {
						return err
					}
					s.Counts["accepted_captures"]++
					s.EligibleCaptureFinalityCounts[c.Finality]++
					s.Counts["unknown_events"] += uint64(c.UnknownEventCount)
					if c.Status != "complete" {
						s.Counts["partial_or_error_captures"]++
					}
				}
			}
		}
		coverage = append(coverage, []string{Hex(c.CaptureId), strconv.FormatUint(c.ChainId, 10), c.CaptureKind, c.CaptureMode, reportNullable(c.FromBlock), reportNullable(c.ToBlock), fromTime, toTime, c.Status, c.Finality, strconv.FormatBool(c.Canonical), strconv.FormatBool(c.Committed), strconv.FormatBool(use), strconv.FormatBool(rawComplete), strconv.FormatBool(decodedComplete), strconv.FormatUint(uint64(c.UnknownEventCount), 10), reportTime(c.StartedAt), reportTime(c.AvailableAt), reason})
		if (captureIndex+1)%1000 == 0 || captureIndex+1 == len(latest) {
			progress(LoadProgress{Phase: "archive_validation_progress", Completed: uint64(captureIndex + 1), Total: uint64(len(latest)), Payloads: archiveValidator.count(), ElapsedMicros: s.Performance.ArchiveValidationMicros})
		}
	}
	progress(LoadProgress{Phase: "archive_validation_finished", Completed: uint64(len(latest)), ElapsedMicros: s.Performance.ArchiveValidationMicros})
	s.Counts["raw_response_unavailable"] = archiveValidator.unavailableCount()
	for _, chain := range manifest.Chains {
		c := reportLogCoverage{Raw: reportMergeLogRanges(rawRanges[chain.ChainID]), Decoded: reportMergeLogRanges(decodedRanges[chain.ChainID])}
		for _, r := range c.Raw {
			c.RawBlocks += r.Last - r.First + 1
		}
		for _, r := range c.Decoded {
			c.DecodedBlocks += r.Last - r.First + 1
		}
		s.LogCoverage[strconv.FormatUint(chain.ChainID, 10)] = c
		for _, kind := range []struct {
			name   string
			ranges []reportLogRange
		}{{"raw", c.Raw}, {"decoded", c.Decoded}} {
			if len(kind.ranges) == 0 {
				s.Counts["chains_without_"+kind.name+"_log_range"]++
				continue
			}
			for i := 1; i < len(kind.ranges); i++ {
				s.Counts["internal_"+kind.name+"_log_height_gaps"]++
				coverage = append(coverage, []string{"", strconv.FormatUint(chain.ChainID, 10), kind.name + "_gap", "", strconv.FormatUint(kind.ranges[i-1].Last+1, 10), strconv.FormatUint(kind.ranges[i].First-1, 10), "", "", "missing", "unknown", "", "", "false", "false", "false", "", "", "", "gap_between_" + kind.name + "_ranges"})
			}
		}
	}
	orders, e := reportOrders(&facts, manifest, from, to, &s)
	if e != nil {
		return e
	}
	progress(LoadProgress{Phase: "orders_built", Completed: uint64(len(orders)), ElapsedMicros: time.Since(started).Microseconds()})
	refunds := reportRefundRows(&facts, manifest, from, to, &s)
	progress(LoadProgress{Phase: "refunds_built", Completed: uint64(len(refunds)), ElapsedMicros: time.Since(started).Microseconds()})
	for _, r := range facts.receipts {
		if r.BlockTime.Before(from) || !r.BlockTime.Before(to) {
			continue
		}
		s.Counts["unique_receipts"]++
		if r.FeeComplete && r.TotalFeeWei != nil {
			k := strconv.FormatUint(r.ChainId, 10)
			n := new(big.Int)
			n.SetString(s.TransactionFeesWei[k], 10)
			n.Add(n, r.TotalFeeWei)
			s.TransactionFeesWei[k] = n.String()
		} else {
			s.Counts["receipt_fee_unknown"]++
		}
	}
	for _, p := range facts.probes {
		if p.RequestedAt.Before(from) || !p.RequestedAt.Before(to) {
			continue
		}
		s.Counts["probe_status_"+p.ProbeStatus]++
		if p.EthPriceAvailableAt == nil || p.AvailableAt.Sub(*p.EthPriceAvailableAt) > 60*time.Second {
			s.Counts["probe_eth_price_missing_or_stale"]++
		}
		if p.UsdcPriceAvailableAt == nil || p.AvailableAt.Sub(*p.UsdcPriceAvailableAt) > 60*time.Second {
			s.Counts["probe_usdc_price_missing_or_stale"]++
		}
	}
	if e = os.MkdirAll(out, 0700); e != nil {
		return e
	}
	if e = writeReportCSV(filepath.Join(out, "coverage.csv"), []string{"capture_id", "chain_id", "kind", "mode", "from_block", "to_block", "from_block_time_utc", "to_block_time_utc", "status", "finality", "canonical", "committed", "members_and_evidence_accepted", "raw_range_complete", "decoded_range_complete", "unknown_event_count", "started_at_utc", "available_at_utc", "reason"}, coverage); e != nil {
		return e
	}
	if e = writeReportCSV(filepath.Join(out, "orders.csv"), reportOrderHeader, orders); e != nil {
		return e
	}
	if e = writeReportCSV(filepath.Join(out, "refunds.csv"), reportRefundHeader, refunds); e != nil {
		return e
	}
	s.Performance.UniqueRPCPayloadChecks = archiveValidator.count()
	s.Performance.TotalBeforeSummaryWriteMicros = time.Since(started).Microseconds()
	raw, e := json.MarshalIndent(s, "", "  ")
	if e != nil {
		return e
	}
	raw = append(raw, '\n')
	return os.WriteFile(filepath.Join(out, "summary.json"), raw, 0600)
}

var reportOrderHeader = []string{"origin_chain_id", "destination_chain_id", "deposit_id", "relay_hash", "origin_block_hash", "deposit_time_utc", "route_eligibility", "input_usdc", "original_output_usdc", "original_terms_fee_space_usdc", "fill_state", "fill_time_utc", "fill_type", "actual_output_usdc", "observed_fast_fill_fee_space_usdc", "fill_chain_id", "fill_block_hash", "fill_tx_hash", "repayment_chain_id", "repayment_address", "sender", "fill_transaction_fee_wei_shared", "fill_transaction_fee_complete", "fill_transaction_fee_scope", "first_live_available_at_utc", "first_live_delay_ms", "first_probe_state", "ever_live_open", "delay_2s_state", "delay_5s_state", "delay_10s_state", "update_count", "attribution", "net_profit_usdt"}

func reportOrders(f *reportFacts, m Manifest, from, to time.Time, s *reportSummary) ([][]string, error) {
	fillIndex := map[string][]Fill{}
	for _, v := range f.fills {
		fillIndex[reportPairKey(v.OriginChainId, v.DepositId)] = append(fillIndex[reportPairKey(v.OriginChainId, v.DepositId)], v)
	}
	probeIndex := map[string][]OrderProbe{}
	for _, v := range f.probes {
		probeIndex[Hex(v.RelayHash)] = append(probeIndex[Hex(v.RelayHash)], v)
	}
	updateIndex := map[string][]DepositUpdate{}
	for _, v := range f.updates {
		k := reportPairKey(v.ChainId, v.DepositId)
		updateIndex[k] = append(updateIndex[k], v)
	}
	matched := map[string]bool{}
	seenOrders := map[string]bool{}
	keys := make([]string, 0, len(f.deposits))
	for k := range f.deposits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := [][]string{}
	for _, key := range keys {
		d := f.deposits[key]
		if d.BlockTime.Before(from) || !d.BlockTime.Before(to) {
			continue
		}
		orderKey := reportOrderKey(d)
		if seenOrders[orderKey] {
			return nil, errors.New("duplicate_relay_identity_in_distinct_deposit_events")
		}
		seenOrders[orderKey] = true
		route := reportRoute(d, m)
		eligible := route == "eligible_original_empty_terms"
		s.Counts[route]++
		s.Counts["unique_deposits"]++
		original := reportPositiveSpread(d.InputAmountRaw, d.OutputAmountRaw)
		ceiling := ""
		if eligible {
			ceiling = reportAmount(original)
		}
		state := "fill_unknown"
		if time.Unix(int64(d.FillDeadline), 0).After(to) {
			state = "right_censored"
			s.Counts["right_censored_orders"]++
		}
		var fill *Fill
		for _, v := range fillIndex[reportPairKey(d.ChainId, d.DepositId)] {
			destination, knownDestination := m.Chain(v.ChainId)
			if knownDestination && v.SpokePool == destination.SpokePool && reportFillMatches(d, v) {
				if fill != nil {
					return nil, errors.New("multiple_distinct_fills_for_relay")
				}
				v := v
				fill = &v
				matched[reportEventKey(v.ChainId, v.SpokePool, v.BlockHash, v.TxHash, v.LogIndex)] = true
			}
		}
		fillTime, fillType, actual, fastSpace, repayChain, repayAddr, sender, fee, feeComplete := "", "", "", "", "", "", "", "", ""
		fillChain, fillBlock, fillTx, feeScope := "", "", "", ""
		fast := big.NewInt(0)
		if fill != nil {
			v := *fill
			state = "filled"
			fillTime = reportTime(v.BlockTime)
			fillType = strconv.FormatUint(uint64(v.FillType), 10)
			fillChain = strconv.FormatUint(v.ChainId, 10)
			fillBlock = Hex(v.BlockHash)
			fillTx = Hex(v.TxHash)
			feeScope = "whole_transaction_shared_do_not_sum_orders"
			actual = reportAmount(v.UpdatedOutputAmountRaw)
			repayChain = strconv.FormatUint(v.RepaymentChainId, 10)
			repayAddr = Hex(v.RepaymentAddress)
			if v.BlockTime.Before(d.BlockTime) {
				s.Counts["cross_chain_clock_order_inversions"]++
				state = "prefill_possible_or_clock_order_inversion"
			}
			if v.FillType == 2 {
				s.Counts["slow_fill_orders"]++
			} else if eligible && v.UpdatedMessageHash == strings.Repeat("\x00", 32) {
				fast = reportPositiveSpread(d.InputAmountRaw, v.UpdatedOutputAmountRaw)
				fastSpace = reportAmount(fast)
				s.Counts["matched_fast_fill_orders"]++
				s.RepaymentAddresses[repayChain+"/"+repayAddr]++
			} else {
				s.Counts["excluded_fill_execution_terms"]++
			}
			r, ok := f.receipts[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)]
			if ok {
				sender = Hex(r.Sender)
				feeComplete = strconv.FormatBool(r.FeeComplete)
				if r.TotalFeeWei != nil && r.FeeComplete {
					fee = r.TotalFeeWei.String()
				}
			} else {
				s.Counts["fill_receipt_missing"]++
			}
		} else {
			s.Counts["fill_unknown_orders"]++
		}
		first, delay := "", ""
		if t, ok := f.firstSeen[orderKey]; ok {
			first = reportTime(t)
			delay = strconv.FormatInt(t.Sub(d.BlockTime).Milliseconds(), 10)
			s.Counts["live_seen_orders"]++
		}
		probes := probeIndex[Hex(d.RelayHash)]
		sort.Slice(probes, func(i, j int) bool { return probes[i].AvailableAt.Before(probes[j].AvailableAt) })
		firstState := "unknown"
		firstProbeSeen := false
		everOpen := false
		delays := map[uint32]string{2000: "unknown", 5000: "unknown", 10000: "unknown"}
		for _, p := range probes {
			ps := reportProbeState(d, p)
			if ps == "unmatched_origin" {
				continue
			}
			if !firstProbeSeen && p.TargetDelayMs == nil && p.ObservationOrigin == "live" {
				firstState = ps
				firstProbeSeen = true
			}
			if ps == "open" && eligible {
				everOpen = true
			}
			if p.TargetDelayMs != nil {
				if _, ok := delays[*p.TargetDelayMs]; ok && delays[*p.TargetDelayMs] == "unknown" {
					delays[*p.TargetDelayMs] = ps
				}
			}
		}
		if firstState == "filled" {
			s.Counts["first_live_probe_already_filled"]++
		}
		if everOpen {
			s.Counts["live_observed_open_orders"]++
		}
		updates := 0
		for _, u := range updateIndex[reportPairKey(d.ChainId, d.DepositId)] {
			if u.SpokePool == d.SpokePool && u.Depositor == d.Depositor {
				updates++
			}
		}
		rows = append(rows, []string{strconv.FormatUint(d.ChainId, 10), strconv.FormatUint(d.DestinationChainId, 10), d.DepositId.String(), Hex(d.RelayHash), Hex(d.BlockHash), reportTime(d.BlockTime), route, reportAmount(d.InputAmountRaw), reportAmount(d.OutputAmountRaw), ceiling, state, fillTime, fillType, actual, fastSpace, fillChain, fillBlock, fillTx, repayChain, repayAddr, sender, fee, feeComplete, feeScope, first, delay, firstState, strconv.FormatBool(everOpen), delays[2000], delays[5000], delays[10000], strconv.Itoa(updates), "attribution_unknown", ""})
		if eligible {
			s.OriginalTermsCeiling = reportAddAmount(s.OriginalTermsCeiling, original)
			s.ObservedFastCeiling = reportAddAmount(s.ObservedFastCeiling, fast)
			if everOpen {
				s.LiveOpenCeiling = reportAddAmount(s.LiveOpenCeiling, original)
			}
			for _, group := range []struct {
				m map[string]*reportTotals
				k string
			}{{s.Daily, d.BlockTime.UTC().Format("2006-01-02")}, {s.Directions, fmt.Sprintf("%d->%d", d.ChainId, d.DestinationChainId)}} {
				t := group.m[group.k]
				if t == nil {
					t = &reportTotals{InputUSDC: "0.000000", OriginalTermsCeiling: "0.000000", ObservedFastCeiling: "0.000000", LiveOpenCeiling: "0.000000"}
					group.m[group.k] = t
				}
				t.Orders++
				t.InputUSDC = reportAddAmount(t.InputUSDC, d.InputAmountRaw)
				t.OriginalTermsCeiling = reportAddAmount(t.OriginalTermsCeiling, original)
				t.ObservedFastCeiling = reportAddAmount(t.ObservedFastCeiling, fast)
				if everOpen {
					t.LiveOpenCeiling = reportAddAmount(t.LiveOpenCeiling, original)
				}
			}
		}
	}
	for key, v := range f.fills {
		if !matched[key] && !v.BlockTime.Before(from) && v.BlockTime.Before(to) {
			s.Counts["unmatched_fills"]++
		}
	}
	return rows, nil
}

var reportRefundHeader = []string{"chain_id", "spoke_pool", "block_hash", "tx_hash", "log_index", "block_time_utc", "event_kind", "root_bundle_id", "leaf_id", "caller_credit_address", "recipient", "expected_usdc", "verified_usdc", "payment_status", "transfer_log_index", "deferred_refunds", "order_attribution", "route_membership"}

func reportRefundRows(f *reportFacts, m Manifest, from, to time.Time, s *reportSummary) [][]string {
	groups := map[string][]Refund{}
	for _, v := range f.refunds {
		if v.BlockTime.Before(from) || !v.BlockTime.Before(to) {
			continue
		}
		allowed := false
		for _, c := range m.Chains {
			if c.ChainID == v.ChainId && c.USDC == v.Token && c.SpokePool == v.SpokePool {
				allowed = true
			}
		}
		if !allowed {
			continue
		}
		k := reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)
		groups[k] = append(groups[k], v)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := [][]string{}
	for _, k := range keys {
		refunds := groups[k]
		sort.Slice(refunds, func(i, j int) bool { return refunds[i].LogIndex < refunds[j].LogIndex })
		r, exists := f.receipts[k]
		var transfers []receiptTransfer
		var err error
		if exists {
			if data, ok := f.transfers[k+"/"+Hex(refunds[0].Token)]; ok {
				transfers, err = storedReceiptTransfers(data, r, refunds[0].Token)
			} else {
				err = errors.New("receipt_transfer_set_missing")
			}
		}
		// Exact amount/address duplicates across claims or leaves are ambiguous;
		// never allocate one transfer twice or invent event-to-transfer ordering.
		demand := map[string]int{}
		supply := map[string][]receiptTransfer{}
		transferKey := func(from, to string, amount *big.Int) string {
			return Hex(from) + "/" + Hex(to) + "/" + amount.String()
		}
		for _, v := range refunds {
			for i, a := range v.RefundAddresses {
				demand[transferKey(v.SpokePool, a, v.RefundAmountsRaw[i])]++
			}
		}
		for _, t := range transfers {
			tk := transferKey(t.from, t.to, t.amount)
			supply[tk] = append(supply[tk], t)
		}
		for refundIndex, v := range refunds {
			for i, a := range v.RefundAddresses {
				amount := v.RefundAmountsRaw[i]
				tk := transferKey(v.SpokePool, a, amount)
				status := "payment_unknown"
				verified, index := "", ""
				if !exists {
					status = "receipt_missing"
				} else if err != nil {
					status = "receipt_transfers_invalid"
					if err.Error() == "receipt_transfer_set_missing" {
						status = "receipt_transfers_missing"
					}
				} else if demand[tk] == 1 && len(supply[tk]) == 1 && supply[tk][0].index < v.LogIndex && (refundIndex == 0 || supply[tk][0].index > refunds[refundIndex-1].LogIndex) {
					status = "verified_transfer"
					verified = reportAmount(amount)
					index = strconv.FormatUint(uint64(supply[tk][0].index), 10)
					s.Counts["verified_refund_transfers"]++
				} else if len(supply[tk]) == 0 {
					status = "no_matching_transfer"
					if v.DeferredRefunds != nil && *v.DeferredRefunds {
						status = "deferred_or_unmatched"
					}
				} else {
					status = "ambiguous_transfer_allocation"
				}
				if status != "verified_transfer" {
					s.Counts["refund_payment_unverified"]++
				}
				rows = append(rows, []string{strconv.FormatUint(v.ChainId, 10), Hex(v.SpokePool), Hex(v.BlockHash), Hex(v.TxHash), strconv.FormatUint(uint64(v.LogIndex), 10), reportTime(v.BlockTime), v.EventKind, reportNullable(v.RootBundleId), reportNullable(v.LeafId), Hex(v.Caller), Hex(a), reportAmount(amount), verified, status, index, reportNullable(v.DeferredRefunds), "attribution_unknown", "other_origins_or_user_refunds_possible"})
			}
		}
	}
	return rows
}
