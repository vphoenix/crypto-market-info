package across

import (
	"context"
	"encoding/csv"
	"encoding/hex"
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
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// Report does not connect to an RPC or use current prices to value history. Its
// ceilings are user fee space, before LP fees, execution, inventory and rivalry.
type reportSummary struct {
	ManifestHash              string                     `json:"manifest_hash"`
	From                      time.Time                  `json:"from_utc"`
	To                        time.Time                  `json:"to_utc"`
	GeneratedAt               time.Time                  `json:"generated_at_utc"`
	Scope                     string                     `json:"scope"`
	CoverageScope             string                     `json:"coverage_scope"`
	OrderScope                string                     `json:"order_scope"`
	ReceiptAndProbeScope      string                     `json:"receipt_and_probe_counts_scope"`
	Counts                    map[string]uint64          `json:"counts"`
	OriginalTermsCeiling      string                     `json:"original_terms_fee_space_usdc"`
	ObservedFastCeiling       string                     `json:"observed_fast_fill_fee_space_usdc"`
	LiveOpenCeiling           string                     `json:"live_observed_open_original_terms_fee_space_usdc"`
	Daily                     map[string]*reportTotals   `json:"utc_days"`
	Directions                map[string]*reportTotals   `json:"directions"`
	RepaymentAddresses        map[string]uint64          `json:"fast_fills_by_repayment_address"`
	TransactionFeesWei        map[string]string          `json:"unique_observed_transaction_fees_wei_by_chain"`
	InventorySensitivity      reportInventorySensitivity `json:"inventory_sensitivity"`
	NetProfitUSDT             *string                    `json:"net_profit_usdt"`
	NegativeConclusionAllowed bool                       `json:"negative_conclusion_allowed"`
	Limitations               []string                   `json:"limitations"`
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
	firstSeen map[string]time.Time
}

func newReportFacts() reportFacts {
	return reportFacts{map[string]Deposit{}, map[string]Fill{}, map[string]DepositUpdate{}, map[string]Refund{}, map[string]TxReceipt{}, map[string]OrderProbe{}, map[string]time.Time{}}
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
func BuildReport(ctx context.Context, store Store, manifest Manifest, archive ethereum.Archive, from, to time.Time, out string) error {
	if from.IsZero() || !to.After(from) {
		return errors.New("invalid_report_interval")
	}
	caps, e := store.AcrossCaptures(ctx, manifest.Hash)
	if e != nil {
		return e
	}
	// Defend callers/fakes as well as FINAL SQL: latest first, filters second.
	latest := map[string]Capture{}
	for _, c := range caps {
		if old, ok := latest[c.CaptureId]; !ok || c.Revision > old.Revision {
			latest[c.CaptureId] = c
		} else if c.Revision == old.Revision && ID(c) != ID(old) {
			return errors.New("conflicting_capture_revision")
		}
	}
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	facts := newReportFacts()
	coverage := [][]string{}
	type logRange struct {
		first, last uint64
		from, to    time.Time
	}
	ranges := map[uint64][]logRange{}
	checkedPayloads := map[string]bool{}
	s := reportSummary{ManifestHash: Hex(manifest.Hash), From: from.UTC(), To: to.UTC(), GeneratedAt: Now(), Scope: "Base-Arbitrum native USDC; original empty-message terms; public polling observations", Counts: map[string]uint64{}, Daily: map[string]*reportTotals{}, Directions: map[string]*reportTotals{}, RepaymentAddresses: map[string]uint64{}, TransactionFeesWei: map[string]string{}, OriginalTermsCeiling: "0.000000", ObservedFastCeiling: "0.000000", LiveOpenCeiling: "0.000000", Limitations: []string{"fee space is before LP, transaction, failure, inventory and operating costs; no verified net profit", "all relayer refunds remain per-order attribution_unknown; no observed capital turnover", "absence of a matched fill remains fill_unknown: destination coverage and prefill history are not proven by an absent event", "unknown ABI, unmatched fills, nonempty messages and updated execution terms prevent a global negative Across conclusion", "CEX prices are reference observations only; historical receipts are not repriced using current quotes", "two-chain capture excludes other origins and user-refund membership; refund transfers are not route income"}}
	s.CoverageScope = "coverage.csv and capture/evidence/unknown-event/log-gap counts use ALL saved captures for this manifest, including outside [from_utc,to_utc); latest revisions are selected before filtering"
	s.OrderScope = "orders.csv, order counts and fee-space totals select deposit block_time in [from_utc,to_utc); matching fills, updates and probes use all saved evidence, including after to_utc; this is a deposit cohort, not an as-of replay"
	s.ReceiptAndProbeScope = "unique receipt fees and receipt counts select receipt block_time in [from_utc,to_utc); probe status/price counts select probe requested_at in that window; refund rows and unmatched-fill counts select their event block_time in that window"
	s.InventorySensitivity = reportInventoryScenarios()
	s.Limitations = append(s.Limitations, "orders.csv fill_transaction_fee_wei_shared is the full shared transaction fee; do not sum this column across orders; summary receipt fees deduplicate chain/block_hash/tx_hash")
	for _, key := range keys {
		c := latest[key]
		reason := c.Reason
		fromTime, toTime := "", ""
		use := c.Canonical && c.Committed
		if !use {
			s.Counts["excluded_orphan_or_uncommitted_captures"]++
		}
		if use {
			b, err := store.AcrossBatch(ctx, c)
			if err != nil {
				use = false
				reason = "incomplete_members: " + err.Error()
				s.Counts["incomplete_capture_members"]++
			} else {
				var evidence CaptureEvidence
				if evidence, err = ReadCaptureEvidence(archive, c); err != nil {
					use = false
					reason = "missing_or_invalid_capture_evidence"
					s.Counts["missing_capture_evidence"]++
				}
				if use {
					for _, ref := range evidence.RPCPayloads {
						good, checked := checkedPayloads[ref]
						if !checked {
							rawHash, parseErr := reportDecodeHex(ref, 32)
							if parseErr == nil {
								var h dex.Hash
								copy(h[:], rawHash)
								_, parseErr = archive.Get(h)
							}
							good = parseErr == nil
							checkedPayloads[ref] = good
						}
						if !good {
							use = false
							reason = "missing_or_invalid_rpc_evidence"
							s.Counts["missing_rpc_evidence"]++
							break
						}
					}
				}
				if use {
					if evidence.FromTime != nil {
						fromTime = reportTime(*evidence.FromTime)
					}
					if evidence.ToTime != nil {
						toTime = reportTime(*evidence.ToTime)
					}
					if c.CaptureKind == "logs" && c.Status == "complete" && c.UnknownEventCount == 0 && c.FromBlock != nil && evidence.FromTime != nil && evidence.ToTime != nil {
						ranges[c.ChainId] = append(ranges[c.ChainId], logRange{*c.FromBlock, *c.ToBlock, *evidence.FromTime, *evidence.ToTime})
					}
					if err = facts.add(b); err != nil {
						return err
					}
					s.Counts["accepted_captures"]++
					s.Counts["unknown_events"] += uint64(c.UnknownEventCount)
					if c.Status != "complete" {
						s.Counts["partial_or_error_captures"]++
					}
				}
			}
		}
		coverage = append(coverage, []string{Hex(c.CaptureId), strconv.FormatUint(c.ChainId, 10), c.CaptureKind, c.CaptureMode, reportNullable(c.FromBlock), reportNullable(c.ToBlock), fromTime, toTime, c.Status, c.Finality, strconv.FormatBool(c.Canonical), strconv.FormatBool(c.Committed), strconv.FormatBool(use), strconv.FormatUint(uint64(c.UnknownEventCount), 10), reportTime(c.StartedAt), reportTime(c.AvailableAt), reason})
	}
	for _, chain := range manifest.Chains {
		rr := ranges[chain.ChainID]
		sort.Slice(rr, func(i, j int) bool { return rr[i].first < rr[j].first })
		if len(rr) == 0 {
			s.Counts["chains_without_complete_known_abi_log_range"]++
			continue
		}
		end := rr[0].last
		for _, r := range rr[1:] {
			if r.first > end && r.first-end > 1 {
				s.Counts["internal_log_height_gaps"]++
				coverage = append(coverage, []string{"", strconv.FormatUint(chain.ChainID, 10), "gap", "", strconv.FormatUint(end+1, 10), strconv.FormatUint(r.first-1, 10), "", "", "missing", "unknown", "", "", "false", "", "", "", "gap_between_complete_known_abi_ranges"})
			}
			if r.last > end {
				end = r.last
			}
		}
	}
	orders, e := reportOrders(&facts, manifest, from, to, &s)
	if e != nil {
		return e
	}
	refunds := reportRefundRows(&facts, archive, manifest, from, to, &s)
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
	if e = writeReportCSV(filepath.Join(out, "coverage.csv"), []string{"capture_id", "chain_id", "kind", "mode", "from_block", "to_block", "from_block_time_utc", "to_block_time_utc", "status", "finality", "canonical", "committed", "members_and_evidence_accepted", "unknown_event_count", "started_at_utc", "available_at_utc", "reason"}, coverage); e != nil {
		return e
	}
	if e = writeReportCSV(filepath.Join(out, "orders.csv"), reportOrderHeader, orders); e != nil {
		return e
	}
	if e = writeReportCSV(filepath.Join(out, "refunds.csv"), reportRefundHeader, refunds); e != nil {
		return e
	}
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

type reportTransfer struct {
	from, to string
	amount   *big.Int
	index    uint32
}

func reportDecodeHex(s string, n int) (string, error) {
	if !strings.HasPrefix(s, "0x") || len(s) != 2+2*n {
		return "", errors.New("invalid_hex_width")
	}
	b, e := hex.DecodeString(s[2:])
	return string(b), e
}
func reportQuantity(s string) (*big.Int, error) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 || len(s) > 3 && s[2] == '0' {
		return nil, errors.New("invalid_quantity")
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return nil, errors.New("invalid_quantity")
		}
	}
	n, ok := new(big.Int).SetString(s[2:], 16)
	if !ok || n.Sign() < 0 || n.BitLen() > 256 {
		return nil, errors.New("invalid_quantity")
	}
	return n, nil
}

// Receipt payloads are content-addressed RPC envelopes. Match transaction and
// block before examining transfers, and reject ambiguous or malformed evidence.
func reportReceiptTransfers(archive ethereum.Archive, r TxReceipt, token string) ([]reportTransfer, error) {
	if !r.Success {
		return nil, errors.New("failed_receipt_cannot_pay_refund")
	}
	var h dex.Hash
	copy(h[:], r.ReceiptPayloadHash)
	raw, e := archive.Get(h)
	if e != nil {
		return nil, e
	}
	var env struct{ Response []byte }
	if e = json.Unmarshal(raw, &env); e != nil || len(env.Response) == 0 {
		return nil, errors.New("invalid_receipt_archive")
	}
	var responses []struct {
		Result json.RawMessage `json:"result"`
	}
	if e = json.Unmarshal(env.Response, &responses); e != nil {
		return nil, e
	}
	type log struct {
		Address         string   `json:"address"`
		Topics          []string `json:"topics"`
		Data            string   `json:"data"`
		LogIndex        string   `json:"logIndex"`
		TransactionHash string   `json:"transactionHash"`
		BlockHash       string   `json:"blockHash"`
		Removed         bool     `json:"removed"`
	}
	var chosen []log
	found := false
	for _, response := range responses {
		var rr struct {
			TransactionHash string `json:"transactionHash"`
			BlockHash       string `json:"blockHash"`
			Status          string `json:"status"`
			Logs            []log  `json:"logs"`
		}
		if json.Unmarshal(response.Result, &rr) != nil {
			continue
		}
		if !strings.EqualFold(rr.TransactionHash, Hex(r.TxHash)) {
			continue
		}
		if found || !strings.EqualFold(rr.BlockHash, Hex(r.BlockHash)) || rr.Status != "0x1" || rr.Logs == nil {
			return nil, errors.New("receipt_anchor_or_status_mismatch")
		}
		found = true
		chosen = rr.Logs
	}
	if !found {
		return nil, errors.New("receipt_result_missing")
	}
	const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	out := []reportTransfer{}
	seen := map[uint32]bool{}
	for _, l := range chosen {
		if !strings.EqualFold(l.Address, Hex(token)) {
			continue
		}
		if len(l.Topics) == 0 || strings.ToLower(l.Topics[0]) != transferTopic {
			continue
		}
		if len(l.Topics) != 3 || l.Removed || !strings.EqualFold(l.TransactionHash, Hex(r.TxHash)) || !strings.EqualFold(l.BlockHash, Hex(r.BlockHash)) {
			return nil, errors.New("invalid_transfer_anchor")
		}
		from, e := reportDecodeHex(l.Topics[1], 32)
		if e != nil {
			return nil, e
		}
		to, e := reportDecodeHex(l.Topics[2], 32)
		if e != nil {
			return nil, e
		}
		data, e := reportDecodeHex(l.Data, 32)
		if e != nil {
			return nil, e
		}
		index, e := reportQuantity(l.LogIndex)
		if e != nil || !index.IsUint64() || index.Uint64() > uint64(^uint32(0)) {
			return nil, errors.New("invalid_transfer_index")
		}
		i := uint32(index.Uint64())
		if seen[i] {
			return nil, errors.New("duplicate_transfer_index")
		}
		seen[i] = true
		if from[:12] != strings.Repeat("\x00", 12) || to[:12] != strings.Repeat("\x00", 12) {
			return nil, errors.New("non_evm_transfer_address")
		}
		out = append(out, reportTransfer{from[12:], to[12:], new(big.Int).SetBytes([]byte(data)), i})
	}
	return out, nil
}

func reportRefundRows(f *reportFacts, archive ethereum.Archive, m Manifest, from, to time.Time, s *reportSummary) [][]string {
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
		var transfers []reportTransfer
		var err error
		if exists {
			transfers, err = reportReceiptTransfers(archive, r, refunds[0].Token)
		}
		// Exact amount/address duplicates across claims or leaves are ambiguous;
		// never allocate one transfer twice or invent event-to-transfer ordering.
		demand := map[string]int{}
		supply := map[string][]reportTransfer{}
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
					status = "receipt_evidence_invalid"
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
