package justlendkeeper

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type ReportReader interface {
	KeeperCaptures(context.Context, any, any) ([]Capture, error)
	KeeperBatch(context.Context, Capture) (Batch, error)
}
type BulkReportReader interface {
	KeeperBatches(context.Context, []Capture) (map[uuid.UUID]Batch, error)
}
type Summary struct {
	TopTwoAddressPercent      string  `json:"top_two_address_percent"`
	TopFiveObservedDayPercent string  `json:"top_five_observed_day_percent"`
	CompleteUTCDays           int     `json:"complete_utc_days"`
	CompleteDayMedianTRX      *string `json:"complete_day_median_trx"`
	WorstCompleteSevenDayTRX  *string `json:"worst_complete_seven_day_trx"`

	From                    time.Time      `json:"from"`
	To                      time.Time      `json:"to"`
	AsOf                    time.Time      `json:"as_of"`
	Events                  int            `json:"liquidation_events"`
	Transactions            int            `json:"liquidation_transactions"`
	GrossTRX                string         `json:"gross_reward_trx"`
	RewardTransfersVerified int            `json:"reward_transfer_verified_transactions"`
	KnownBurnTRX            *string        `json:"known_whole_transaction_burn_trx"`
	KnownBurnTransactions   int            `json:"known_burn_transactions"`
	BurnCoverage            string         `json:"burn_coverage"`
	NetProfitStatus         string         `json:"net_profit_status"`
	UnknownBurn             int            `json:"unknown_burn_transactions"`
	Probes                  int            `json:"simulation_observations"`
	ProbeStatus             map[string]int `json:"probe_status"`
	ProbeObservedStatus     map[string]int `json:"probe_observed_status"`
	ProbeReclassified       int            `json:"probe_reclassified_observations"`
	PositiveUncertified     int            `json:"positive_return_unclassified"`
	CoverageCaptures        int            `json:"capture_rows"`
	Incomplete              int            `json:"incomplete_or_failed_captures"`
	Notes                   []string       `json:"notes"`
}

func CSV(path string, header []string, rows [][]string) error {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".report-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
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
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
func in(t, from, to time.Time) bool { return !t.Before(from) && t.Before(to) }
func sun(n *big.Int) string         { return decimal.NewFromBigInt(n, -6).String() }
func fieldU(n *uint64) string {
	if n == nil {
		return "unknown"
	}
	return strconv.FormatUint(*n, 10)
}
func fieldBig(n *big.Int) string {
	if n == nil {
		return "unknown"
	}
	return n.String()
}
func Report(ctx context.Context, r ReportReader, a Archive, cfg Config, from, to, now time.Time, out string) error {
	if !from.Before(to) {
		return errors.New("invalid_report_window")
	}
	caps, e := r.KeeperCaptures(ctx, time.Unix(0, 0).UTC(), now.Add(time.Second))
	if e != nil {
		return e
	}
	summary := Summary{From: UTC(from), To: UTC(to), AsOf: UTC(now), ProbeStatus: map[string]int{}, Notes: []string{"gross rewards belong to historical winners, not an estimate of our income", "failed competing attempts and non-burn resource cost are unknown", "missing fee is unknown, never zero; mixed transaction costs are not allocated per event", "positive simulation ABI fixture is not certified; no measured win rate or guaranteed income", "bootstrap is a bounded sample; no observations are not evidence of no market opportunity", "coverage rows describe returned pages, not an independently proven complete chain index"}}
	events := map[string]RentalEvent{}
	receipts := map[string]TxReceipt{}
	probes := []Probe{}
	coverage := [][]string{}
	costs := []CostObservation{}
	windows := map[string]*windowPages{}
	anchors := map[uint64]string{}
	verifiedHashes := map[string]bool{}
	observedStatus := map[string]string{}
	bulk, useBulk := r.(BulkReportReader)
	var batches map[uuid.UUID]Batch
	var manifests map[uuid.UUID]Manifest
	var responseBodies map[string][]byte
	summary.ProbeObservedStatus = map[string]int{}
	summary.NetProfitStatus = "unknown"
	checkAnchor := func(number uint64, hash string) error {
		if prior, ok := anchors[number]; ok && prior != hash {
			return errors.New("report_solid_hash_conflict")
		}
		anchors[number] = hash
		return nil
	}
	for i, cap := range caps {
		if i%256 == 0 {
			group := []Capture{}
			for _, member := range caps[i:min(i+256, len(caps))] {
				if member.Committed && member.ConfigHash == cfg.Hash() {
					group = append(group, member)
				}
			}
			retain := map[string]bool{}
			if useBulk {
				batches, e = bulk.KeeperBatches(ctx, group)
				if e != nil {
					return e
				}
				for _, batch := range batches {
					for _, p := range batch.Probes {
						if in(p.AvailableAt, from, to) && (p.Status == "revert" || p.Status == "rpc_error") && p.ResponsePayloadHash != nil {
							retain[*p.ResponsePayloadHash] = true
						}
					}
				}
			}
			manifests, responseBodies, e = reportEvidence(ctx, a, group, verifiedHashes, retain)
			if e != nil {
				return fmt.Errorf("capture_evidence: %w", e)
			}
		}
		if cap.ConfigHash != cfg.Hash() {
			continue
		}
		if !cap.Committed {
			summary.CoverageCaptures++
			summary.Incomplete++
			coverage = append(coverage, []string{cap.CaptureId.String(), cap.CaptureMode, cap.SourceId, cap.EventKind, "", "", "uncommitted", cap.Reason, "false", strconv.Itoa(int(cap.EventRows))})
			continue
		}
		var b Batch
		if useBulk {
			var ok bool
			b, ok = batches[cap.CaptureId]
			if !ok {
				return errors.New("report_batch_missing")
			}
			e = Validate(b)
		} else {
			b, e = r.KeeperBatch(ctx, cap)
		}
		if e != nil {
			return e
		}
		m, ok := manifests[cap.CaptureId]
		if !ok {
			return errors.New("report_manifest_missing")
		}
		var er error
		for _, row := range b.Events {
			if er = checkAnchor(row.BlockNumber, row.BlockHash); er != nil {
				return er
			}
		}
		for _, row := range b.Receipts {
			if er = checkAnchor(row.BlockNumber, row.BlockHash); er != nil {
				return er
			}
		}
		for _, q := range b.Costs {
			// Older combined rows recorded parameter receipt time. Keep stored
			// facts immutable and use capture completion for as-of analysis.
			if q.ObservationKind == "chain_resource" && q.Status != "error" && q.AvailableAt.Before(cap.AvailableAt) {
				q.AvailableAt = cap.AvailableAt
			}
			costs = append(costs, q)
		}
		if m.Kind == "page" && cap.EventKind == "Liquidate" && cap.RequestedFrom != nil && cap.RequestedTo != nil && (cap.Status == "complete" || cap.Reason == "pagination_continues") {
			w := windows[m.ScanID]
			if w == nil {
				w = &windowPages{From: *cap.RequestedFrom, To: *cap.RequestedTo, Edges: map[string]string{}, Valid: true}
				windows[m.ScanID] = w
			}
			if prev, ok := w.Edges[m.FingerprintIn]; ok && prev != m.FingerprintOut {
				w.Valid = false
			}
			w.Edges[m.FingerprintIn] = m.FingerprintOut
		}

		summary.CoverageCaptures++
		if cap.Status != "complete" {
			summary.Incomplete++
		}
		f, t := "", ""
		if cap.RequestedFrom != nil {
			f = cap.RequestedFrom.Format(time.RFC3339Nano)
		}
		if cap.RequestedTo != nil {
			t = cap.RequestedTo.Format(time.RFC3339Nano)
		}
		coverage = append(coverage, []string{cap.CaptureId.String(), cap.CaptureMode, cap.SourceId, cap.EventKind, f, t, cap.Status, cap.Reason, strconv.FormatBool(cap.PaginationExhausted), strconv.Itoa(int(cap.EventRows))})
		for _, ev := range b.Events {
			if ev.EventKind != "liquidate" || ev.PositionStatus != "receipt_verified" || !in(ev.BlockTime, from, to) {
				continue
			}
			key := Hex(ev.BlockHash) + ":" + Hex(ev.TxId) + ":" + strconv.Itoa(int(*ev.ReceiptLogIndex))
			if prev, ok := events[key]; ok && !EventEqual(prev, ev) {
				return errors.New("canonical_event_conflict")
			}
			events[key] = ev
		}
		for _, rr := range b.Receipts {
			key := Hex(rr.BlockHash) + ":" + Hex(rr.TxId)
			if prev, ok := receipts[key]; ok {
				rr, e = MergeReceipt(prev, rr)
				if e != nil {
					return e
				}
			}
			receipts[key] = rr
		}
		for _, p := range b.Probes {
			if in(p.AvailableAt, from, to) {
				observedStatus[p.CaptureId.String()+":"+strconv.Itoa(int(p.ProbeIndex))] = p.Status
				if (p.Status == "revert" || p.Status == "rpc_error") && p.ResponsePayloadHash != nil {
					raw, ok := responseBodies[*p.ResponsePayloadHash]
					if !ok {
						raw, er = a.Get(*p.ResponsePayloadHash)
						if er != nil {
							return er
						}
					}
					var result struct {
						Constant []string `json:"constant_result"`
						Result   struct {
							Success *bool  `json:"result"`
							Code    string `json:"code"`
							Message string `json:"message"`
						} `json:"result"`
					}
					if er = Decode(raw, &result); er != nil {
						return er
					}
					if p.Status == "revert" {
						p.Status = classifyTVMFailure(p.TvmResult, p.ErrorMessage, result.Constant)
					} else if (result.Result.Success == nil || !*result.Result.Success) && result.Result.Code == "OTHER_ERROR" {
						message, err := hex.DecodeString(result.Result.Message)
						if err == nil && vmExecutionException(string(message)) {
							p.Status = "tvm_failure"
						}
					}
				}
				probes = append(probes, p)
			}
		}
	}
	eventRows := []RentalEvent{}
	for _, ev := range events {
		eventRows = append(eventRows, ev)
	}
	sort.Slice(eventRows, func(i, j int) bool {
		a, b := eventRows[i], eventRows[j]
		if a.BlockNumber != b.BlockNumber {
			return a.BlockNumber < b.BlockNumber
		}
		if *a.TransactionIndex != *b.TransactionIndex {
			return *a.TransactionIndex < *b.TransactionIndex
		}
		return *a.ReceiptLogIndex < *b.ReceiptLogIndex
	})
	reward := big.NewInt(0)
	txs := map[string][]RentalEvent{}
	for _, ev := range eventRows {
		reward.Add(reward, ev.RewardSun)
		k := Hex(ev.BlockHash) + ":" + Hex(ev.TxId)
		txs[k] = append(txs[k], ev)
	}
	burn := big.NewInt(0)
	liquidations := [][]string{}
	for k, ee := range txs {
		rr, ok := receipts[k]
		if !ok {
			return errors.New("event_receipt_missing")
		}
		status := "unknown"
		sums := map[string]*big.Int{}
		for _, ev := range ee {
			if ev.Liquidator == nil {
				return errors.New("missing_liquidator")
			}
			if sums[*ev.Liquidator] == nil {
				sums[*ev.Liquidator] = big.NewInt(0)
			}
			sums[*ev.Liquidator].Add(sums[*ev.Liquidator], ev.RewardSun)
		}
		verified := rr.NativeTransferStatus == "present"
		for who, want := range sums {
			got := big.NewInt(0)
			for _, tr := range rr.NativeTransfers {
				if !tr.Rejected && tr.Sender == ee[0].ContractAddress && tr.Recipient == who {
					got.Add(got, &tr.AmountSun)
				}
			}
			if got.Cmp(want) != 0 {
				verified = false
			}
		}
		if verified {
			status = "matched"
			summary.RewardTransfersVerified++
		}
		if rr.FeeSun == nil {
			summary.UnknownBurn++
		} else {
			summary.KnownBurnTransactions++
			burn.Add(burn, new(big.Int).SetUint64(*rr.FeeSun))
		}
		for _, ev := range ee {
			liquidations = append(liquidations, []string{ev.BlockTime.Format(time.RFC3339Nano), Hex(ev.BlockHash), Hex(ev.TxId), strconv.Itoa(int(*ev.ReceiptLogIndex)), Hex(*ev.Liquidator), fieldBig(ev.RewardSun), status, rr.CallClass, fieldU(rr.FeeSun), fieldU(rr.EnergyUsageTotal)})
		}
	}
	sort.Slice(liquidations, func(i, j int) bool { return join(liquidations[i]) < join(liquidations[j]) })
	summary.Events = len(eventRows)
	summary.Transactions = len(txs)
	summary.GrossTRX = sun(reward)
	summary.BurnCoverage = "none"
	if summary.KnownBurnTransactions > 0 {
		summary.KnownBurnTRX = Ptr(sun(burn))
		summary.BurnCoverage = "partial"
		if summary.UnknownBurn == 0 {
			summary.BurnCoverage = "complete"
		}
	}
	summary.Probes = len(probes)
	// A row is an observed point, never another hypothetical trade or a continuous interval.
	sort.Slice(probes, func(i, j int) bool { return probes[i].AvailableAt.Before(probes[j].AvailableAt) })
	episodes := [][]string{}
	for _, p := range probes {
		observed := observedStatus[p.CaptureId.String()+":"+strconv.Itoa(int(p.ProbeIndex))]
		summary.ProbeObservedStatus[observed]++
		if observed != p.Status {
			summary.ProbeReclassified++
		}
		summary.ProbeStatus[p.Status]++
		if p.RewardReturnSun != nil && p.RewardReturnSun.Sign() > 0 && p.Status == "unknown" {
			summary.PositiveUncertified++
		}
		episodes = append(episodes, []string{p.CaptureId.String(), strconv.Itoa(int(p.ProbeIndex)), Hex(p.Renter), Hex(p.Receiver), p.ScheduledAt.Format(time.RFC3339Nano), p.AvailableAt.Format(time.RFC3339Nano), p.Status, fieldBig(p.RewardReturnSun), p.RewardConsistency, fieldU(p.EnergyUsed), p.IdentityStatus, p.StateBinding, p.ErrorCode, observed, p.TvmResult, p.ErrorMessage})
	}
	completed := []completeWindow{}
	for _, w := range windows {
		if windowComplete(*w) {
			completed = append(completed, completeWindow{w.From, w.To})
		}
	}
	if e = saveStats(out, eventRows, completed, from, to, &summary); e != nil {
		return e
	}
	if e = saveCosts(out, costs, probes); e != nil {
		return e
	}
	if e = CSV(filepath.Join(out, "coverage.csv"), []string{"capture_id", "mode", "source", "event", "from", "to", "status", "reason", "pagination_exhausted", "events"}, coverage); e != nil {
		return e
	}
	if e = CSV(filepath.Join(out, "liquidations.csv"), []string{"block_time_utc", "block_hash", "tx_id", "receipt_log_index", "liquidator_hex", "reward_sun", "reward_transfer", "call_class", "whole_tx_burn_sun_do_not_sum_per_event", "energy_usage_total"}, liquidations); e != nil {
		return e
	}
	if e = CSV(filepath.Join(out, "probe_observations.csv"), []string{"capture_id", "probe_index", "renter", "receiver", "scheduled_utc", "available_utc", "status", "reward_return_sun", "reward_consistency", "energy_used", "identity_status", "state_binding", "error_code", "observed_status", "tvm_result", "error_message"}, episodes); e != nil {
		return e
	}
	if e = CSV(filepath.Join(out, "probe_episodes.csv"), []string{"lifecycle_id", "first_observed_utc", "last_observed_utc", "certified_reward_observations", "continuous_availability"}, nil); e != nil {
		return e
	}
	return Atomic(filepath.Join(out, "summary.json"), append(JSONEvidence(summary), '\n'))
}
