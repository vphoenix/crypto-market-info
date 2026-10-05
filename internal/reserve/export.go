package reserve

import (
	"context"
	"encoding/csv"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func csvNumber(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.String()
}
func csvTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func csvFile(dir, name string, rows [][]string) error {
	f, e := os.CreateTemp(dir, ".export-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	w := csv.NewWriter(f)
	w.WriteAll(rows)
	if e = w.Error(); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), filepath.Join(dir, name))
}

// Audit coverage retains attempts, including failures/orphans. Activity and
// candidates are selected canonical committed facts with the requested finality.
// All candidates are exported, including negative and incomplete observations.
func ExportCSV(ctx context.Context, store Store, m Manifest, finalized bool, dir string) error {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	caps, e := store.ReserveCaptures(ctx, Hash(m.Hash))
	if e != nil {
		return e
	}
	chosen := selectedCaptures(caps, finalized)
	batches, e := loadBatches(ctx, store, chosen)
	if e != nil {
		return e
	}
	selected := map[string]bool{}
	for _, v := range chosen {
		selected[v.BatchId] = true
	}
	coverage := [][]string{{"manifest_hash", "capture_id", "batch_id", "kind", "mode", "from_block", "to_block", "from_hash", "to_hash", "available_at_utc", "canonical", "finality", "selected_for_report", "log_coverage", "receipt_coverage", "state_coverage", "quote_coverage", "expected_quotes", "actual_quotes", "skipped_routes", "reason"}}
	for _, c := range caps {
		coverage = append(coverage, []string{Hex(c.ManifestHash), Hex(c.CaptureId), Hex(c.BatchId), c.CaptureKind, c.CaptureMode, strconv.FormatUint(c.FromBlock, 10), strconv.FormatUint(c.ToBlock, 10), Hex(c.FromHash), Hex(c.ToHash), csvTime(c.AvailableAt), strconv.FormatBool(c.Canonical), c.Finality, strconv.FormatBool(selected[c.BatchId]), c.LogCoverage, c.ReceiptCoverage, c.StateCoverage, c.QuoteCoverage, strconv.FormatUint(uint64(c.ExpectedQuotes), 10), strconv.FormatUint(uint64(c.ActualQuotes), 10), strconv.FormatUint(uint64(c.SkippedRoutes), 10), c.Reason})
	}
	activity := [][]string{{"block", "block_hash", "tx_hash", "log_index", "folio", "event", "block_time_utc", "capture_available_at_utc", "finality", "receipt_complete", "gas_used", "effective_gas_price_wei", "calldata_hash", "log_payload_hash", "historical_permission"}}
	candidates := [][]string{{"block", "block_hash", "folio", "route_id", "quote_id", "kind", "mode", "budget_usdc_raw", "amount_in_usdc_raw", "amount_out_usdc_raw", "gross_usdc_raw", "quality", "status", "reason", "available_at_utc", "finality", "shared_pool_count", "joint_status", "joint_input_usdc_raw", "joint_output_usdc_raw", "joint_gas_internal", "joint_payload_hash", "duration"}}
	simulations := map[string]Simulation{}
	if ss, ok := store.(SimulationStore); ok {
		rows, e := ss.ReserveSimulations(ctx, Hash(m.Hash))
		if e != nil {
			return e
		}
		for _, v := range rows {
			if e = ValidateSimulation(v); e != nil {
				return e
			}
			old, ok := simulations[v.QuoteId]
			if !ok || v.AvailableAt.After(old.AvailableAt) {
				simulations[v.QuoteId] = v
			}
		}
	}
	seen := map[string]string{}
	for _, c := range chosen {
		b, ok := batches[c.BatchId]
		if !ok {
			return errors.New("export_batch_missing")
		}
		rs := map[string]dex.Receipt{}
		for _, r := range b.Receipts {
			rs[r.Hash.String()+r.TxHash.String()] = r
		}
		for _, l := range b.Logs {
			key := l.Hash.String() + l.TxHash.String() + strconv.FormatUint(uint64(l.Index), 10)
			digest := LogFact(l)
			if old, ok := seen[key]; ok {
				if old != digest {
					return errors.New("export_conflicting_log")
				}
				continue
			}
			seen[key] = digest
			gasUsed, gasPrice, calldata := "", "", ""
			r, complete := rs[l.Hash.String()+l.TxHash.String()]
			if complete {
				gasUsed = strconv.FormatUint(r.GasUsed, 10)
				gasPrice = r.GasPrice.String()
				calldata = r.CalldataHash.String()
			}
			activity = append(activity, []string{strconv.FormatUint(l.Number, 10), l.Hash.String(), l.TxHash.String(), strconv.FormatUint(uint64(l.Index), 10), l.Emitter.String(), l.Event, csvTime(l.Time), csvTime(c.AvailableAt), c.Finality, strconv.FormatBool(complete), gasUsed, gasPrice, calldata, l.Payload.String(), "unknown_not_reconstructed"})
		}
		for _, q := range b.Quotes {
			if q.RouteKind == "cost_reference" {
				continue
			}
			gross := ""
			if q.Quality != "incomplete" && q.AmountInRaw != nil && q.AmountOutRaw != nil {
				gross = new(big.Int).Sub(q.AmountOutRaw, q.AmountInRaw).String()
			}
			simStatus, simIn, simOut, simGas, simPayload := "unobserved", "", "", "", ""
			if s, ok := simulations[q.QuoteId]; ok {
				if !SimulationMatchesQuote(s, q) {
					return errors.New("simulation_quote_anchor_mismatch")
				}
				simStatus = s.Status
				simPayload = Hex(s.PayloadHash)
				if s.Status == "joint_call_simulated" {
					simIn = csvNumber(s.AmountInRaw)
					simOut = csvNumber(s.AmountOutRaw)
					if s.GasInternal != nil {
						simGas = strconv.FormatUint(*s.GasInternal, 10)
					}
				}
			}
			candidates = append(candidates, []string{strconv.FormatUint(q.BlockNumber, 10), Hex(q.BlockHash), Hex(q.Folio), Hex(q.RouteId), Hex(q.QuoteId), q.RouteKind, c.CaptureMode, csvNumber(q.RequestedBudgetRaw), csvNumber(q.AmountInRaw), csvNumber(q.AmountOutRaw), gross, q.Quality, q.Status, q.Reason, csvTime(q.AvailableAt), c.Finality, strconv.Itoa(len(q.SharedPools)), simStatus, simIn, simOut, simGas, simPayload, "unknown_sampling_gaps"})
		}
	}
	if e = csvFile(dir, "coverage.csv", coverage); e != nil {
		return e
	}
	if e = csvFile(dir, "activity.csv", activity); e != nil {
		return e
	}
	return csvFile(dir, "candidates.csv", candidates)
}
