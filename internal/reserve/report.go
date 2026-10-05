package reserve

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"
)

type Candidate struct {
	BlockTime      time.Time `json:"block_time"`
	Finality       string    `json:"finality"`
	CaptureMode    string    `json:"capture_mode"`
	GasBasis       string    `json:"gas_basis,omitempty"`
	GasRequiredWei *string   `json:"gas_required_wei,omitempty"`
	GasBucketWei   *string   `json:"gas_bucket_wei,omitempty"`
	Block          uint64    `json:"block"`
	BlockHash      string    `json:"block_hash"`
	Folio          string    `json:"folio"`
	Kind           string    `json:"kind"`
	Quality        string    `json:"quality"`
	AvailableAt    time.Time `json:"available_at"`
	AmountIn       string    `json:"amount_in_usdc"`
	AmountOut      string    `json:"amount_out_usdc"`
	Gross          string    `json:"gross_usdc"`
	Net            *string   `json:"net_usdc,omitempty"`
	GasCost        *string   `json:"gas_cost_usdc,omitempty"`
	CostStatus     string    `json:"cost_status"`
}
type Report struct {
	GeneratedAt       time.Time        `json:"generated_at"`
	Manifest          string           `json:"manifest_hash"`
	RawResponsePolicy string           `json:"raw_response_policy"`
	Captures          int              `json:"captures"`
	CanonicalCaptures int              `json:"canonical_captures"`
	FinalizedCaptures int              `json:"finalized_captures"`
	LiveSnapshots     int              `json:"live_snapshots"`
	CompleteStates    int              `json:"complete_states"`
	IncompleteStates  int              `json:"incomplete_states"`
	LogRanges         int              `json:"log_ranges"`
	MissingLogRanges  int              `json:"missing_log_ranges"`
	UniqueLogs        int              `json:"unique_logs"`
	UniqueReceipts    int              `json:"unique_receipts"`
	FirstBlock        *uint64          `json:"first_block,omitempty"`
	LastBlock         *uint64          `json:"last_block,omitempty"`
	LastAvailableAt   *time.Time       `json:"last_available_at,omitempty"`
	QuoteQuality      map[string]int   `json:"quote_quality"`
	Reasons           map[string]int   `json:"reasons"`
	Events            map[string]int   `json:"events"`
	Candidates        []Candidate      `json:"positive_observations"`
	Coverage          []Coverage       `json:"log_coverage_scopes"`
	LegFailureCauses  map[string]int   `json:"failed_leg_observations"`
	Simulations       []SimObservation `json:"joint_simulations"`
	Notes             []string         `json:"notes"`
}

func decimal6(n *big.Int) string { return new(big.Rat).SetFrac(n, Uint(1_000_000)).FloatString(6) }

// GasBucket uses matching or larger exact-output reference quantities. It never
// scales a nonlinear Quoter result or substitutes a smaller quantity.
func GasBucket(quotes []Quote, wei *big.Int) (*big.Int, bool) {
	var best *Quote
	for i := range quotes {
		q := &quotes[i]
		if q.RouteKind != "cost_reference" || q.TokenIn != Address(USDC) || q.TokenOut != Address(WETH) || q.Status != "ok" || q.AmountOutRaw == nil || q.AmountOutRaw.Cmp(wei) < 0 {
			continue
		}
		if best == nil || q.AmountOutRaw.Cmp(best.AmountOutRaw) < 0 {
			best = q
		}
	}
	if best == nil {
		return nil, false
	}
	return Copy(best.AmountInRaw), true
}
func BuildReport(ctx context.Context, store Store, m Manifest, finalizedOnly bool, gasUnits uint64, tipWei *big.Int) (Report, error) {
	report := Report{GeneratedAt: time.Now().UTC(), Manifest: m.Hash.String(), QuoteQuality: map[string]int{}, Reasons: map[string]int{}, Events: map[string]int{}, Candidates: []Candidate{}, Notes: []string{"Counts are observations, not independent trades or daily profit. Sampling gaps cannot establish opportunity duration. For the same immutable canonical hash, a complete earlier attempt is retained when a later attempt fails; its original available_at is preserved.", "Independent shared-pool quotes remain indicative_overlap. joint_simulations separately record sequential eth_call validation, synthetic funding and internal gas only; no realized profit or full inclusion/MEV/USDT costs.", "Historical event permissions and upgrades are not reconstructed; raw events and full receipts are retained. No historical executable-opportunity count.", "Net values, when supplied, subtract an explicit assumed transaction gas budget only. USDT capital conversion, hedge, MEV and inclusion costs remain unevaluated."}}
	report.RawResponsePolicy = "not_retained"
	report.Notes = append(report.Notes, "Source hashes are digests, not retrievable raw-response files. Typed receipt logs/calldata and batch member digests are validated from the database.", "missing_log_ranges counts selected incomplete range attempts, not currently missing heights; use log_coverage_scopes.missing_blocks and gaps for actual coverage.")
	caps, e := store.ReserveCaptures(ctx, Hash(m.Hash))
	if e != nil {
		return report, e
	}
	report.Captures = len(caps)
	for _, c := range caps {
		if c.Canonical && c.Committed {
			report.CanonicalCaptures++
			if c.Finality == "finalized" {
				report.FinalizedCaptures++
			}
		}
	}
	ordered := selectedCaptures(caps, finalizedOnly)
	loaded, e := loadBatches(ctx, store, ordered)
	if e != nil {
		return report, e
	}
	report.Coverage = CoverageScopes(caps, finalizedOnly)
	report.LegFailureCauses = map[string]int{}
	type selectedQuote struct {
		quote   Quote
		capture Capture
	}
	selectedQuotes := map[string]selectedQuote{}
	logs := map[string]string{}
	receipts := map[string]bool{}
	for _, c := range ordered {
		if report.FirstBlock == nil || c.FromBlock < *report.FirstBlock {
			report.FirstBlock = Ptr(c.FromBlock)
		}
		if report.LastBlock == nil || c.ToBlock > *report.LastBlock {
			report.LastBlock = Ptr(c.ToBlock)
		}
		if report.LastAvailableAt == nil || c.AvailableAt.After(*report.LastAvailableAt) {
			report.LastAvailableAt = Ptr(c.AvailableAt)
		}
		if c.Reason != "" {
			report.Reasons[c.Reason]++
		}
		if c.CaptureKind == "logs" {
			report.LogRanges++
			if c.LogCoverage != "complete" {
				report.MissingLogRanges++
			}
		}
		if c.CaptureKind == "snapshot" && c.CaptureMode == "live" {
			report.LiveSnapshots++
		}
		b, ok := loaded[c.BatchId]
		if !ok {
			return report, errors.New("bulk_batch_missing")
		}

		for _, s := range b.States {
			if s.StateComplete {
				report.CompleteStates++
			} else {
				report.IncompleteStates++
				report.Reasons[s.Reason]++
			}
		}
		for _, l := range b.Logs {
			key := l.Hash.String() + l.TxHash.String() + fmt.Sprint(l.Index)
			digest := LogFact(l)
			if old, ok := logs[key]; ok && old != digest {
				return report, errors.New("conflicting_log_fact")
			}
			if _, ok := logs[key]; !ok {
				logs[key] = digest
				report.Events[l.Event]++
			}
		}
		for _, r := range b.Receipts {
			receipts[r.Hash.String()+r.TxHash.String()] = true
		}
		for _, q := range b.Quotes {
			selectedQuotes[q.QuoteId] = selectedQuote{q, c}
			for _, l := range q.DexLegs {
				if l.Status != "ok" {
					report.LegFailureCauses[l.Status]++
				}
			}
			if q.RouteKind == "cost_reference" {
				continue
			}
			report.QuoteQuality[q.Quality]++
			if q.Reason != "" {
				report.Reasons[q.Reason]++
			}
			if q.Quality == "incomplete" || q.AmountInRaw == nil || q.AmountOutRaw == nil {
				continue
			}
			gross := new(big.Int).Sub(q.AmountOutRaw, q.AmountInRaw)
			if gross.Sign() <= 0 {
				continue
			}
			candidate := Candidate{BlockTime: q.BlockTime, Finality: c.Finality, CaptureMode: c.CaptureMode, Block: q.BlockNumber, BlockHash: Hex(q.BlockHash), Folio: Hex(q.Folio), Kind: q.RouteKind, Quality: q.Quality, AvailableAt: q.AvailableAt, AmountIn: decimal6(q.AmountInRaw), AmountOut: decimal6(q.AmountOutRaw), Gross: decimal6(gross), CostStatus: "unknown_transaction_gas"}
			if gasUnits > 0 {
				var fee *big.Int
				for _, s := range b.States {
					if s.Folio == q.Folio {
						fee = Copy(s.BaseFeeWei)
					}
				}
				if fee != nil {
					fee.Add(fee, tipWei)
					wei := new(big.Int).Mul(fee, new(big.Int).SetUint64(gasUnits))
					cost, ok := GasBucket(b.Quotes, wei)
					if ok {
						candidate.GasCost = Ptr(decimal6(cost))
						candidate.GasRequiredWei = Ptr(wei.String())
						for _, ref := range b.Quotes {
							if ref.RouteKind == "cost_reference" && ref.TokenIn == Address(USDC) && ref.TokenOut == Address(WETH) && ref.AmountInRaw != nil && ref.AmountInRaw.Cmp(cost) == 0 {
								candidate.GasBucketWei = Ptr(ref.AmountOutRaw.String())
								candidate.GasBasis = "conservatively_bucketed"
								if ref.AmountOutRaw.Cmp(wei) == 0 {
									candidate.GasBasis = "exact_bucket"
								}
								break
							}
						}
						candidate.Net = Ptr(decimal6(new(big.Int).Sub(gross, cost)))
						candidate.CostStatus = "assumed_gas_only_other_costs_unknown"
					} else {
						candidate.CostStatus = "gas_reference_bucket_missing"
					}
				}
			}
			report.Candidates = append(report.Candidates, candidate)
		}
	}
	if simStore, ok := store.(SimulationStore); ok {
		rows, e := simStore.ReserveSimulations(ctx, Hash(m.Hash))
		if e != nil {
			return report, e
		}
		for _, s := range rows {
			if e = ValidateSimulation(s); e != nil {
				return report, e
			}
			if selected, ok := selectedQuotes[s.QuoteId]; ok {
				q := selected.quote
				if !SimulationMatchesQuote(s, q) {
					return report, errors.New("simulation_quote_anchor_mismatch")
				}
				observation := SimulationObservation(s)
				observation.Finality = selected.capture.Finality
				observation.CaptureMode = selected.capture.CaptureMode
				report.Simulations = append(report.Simulations, observation)
			}
		}
	}
	report.UniqueLogs = len(logs)
	report.UniqueReceipts = len(receipts)
	sort.Slice(report.Candidates, func(i, j int) bool {
		a, _ := new(big.Rat).SetString(report.Candidates[i].Gross)
		b, _ := new(big.Rat).SetString(report.Candidates[j].Gross)
		return a.Cmp(b) > 0
	})
	if len(report.Candidates) > 20 {
		report.Candidates = report.Candidates[:20]
	}
	return report, nil
}
