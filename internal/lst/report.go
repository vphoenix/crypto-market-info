package lst

import (
	"bytes"
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

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type reportQuote struct {
	quote      Quote
	capture    Capture
	protocolOK bool
}
type reportRange struct{ from, to uint64 }
type reportSummary struct {
	ManifestHash              string   `json:"manifest_hash"`
	From                      string   `json:"from_inclusive_utc"`
	To                        string   `json:"to_exclusive_utc"`
	Captures                  int      `json:"capture_count"`
	ExcludedCaptures          int      `json:"excluded_capture_count"`
	QuoteRows                 int      `json:"quote_rows"`
	CurrentGrossQuotes        int      `json:"current_gross_discount_quotes"`
	RequestCohort             int      `json:"request_cohort_count"`
	MatchedFinalizations      int      `json:"matched_finalizations_with_continuous_coverage"`
	RightCensored             int      `json:"right_censored_or_unresolved_requests"`
	LeftCensored              int      `json:"left_censored_claims"`
	FundingSettlements        int      `json:"actual_funding_settlements"`
	ConditionalGrossScenarios int      `json:"conditional_gross_scenarios"`
	GasSampleTransactions     int      `json:"gas_sample_transactions"`
	GasSampleTotalWei         string   `json:"gas_sample_total_wei"`
	ProfitStatus              string   `json:"profit_status"`
	Limitations               []string `json:"limitations"`
}

// Report exports only stored public evidence. It never fetches missing evidence,
// assigns zero to unknown costs, or treats fixed-delay observations as redemption.
func Report(ctx context.Context, store Store, manifest Manifest, from, to time.Time, outDir string) error {
	from = from.UTC()
	to = to.UTC()
	if !to.After(from) || len(manifest.Hash) != 32 {
		return errors.New("invalid_report_range_or_manifest")
	}
	captures, e := store.LSTCaptures(ctx, manifest.Hash)
	if e != nil {
		return e
	}
	latest := map[uuid.UUID]Capture{}
	for _, c := range captures {
		if old, ok := latest[c.CaptureId]; !ok || c.Revision > old.Revision {
			latest[c.CaptureId] = c
		} else if c.Revision == old.Revision && CanonicalHash(c) != CanonicalHash(old) {
			return errors.New("conflicting_capture_revision")
		}
	}
	captures = nil
	for _, c := range latest {
		captures = append(captures, c)
	}
	sort.Slice(captures, func(i, j int) bool {
		if captures[i].StartedAt.Equal(captures[j].StartedAt) {
			return captures[i].CaptureId.String() < captures[j].CaptureId.String()
		}
		return captures[i].StartedAt.Before(captures[j].StartedAt)
	})
	summary := reportSummary{ManifestHash: Hex(manifest.Hash), From: reportTime(from), To: reportTime(to), ProfitStatus: "unknown_costs_and_funding_coverage", Limitations: []string{"Amounts and prices are observations, not executed fills.", "Current gross discount excludes all gas, trading fees, redemption losses, hedge funding and capital costs.", "Fixed-delay followups do not establish our actual redemption time or amount.", "Actual funding observations are exported; historical settlement schedule and full holding-period coverage are not certified.", "Net profit, APR and capacity across overlapping routes/time samples are not estimated.", "Gas statistics cover only sampled known receipts; they are not total strategy execution cost."}}
	coverage := [][]string{{"capture_id", "kind", "started_at", "status", "finality", "canonical", "committed", "report_eligibility", "reason"}}
	quotes := map[string]reportQuote{}
	requestEvents := map[string]WithdrawalRequest{}
	finalEvents := map[string]WithdrawalFinalization{}
	claimEvents := map[string]WithdrawalClaim{}
	funding := map[string]FundingSettlement{}
	ranges := []reportRange{}
	for _, c := range captures {
		if e := ctx.Err(); e != nil {
			return e
		}
		inWindow := reportWithin(c.StartedAt, from, to)
		eligibility := "accepted"
		reason := c.Reason
		if c.ManifestHash != manifest.Hash || !c.Committed {
			eligibility = "uncommitted_or_other_manifest"
		} else if !c.Canonical {
			eligibility = "orphaned"
		} else if c.CaptureKind != "funding" && c.Finality != "finalized" {
			eligibility = "pending_finality"
		} else if c.Status == "failed" {
			eligibility = "failed"
		}
		if inWindow {
			summary.Captures++
			if eligibility != "accepted" {
				summary.ExcludedCaptures++
			}
			coverage = append(coverage, []string{c.CaptureId.String(), c.CaptureKind, reportTime(c.StartedAt), c.Status, c.Finality, strconv.FormatBool(c.Canonical), strconv.FormatBool(c.Committed), eligibility, reason})
		}
		if eligibility != "accepted" {
			continue
		}
		b, e := store.LSTBatch(ctx, c)
		if e != nil {
			return fmt.Errorf("read capture %s: %w", c.CaptureId, e)
		}
		if e = Validate(b); e != nil {
			return fmt.Errorf("validate capture %s: %w", c.CaptureId, e)
		}
		switch c.CaptureKind {
		case "market":
			protocolOK := len(b.Protocols) == 1 && b.Protocols[0].IdentityOk && b.Protocols[0].StateStatus == "ok"
			for _, q := range b.Quotes {
				if old, ok := quotes[q.QuoteId]; ok && old.quote.RowHash != q.RowHash {
					return errors.New("conflicting_quote_identity")
				}
				quotes[q.QuoteId] = reportQuote{q, c, protocolOK}
			}
		case "logs":
			if c.Status != "complete" {
				continue
			}
			ranges = append(ranges, reportRange{*c.FromBlock, *c.ToBlock})
			for _, r := range b.Requests {
				k := reportEventKey(r.ChainId, r.QueueAddress, r.BlockHash, r.TransactionHash, r.LogIndex)
				if old, ok := requestEvents[k]; ok {
					if reportEventFact(old) != reportEventFact(r) {
						return errors.New("conflicting_request_event")
					}
					if e = reportReceiptConflict(old, r); e != nil {
						return e
					}
					if old.GasUsed != nil && r.GasUsed == nil {
						continue
					}
				}
				requestEvents[k] = r
			}
			for _, r := range b.Finalizations {
				k := reportEventKey(r.ChainId, r.QueueAddress, r.BlockHash, r.TransactionHash, r.LogIndex)
				if old, ok := finalEvents[k]; ok && reportEventFact(old) != reportEventFact(r) {
					return errors.New("conflicting_finalization_event")
				}
				finalEvents[k] = r
			}
			for _, r := range b.Claims {
				k := reportEventKey(r.ChainId, r.QueueAddress, r.BlockHash, r.TransactionHash, r.LogIndex)
				if old, ok := claimEvents[k]; ok {
					if reportEventFact(old) != reportEventFact(r) {
						return errors.New("conflicting_claim_event")
					}
					if e = reportReceiptConflict(old, r); e != nil {
						return e
					}
					if old.GasUsed != nil && r.GasUsed == nil {
						continue
					}
				}
				claimEvents[k] = r
			}
		case "funding":
			for _, f := range b.Funding {
				k := CanonicalHash([]any{f.InstrumentId, f.FundingTime})
				if old, ok := funding[k]; ok {
					if !old.FundingRate.Equal(f.FundingRate) || old.SettlementMarkPriceTickE8 != nil && f.SettlementMarkPriceTickE8 != nil && *old.SettlementMarkPriceTickE8 != *f.SettlementMarkPriceTickE8 {
						return errors.New("conflicting_actual_funding")
					}
					if old.SettlementMarkPriceTickE8 != nil && f.SettlementMarkPriceTickE8 == nil {
						continue
					}
				}
				funding[k] = f
			}
		}
	}
	ranges = reportMergeRanges(ranges)
	quoteRows := [][]string{append(reportColumns(reflect.TypeOf(Quote{})), "capture_status", "chain_gross_quote_usable", "gross_discount_usdt", "gross_discount_fraction", "profit_status")}
	orderedQuotes := make([]reportQuote, 0, len(quotes))
	for _, q := range quotes {
		orderedQuotes = append(orderedQuotes, q)
	}
	sort.Slice(orderedQuotes, func(i, j int) bool {
		a, b := orderedQuotes[i].quote, orderedQuotes[j].quote
		if a.ObservedAt.Equal(b.ObservedAt) {
			return a.QuoteId < b.QuoteId
		}
		return a.ObservedAt.Before(b.ObservedAt)
	})
	for _, record := range orderedQuotes {
		q := record.quote
		if !reportWithin(q.ObservedAt, from, to) {
			continue
		}
		summary.QuoteRows++
		usable := reportGrossUsable(record)
		gross, fraction := "", ""
		if usable {
			g := new(big.Int).Sub(q.EthExitUsdtOutRaw, q.PurchaseBudgetUsdtRaw)
			gross = decimal.NewFromBigInt(g, -6).String()
			fraction = new(big.Rat).SetFrac(g, q.PurchaseBudgetUsdtRaw).RatString()
			summary.CurrentGrossQuotes++
		}
		quoteRows = append(quoteRows, append(reportFields(q), record.capture.Status, strconv.FormatBool(usable), gross, fraction, "unknown_costs_and_redemption"))
	}
	requests := map[string]WithdrawalRequest{}
	claims := map[string]WithdrawalClaim{}
	for _, r := range requestEvents {
		k := reportRequestKey(r.ChainId, r.QueueAddress, r.RequestId)
		if old, ok := requests[k]; ok && reportEventFact(old) != reportEventFact(r) {
			return errors.New("request_id_has_multiple_canonical_events")
		}
		requests[k] = r
	}
	for _, r := range claimEvents {
		k := reportRequestKey(r.ChainId, r.QueueAddress, r.RequestId)
		if old, ok := claims[k]; ok && reportEventFact(old) != reportEventFact(r) {
			return errors.New("request_id_has_multiple_claims")
		}
		claims[k] = r
	}
	finals := []WithdrawalFinalization{}
	for _, f := range finalEvents {
		finals = append(finals, f)
	}
	sort.Slice(finals, func(i, j int) bool { return finals[i].FromRequestId.Cmp(finals[j].FromRequestId) < 0 })
	withdrawalRows := [][]string{{"chain_id", "queue_address", "request_id", "request_block_time", "requested_steth_wei", "requested_shares_raw", "finalized_at", "claimed_at", "claimed_eth_wei", "request_to_finalized_seconds", "finalized_to_claim_seconds", "request_to_claim_seconds", "elapsed_since_request_at_report_cutoff_seconds", "status", "claim_ratio_exact", "request_tx_hash", "claim_tx_hash", "request_gas_used", "request_gas_price_wei", "claim_gas_used", "claim_gas_price_wei"}}
	orderedRequests := []WithdrawalRequest{}
	for _, r := range requests {
		orderedRequests = append(orderedRequests, r)
	}
	sort.Slice(orderedRequests, func(i, j int) bool { return orderedRequests[i].RequestId.Cmp(orderedRequests[j].RequestId) < 0 })
	for _, r := range orderedRequests {
		inCohort := reportWithin(r.BlockTime, from, to)
		contextInWindow := false
		if claim, ok := claims[reportRequestKey(r.ChainId, r.QueueAddress, r.RequestId)]; ok && reportWithin(claim.BlockTime, from, to) {
			contextInWindow = true
		}
		for _, f := range finals {
			if f.ChainId == r.ChainId && f.QueueAddress == r.QueueAddress && r.RequestId.Cmp(f.FromRequestId) >= 0 && r.RequestId.Cmp(f.ToRequestId) <= 0 && reportWithin(f.BlockTime, from, to) {
				contextInWindow = true
			}
		}
		if !inCohort && !contextInWindow {
			continue
		}
		if inCohort {
			summary.RequestCohort++
		}
		row := []string{strconv.FormatUint(r.ChainId, 10), Hex(r.QueueAddress), r.RequestId.String(), reportTime(r.BlockTime), r.AmountStethWei.String(), r.AmountSharesRaw.String(), "", "", "", "", "", "", reportDuration(to.Sub(r.BlockTime)), "right_censored_coverage_unknown", "", Hex(r.TransactionHash), "", reportOptional(r.GasUsed), reportOptional(r.EffectiveGasPriceWei), "", ""}
		matching := []WithdrawalFinalization{}
		for _, f := range finals {
			if f.ChainId == r.ChainId && f.QueueAddress == r.QueueAddress && r.RequestId.Cmp(f.FromRequestId) >= 0 && r.RequestId.Cmp(f.ToRequestId) <= 0 && f.BlockTime.Before(to) {
				matching = append(matching, f)
			}
		}
		var final *WithdrawalFinalization
		if len(matching) > 1 {
			row[13] = "overlapping_finalization_ranges"
		} else if len(matching) == 1 {
			f := matching[0]
			if f.BlockNumber >= r.BlockNumber && !f.BlockTime.Before(r.BlockTime) && reportCovered(ranges, r.BlockNumber, f.BlockNumber) {
				final = &f
				row[6] = reportTime(f.BlockTime)
				row[9] = reportDuration(f.BlockTime.Sub(r.BlockTime))
				row[12] = ""
				row[13] = "finalized_unclaimed"
				if inCohort {
					summary.MatchedFinalizations++
				}
			} else {
				row[13] = "finalization_coverage_unknown"
			}
		}
		claim, hasClaim := claims[reportRequestKey(r.ChainId, r.QueueAddress, r.RequestId)]
		if hasClaim && claim.BlockTime.Before(to) {
			row[7] = reportTime(claim.BlockTime)
			row[8] = claim.AmountEthWei.String()
			row[16] = Hex(claim.TransactionHash)
			row[19] = reportOptional(claim.GasUsed)
			row[20] = reportOptional(claim.EffectiveGasPriceWei)
			if !claim.BlockTime.Before(r.BlockTime) {
				row[11] = reportDuration(claim.BlockTime.Sub(r.BlockTime))
				if r.AmountStethWei.Sign() > 0 {
					row[14] = new(big.Rat).SetFrac(claim.AmountEthWei, r.AmountStethWei).RatString()
				}
			}
			if final != nil && claim.BlockNumber >= final.BlockNumber && !claim.BlockTime.Before(final.BlockTime) && reportCovered(ranges, final.BlockNumber, claim.BlockNumber) {
				row[10] = reportDuration(claim.BlockTime.Sub(final.BlockTime))
				row[13] = "claimed"
			} else {
				row[13] = "claim_with_finalization_or_coverage_unknown"
			}
		}
		if final == nil && inCohort {
			summary.RightCensored++
		}
		if !inCohort {
			row[13] = "context_request_before_window:" + row[13]
		}
		withdrawalRows = append(withdrawalRows, row)
	}
	for _, claim := range claims {
		if !reportWithin(claim.BlockTime, from, to) {
			continue
		}
		key := reportRequestKey(claim.ChainId, claim.QueueAddress, claim.RequestId)
		if _, ok := requests[key]; ok {
			continue
		}
		summary.LeftCensored++
		row := make([]string, 21)
		row[0] = strconv.FormatUint(claim.ChainId, 10)
		row[1] = Hex(claim.QueueAddress)
		row[2] = claim.RequestId.String()
		row[7] = reportTime(claim.BlockTime)
		row[8] = claim.AmountEthWei.String()
		row[13] = "left_censored_request_missing"
		row[16] = Hex(claim.TransactionHash)
		row[19] = reportOptional(claim.GasUsed)
		row[20] = reportOptional(claim.EffectiveGasPriceWei)
		withdrawalRows = append(withdrawalRows, row)
	}
	fundingRows := [][]string{append(reportColumns(reflect.TypeOf(FundingSettlement{})), "short_cashflow_per_eth_usdt", "holding_interval_coverage")}
	orderedFunding := []FundingSettlement{}
	for _, f := range funding {
		orderedFunding = append(orderedFunding, f)
	}
	sort.Slice(orderedFunding, func(i, j int) bool { return orderedFunding[i].FundingTime.Before(orderedFunding[j].FundingTime) })
	for _, f := range orderedFunding {
		if !reportWithin(f.FundingTime, from, to) {
			continue
		}
		cashflow := ""
		if f.SettlementMarkPriceTickE8 != nil {
			cashflow = f.FundingRate.Mul(decimal.New(*f.SettlementMarkPriceTickE8, -8)).String()
		}
		fundingRows = append(fundingRows, append(reportFields(f), cashflow, "unknown_historical_settlement_schedule"))
		summary.FundingSettlements++
	}
	scenarios := [][]string{{"entry_quote_id", "followup_quote_id", "entry_at", "followup_at", "target_delay_seconds", "actual_delay_seconds", "entry_budget_usdt", "same_eth_exit_wei", "same_hedge_eth_wei", "gross_cashflow_before_funding_and_costs_usdt", "observed_partial_funding_cashflow_usdt", "funding_coverage", "net_profit_usdt", "annualized_return", "status"}}
	for _, follow := range orderedQuotes {
		q := follow.quote
		if q.QuoteRole != "followup" || q.ReferenceQuoteId == nil || !reportWithin(q.ObservedAt, from, to) {
			continue
		}
		entry, ok := quotes[*q.ReferenceQuoteId]
		status := "missing_entry_or_incomplete_quote"
		row := []string{Hex(*q.ReferenceQuoteId), Hex(q.QuoteId), "", reportTime(q.ObservedAt), reportOptional(q.TargetDelaySeconds), "", "", "", "", "", "", "unknown_historical_settlement_schedule", "", "", status}
		if ok {
			p := entry.quote
			row[2] = reportTime(p.ObservedAt)
			row[5] = reportDuration(q.ObservedAt.Sub(p.ObservedAt))
			row[6] = reportScaled(p.PurchaseBudgetUsdtRaw, -6)
			if reportGrossUsable(entry) && reportFollowUsable(follow) && p.HedgeStatus == "ok" && p.HedgeSellNotionalUsdtE8 != nil && p.HedgeEthWei != nil && q.HedgeEthWei != nil && p.NominalRedeemEthWei.Cmp(q.EthExitInputWei) == 0 && p.HedgeEthWei.Cmp(q.HedgeEthWei) == 0 && p.HedgeInstrumentId == q.HedgeInstrumentId && p.RouteId == q.RouteId && p.LstAddress == q.LstAddress && q.NominalRedeemEthWei != nil && p.NominalRedeemEthWei.Cmp(q.NominalRedeemEthWei) == 0 && !q.ObservedAt.Before(p.ObservedAt) {
				gross := decimal.NewFromBigInt(q.EthExitUsdtOutRaw, -6).Sub(decimal.NewFromBigInt(p.PurchaseBudgetUsdtRaw, -6)).Add(decimal.NewFromBigInt(p.HedgeSellNotionalUsdtE8, -8)).Sub(decimal.NewFromBigInt(q.HedgeBuyNotionalUsdtE8, -8))
				row[7] = q.EthExitInputWei.String()
				row[8] = q.HedgeEthWei.String()
				row[9] = gross.String()
				row[14] = "conditional_gross_only"
				sum := decimal.Zero
				observed := 0
				for _, f := range orderedFunding {
					if f.InstrumentId == p.HedgeInstrumentId && f.FundingTime.After(p.ObservedAt) && !f.FundingTime.After(q.ObservedAt) && f.SettlementMarkPriceTickE8 != nil {
						sum = sum.Add(decimal.NewFromBigInt(p.HedgeEthWei, -18).Mul(decimal.New(*f.SettlementMarkPriceTickE8, -8)).Mul(f.FundingRate))
						observed++
					}
				}
				if observed > 0 {
					row[10] = sum.String()
				}
				summary.ConditionalGrossScenarios++
			}
		}
		scenarios = append(scenarios, row)
	}
	gasCosts := map[string]*big.Int{}
	addGas := func(chain uint64, block, tx string, at time.Time, gas *uint64, price *big.Int) error {
		if gas == nil || price == nil || !reportWithin(at, from, to) {
			return nil
		}
		key := CanonicalHash([]any{chain, block, tx})
		cost := new(big.Int).Mul(new(big.Int).SetUint64(*gas), price)
		if old, ok := gasCosts[key]; ok && old.Cmp(cost) != 0 {
			return errors.New("conflicting_sampled_transaction_cost")
		}
		gasCosts[key] = cost
		return nil
	}
	for _, r := range requestEvents {
		if e = addGas(r.ChainId, r.BlockHash, r.TransactionHash, r.BlockTime, r.GasUsed, r.EffectiveGasPriceWei); e != nil {
			return e
		}
	}
	for _, r := range claimEvents {
		if e = addGas(r.ChainId, r.BlockHash, r.TransactionHash, r.BlockTime, r.GasUsed, r.EffectiveGasPriceWei); e != nil {
			return e
		}
	}
	totalGas := new(big.Int)
	for _, cost := range gasCosts {
		totalGas.Add(totalGas, cost)
	}
	summary.GasSampleTransactions = len(gasCosts)
	if len(gasCosts) > 0 {
		summary.GasSampleTotalWei = totalGas.String()
	}
	if e = os.MkdirAll(outDir, 0700); e != nil {
		return e
	}
	for _, file := range []struct {
		name string
		rows [][]string
	}{{"coverage.csv", coverage}, {"quotes.csv", quoteRows}, {"withdrawals.csv", withdrawalRows}, {"funding.csv", fundingRows}, {"scenarios.csv", scenarios}} {
		if e = reportCSV(filepath.Join(outDir, file.name), file.rows); e != nil {
			return e
		}
	}
	raw, e := json.MarshalIndent(summary, "", "  ")
	if e != nil {
		return e
	}
	return reportAtomic(filepath.Join(outDir, "summary.json"), append(raw, '\n'))
}
func reportWithin(t, from, to time.Time) bool { return !t.Before(from) && t.Before(to) }
func reportTime(t time.Time) string           { return t.UTC().Format(time.RFC3339Nano) }
func reportDuration(d time.Duration) string   { return decimal.New(d.Microseconds(), -6).String() }
func reportOptional(v any) string {
	r := reflect.ValueOf(v)
	if !r.IsValid() || r.Kind() == reflect.Pointer && r.IsNil() {
		return ""
	}
	if r.Kind() == reflect.Pointer {
		return reportOptional(r.Elem().Interface())
	}
	switch x := v.(type) {
	case big.Int:
		return x.String()
	case time.Time:
		return reportTime(x)
	case decimal.Decimal:
		return x.String()
	}
	return fmt.Sprint(v)
}
func reportScaled(n *big.Int, scale int32) string {
	if n == nil {
		return ""
	}
	return decimal.NewFromBigInt(n, scale).String()
}
func reportColumns(t reflect.Type) []string {
	out := []string{}
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Tag.Get("ch"))
	}
	return out
}
func reportFields(row any) []string {
	v := reflect.ValueOf(row)
	out := make([]string, v.NumField())
	for i := range out {
		f := v.Field(i)
		tag := v.Type().Field(i).Tag.Get("lst")
		if f.Kind() == reflect.Pointer {
			if f.IsNil() {
				continue
			}
			f = f.Elem()
		}
		if strings.Contains(tag, "FixedString(") {
			if f.Kind() == reflect.Slice {
				parts := []string{}
				for j := 0; j < f.Len(); j++ {
					parts = append(parts, Hex(f.Index(j).String()))
				}
				out[i] = strings.Join(parts, ";")
			} else {
				out[i] = Hex(f.String())
			}
		} else {
			out[i] = reportOptional(f.Interface())
		}
	}
	return out
}
func reportCSV(path string, rows [][]string) error {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	w.WriteAll(rows)
	if e := w.Error(); e != nil {
		return e
	}
	return reportAtomic(path, b.Bytes())
}
func reportAtomic(path string, b []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".lst-report-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(name, path)
}
func reportGrossUsable(r reportQuote) bool {
	q := r.quote
	return r.protocolOK && q.QuoteRole == "entry" && q.TimingStatus == "fresh" && q.BuyStatus == "ok" && q.ConversionStatus == "ok" && q.ExitStatus == "ok" && q.PurchaseBudgetUsdtRaw != nil && q.PurchaseBudgetUsdtRaw.Sign() > 0 && q.EthExitUsdtOutRaw != nil && q.NominalRedeemEthWei != nil
}
func reportFollowUsable(r reportQuote) bool {
	q := r.quote
	return r.protocolOK && q.TimingStatus == "fresh" && q.ExitStatus == "ok" && q.HedgeStatus == "ok" && q.EthExitInputWei != nil && q.EthExitUsdtOutRaw != nil && q.HedgeBuyNotionalUsdtE8 != nil
}
func reportEventKey(chain uint64, queue, block, tx string, index uint32) string {
	return CanonicalHash([]any{chain, queue, block, tx, index})
}
func reportRequestKey(chain uint64, queue string, id *big.Int) string {
	return CanonicalHash([]any{chain, queue, id})
}
func reportEventFact(row any) string {
	v := reflect.ValueOf(row)
	r := reflect.New(v.Type()).Elem()
	r.Set(v)
	for _, name := range []string{"CaptureId", "EventPayloadHash", "AvailableAt", "ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasSampleClass", "GasUsed", "EffectiveGasPriceWei", "ReceiptPayloadHash", "ReceiptAvailableAt", "RowHash"} {
		f := r.FieldByName(name)
		if f.IsValid() {
			f.SetZero()
		}
	}
	return CanonicalHash(r.Interface())
}
func reportReceiptConflict(a, b any) error {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	for _, name := range []string{"ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasUsed", "EffectiveGasPriceWei"} {
		x, y := av.FieldByName(name), bv.FieldByName(name)
		if x.IsValid() && y.IsValid() && !x.IsNil() && !y.IsNil() && CanonicalHash(x.Interface()) != CanonicalHash(y.Interface()) {
			return errors.New("conflicting_receipt_evidence")
		}
	}
	return nil
}
func reportMergeRanges(in []reportRange) []reportRange {
	sort.Slice(in, func(i, j int) bool { return in[i].from < in[j].from })
	out := []reportRange{}
	for _, r := range in {
		if len(out) == 0 || out[len(out)-1].to != ^uint64(0) && r.from > out[len(out)-1].to+1 {
			out = append(out, r)
		} else if r.to > out[len(out)-1].to {
			out[len(out)-1].to = r.to
		}
	}
	return out
}
func reportCovered(ranges []reportRange, from, to uint64) bool {
	for _, r := range ranges {
		if r.from <= from && r.to >= to {
			return true
		}
	}
	return false
}
