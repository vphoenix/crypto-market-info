package dexarb

import (
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"sort"
	"time"
)

type ReportOptions struct {
	Head        bool
	CapitalUSDT *big.Int
	Route       string
}
type Candidate struct {
	Block    dex.Block
	Quote    dex.Quote
	Gross    *big.Int
	Budget   *big.Int
	BudgetOK bool
	Complete bool
	Reason   string
}
type Window struct {
	Alternatives                  []Candidate // Other complete sizes at the same peak block; never summed.
	ID                            dex.Hash
	Route                         string
	Manifest                      dex.Hash
	StartBlock, EndBlock          uint64
	Start, End                    time.Time
	FirstAvailable, LastAvailable time.Time
	Observations                  uint64
	ConfirmedNew                  bool
	ContinuityUnknown             bool
	Best                          Candidate
}
type RouteSummary struct {
	Assessment                                                        string
	Route                                                             string
	CompleteBlocks, ConfirmedWindows, UnknownFragments, PositiveDates uint64
	MaxDurationSeconds                                                int64
	BestGross                                                         *big.Int
	BestAmount                                                        *big.Int
}
type Report struct {
	Blocks, ExpectedHeights, QuoteCompleteBlocks uint64
	MissingHeights                               uint64
	Windows                                      []Window
	Routes                                       []RouteSummary
	Candidates                                   []Candidate // Best amount per route/block, never summed.
	Warnings                                     []string
}

func gross(q dex.Quote) *big.Int { return new(big.Int).Sub(q.AmountOut, q.Requested) }

// Analyze never converts block quotes to hypothetical daily cash flows or APR.
// All windows are observations, and all routes share the underlying Sky PSM.
func Analyze(blocks []dex.Block, quotes map[dex.Hash][]dex.Quote, routes []Route, sizes []*big.Int, usdc, usdt dex.Address, opt ReportOptions) Report {
	var report Report
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Number == blocks[j].Number {
			return blocks[i].Manifest.String() < blocks[j].Manifest.String()
		}
		return blocks[i].Number < blocks[j].Number
	})
	type state struct {
		last           uint64
		hash, manifest dex.Hash
		kind           string
		window         int
	}
	states := map[string]state{}
	summaries := map[string]*RouteSummary{}
	days := map[string]map[string]bool{}
	heights := map[uint64]bool{}
	var first, last uint64
	for _, b := range blocks {
		if !b.Committed || !b.Canonical || (!opt.Head && b.Finality != "finalized") {
			continue
		}
		if !heights[b.Number] {
			report.Blocks++
			heights[b.Number] = true
		}
		if first == 0 || b.Number < first {
			first = b.Number
		}
		if b.Number > last {
			last = b.Number
		}
		qs := quotes[b.Batch]
		membersOK := len(qs) == int(b.ActualQuotes) && dex.QuoteDigest(qs) == b.QuoteMembers
		if membersOK && b.QuoteCoverage == "complete" && b.Capture == "live" {
			report.QuoteCompleteBlocks++
		}
		var budget *big.Int
		if membersOK {
			for _, q := range qs {
				if q.Role == "capital_entry" && q.Status == "ok" && q.Mode == "exact_in" && q.TokenIn == usdt && q.TokenOut == usdc && q.Requested.Cmp(opt.CapitalUSDT) == 0 && q.AmountIn != nil && q.AmountIn.Cmp(q.Requested) == 0 && q.AmountOut != nil {
					budget = dex.Copy(q.AmountOut)
				}
			}
		}
		for _, route := range routes {
			if opt.Route != "" && route.ID != opt.Route {
				continue
			}
			summary := summaries[route.ID]
			if summary == nil {
				summary = &RouteSummary{Route: route.ID}
				summaries[route.ID] = summary
				days[route.ID] = map[string]bool{}
			}
			previous := states[route.ID]
			continuous := previous.last+1 == b.Number && previous.hash == b.Parent && previous.manifest == b.Manifest
			if !continuous {
				previous.kind = "unknown"
			}
			full := membersOK && budget != nil && b.Capture == "live"
			var best *Candidate
			var alternatives []Candidate
			var unbudgeted *Candidate
			seen := map[string]bool{}
			for _, q := range qs {
				if q.Role != "strategy" || q.Route != route.ID {
					continue
				}
				key := q.Requested.String()
				if seen[key] {
					full = false
					continue
				}
				seen[key] = true
				supported := false
				for _, size := range sizes {
					if q.Requested.Cmp(size) == 0 {
						supported = true
						break
					}
				}
				valid := membersOK && supported && q.Status == "ok" && q.Mode == "exact_in" && q.TokenIn == usdc && q.TokenOut == usdc && q.AmountIn != nil && q.AmountOut != nil && q.AmountIn.Cmp(q.Requested) == 0 && q.Hash == b.Hash && q.Batch == b.Batch && q.Manifest == b.Manifest
				if !valid {
					full = false
					continue
				}
				cand := Candidate{Block: b, Quote: q, Gross: gross(q), Budget: dex.Copy(budget), BudgetOK: budget != nil && q.Requested.Cmp(budget) <= 0, Reason: "capital_or_cost_reserve_unknown"}
				alternatives = append(alternatives, cand)
				if unbudgeted == nil || cand.Gross.Cmp(unbudgeted.Gross) > 0 {
					copy := cand
					unbudgeted = &copy
				}
				if !cand.BudgetOK {
					if budget != nil {
						cand.Reason = "over_capital_budget"
					}
					continue
				}
				cand.Reason = "costs_unspecified"
				if summary.BestGross == nil || cand.Gross.Cmp(summary.BestGross) > 0 {
					summary.BestGross = dex.Copy(cand.Gross)
					summary.BestAmount = dex.Copy(q.Requested)
				}
				if best == nil || cand.Gross.Cmp(best.Gross) > 0 {
					copy := cand
					best = &copy
				}
			}
			if len(seen) != len(sizes) {
				full = false
			}
			if full {
				summary.CompleteBlocks++
			}
			if best != nil && best.Gross.Sign() > 0 {
				best.Complete = full
				report.Candidates = append(report.Candidates, *best)
				days[route.ID][b.Time.UTC().Format("2006-01-02")] = true
				if full && continuous && previous.kind == "positive" {
					w := &report.Windows[previous.window]
					w.EndBlock = b.Number
					w.End = b.Time
					w.LastAvailable = b.AvailableAt
					w.Observations++
					if best.Gross.Cmp(w.Best.Gross) > 0 {
						w.Best = *best
						w.Alternatives = append([]Candidate{}, alternatives...)
					}
					summary.MaxDurationSeconds = max(summary.MaxDurationSeconds, int64(w.End.Sub(w.Start)/time.Second))
				} else {
					confirmed := full && continuous && previous.kind == "negative"
					w := Window{Route: route.ID, Manifest: b.Manifest, StartBlock: b.Number, EndBlock: b.Number, Start: b.Time, End: b.Time, FirstAvailable: b.AvailableAt, LastAvailable: b.AvailableAt, Observations: 1, ConfirmedNew: confirmed, ContinuityUnknown: !confirmed, Best: *best}
					w.Alternatives = append([]Candidate{}, alternatives...)
					w.ID = dex.ObjectHash(struct {
						Route           string
						Manifest, Start dex.Hash
					}{route.ID, b.Manifest, b.Hash})
					report.Windows = append(report.Windows, w)
					previous.window = len(report.Windows) - 1
					if confirmed {
						summary.ConfirmedWindows++
					} else {
						summary.UnknownFragments++
					}
				}
				previous.kind = "positive"
				if !full {
					previous.kind = "unknown"
				}
			} else if full && best != nil {
				previous.kind = "negative"
			} else {
				previous.kind = "unknown"
			}
			if best == nil && unbudgeted != nil && unbudgeted.Gross.Sign() > 0 {
				report.Candidates = append(report.Candidates, *unbudgeted)
			}
			previous.last = b.Number
			previous.hash = b.Hash
			previous.manifest = b.Manifest
			states[route.ID] = previous
		}
	}
	if first != 0 {
		report.ExpectedHeights = last - first + 1
		report.MissingHeights = report.ExpectedHeights - uint64(len(heights))
	}
	for _, s := range summaries {
		s.Assessment = "unknown"
		if s.BestGross != nil {
			s.Assessment = "quoted_nonpositive"
			if s.BestGross.Sign() > 0 {
				s.Assessment = "gross_candidate"
			}
		}
		s.PositiveDates = uint64(len(days[s.Route]))
		report.Routes = append(report.Routes, *s)
	}
	sort.Slice(report.Routes, func(i, j int) bool { return report.Routes[i].Route < report.Routes[j].Route })
	report.Warnings = []string{"all routes share Sky PSM; simultaneous candidates are mutually exclusive", "block-end quotations do not establish inclusion, realized profit or APR", "USDC profits are not USDT returns; entry/exit and idle capital need separate accounting", "missing heights outside the observed endpoints cannot be measured from this report"}
	return report
}
func Units(n *big.Int, decimals int) string {
	if n == nil {
		return "unknown"
	}
	sign := ""
	v := new(big.Int).Set(n)
	if v.Sign() < 0 {
		sign = "-"
		v.Neg(v)
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	whole, frac := new(big.Int), new(big.Int)
	whole.QuoRem(v, scale, frac)
	return fmt.Sprintf("%s%s.%0*s", sign, whole.String(), decimals, frac.String())
}
