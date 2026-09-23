package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/dexarb"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	fromRaw := flag.String("from", "", "UTC RFC3339 inclusive; default previous 24 hours")
	toRaw := flag.String("to", "", "UTC RFC3339 exclusive; default now")
	head := flag.Bool("include-head", false, "include explicitly unfinalized candidates")
	route := flag.String("route", "", "optional exact route id")
	capitalRaw := flag.String("capital-usdt", "1000000", "total USDT budget; offline entry evidence must match this exact size")
	reportDir := flag.String("report-dir", "", "write JSON, windows.csv and candidate CSV to this directory")
	costs := flag.Bool("quote-costs-rpc", false, "explicitly supplement historical same-hash cost quotes; at most 50 windows; never writes database")
	gas := flag.Uint64("gas-units", 0, "explicit full-route gas scenario (not Quoter gas)")
	tipRaw := flag.String("priority-fee-gwei", "", "explicit gas priority fee")
	orderRaw := flag.String("ordering-usdc", "", "explicit ordering payment")
	otherRaw := flag.String("other-cost-usdc", "", "explicit other costs; zero must be specified")
	sample := flag.String("sample-block", "", "read one public block (latest/finalized/0xHEIGHT), archive research quotes without writing database")
	flag.Parse()
	cfg, e := config.Load()
	if e != nil {
		return e
	}
	capital, e := dexarb.ParseAtoms(*capitalRaw, 6)
	if e != nil || capital.Sign() == 0 {
		return fmt.Errorf("invalid capital-usdt")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	m := ethereum.DefaultManifest()
	if *sample != "" {
		if *reportDir == "" {
			return fmt.Errorf("sample-block requires report-dir")
		}
		return sampleBlock(ctx, cfg, *sample, *reportDir)
	}
	to := dex.Now()
	if *toRaw != "" {
		to, e = time.Parse(time.RFC3339, *toRaw)
		if e != nil {
			return e
		}
	}
	from := to.Add(-24 * time.Hour)
	if *fromRaw != "" {
		from, e = time.Parse(time.RFC3339, *fromRaw)
		if e != nil {
			return e
		}
	}
	if !from.Before(to) {
		return fmt.Errorf("from must precede to")
	}
	if *route != "" {
		found := false
		for _, r := range m.Routes() {
			found = found || r.ID == *route
		}
		if !found {
			return fmt.Errorf("unknown route")
		}
	}
	var scenario *dexarb.CostScenario
	if *costs || *gas != 0 || *tipRaw != "" || *orderRaw != "" || *otherRaw != "" {
		if *gas == 0 || *tipRaw == "" || *orderRaw == "" || *otherRaw == "" {
			return fmt.Errorf("cost scenario requires gas-units, priority-fee-gwei, ordering-usdc and other-cost-usdc (zero explicitly)")
		}
		tip, e := dexarb.ParseAtoms(*tipRaw, 9)
		if e != nil {
			return e
		}
		order, e := dexarb.ParseAtoms(*orderRaw, 6)
		if e != nil {
			return e
		}
		other, e := dexarb.ParseAtoms(*otherRaw, 6)
		if e != nil {
			return e
		}
		scenario = &dexarb.CostScenario{GasUnits: *gas, PriorityWei: tip, OrderingUSDC: order, OtherUSDC: other, CapitalUSDT: capital}
	}
	if *costs && *reportDir == "" {
		return fmt.Errorf("quote-costs-rpc requires report-dir for evidence")
	}
	store, e := chstore.OpenDEXReader(ctx, cfg.ClickHouse)
	if e != nil {
		return e
	}
	defer store.Close()
	blocks, e := store.DEXBlocks(ctx, from, to)
	if e != nil {
		return e
	}
	quotes, e := store.DEXQuotes(ctx, blocks)
	if e != nil {
		return e
	}
	report := dexarb.Analyze(blocks, quotes, m.Routes(), m.Sizes(), m.Addresses["USDC"], m.Addresses["USDT"], dexarb.ReportOptions{Head: *head, CapitalUSDT: capital, Route: *route})
	costResults := map[dex.Hash]dexarb.CostResult{}
	if *costs {
		client, e := ethereum.NewClient(cfg.DEXRPCURL, filepath.Join(*reportDir, "evidence"))
		if e != nil {
			return e
		}
		cache := &dexarb.CostCache{RPC: client}
		// Expensive supplement is bounded by windows, not every repeated block.
		windows := append([]dexarb.Window{}, report.Windows...)
		sort.Slice(windows, func(i, j int) bool { return windows[i].Best.Gross.Cmp(windows[j].Best.Gross) > 0 })
		if len(windows) > 50 {
			windows = windows[:50]
			report.Warnings = append(report.Warnings, "cost supplement limited to top 50 observed windows")
		}
		for _, w := range windows {
			if w.Manifest != ethereum.ManifestHash() {
				continue
			}
			res := dexarb.QuoteCosts(ctx, cache, w, *scenario, m.Addresses["USDC"], m.Addresses["USDT"], m.Addresses["WETH"])
			res.SourceID = client.SourceID
			if _, e = client.Archive.PutObject(res); e != nil {
				return e
			}
			costResults[w.ID] = res
		}
	}
	fmt.Printf("UTC %s .. %s | capital %s USDT | finalized_only=%t\n", from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339), dexarb.Units(capital, 6), !*head)
	fmt.Printf("blocks=%d complete_live_quotes=%d observed_height_span=%d missing_heights=%d\n", report.Blocks, report.QuoteCompleteBlocks, report.ExpectedHeights, report.MissingHeights)
	if scenario != nil {
		fmt.Printf("cost_scenario_hash=%s gas_units=%d tip_gwei=%s ordering_usdc=%s other_usdc=%s\n", dex.ObjectHash(*scenario), scenario.GasUnits, dexarb.Units(scenario.PriorityWei, 9), dexarb.Units(scenario.OrderingUSDC, 6), dexarb.Units(scenario.OtherUSDC, 6))
		if !*costs {
			fmt.Println("Costs remain unknown: offline reference prices cannot establish exact-output gas replacement cost.")
		}
	}
	fmt.Println("route | complete blocks | confirmed windows | unknown fragments | positive UTC dates | best gross USDC | amount USDC | assessment")
	for _, s := range report.Routes {
		fmt.Printf("%s | %d | %d | %d | %d | %s | %s | %s\n", s.Route, s.CompleteBlocks, s.ConfirmedWindows, s.UnknownFragments, s.PositiveDates, dexarb.Units(s.BestGross, 6), dexarb.Units(s.BestAmount, 6), s.Assessment)
	}
	fmt.Printf("positive observed segments=%d; no cumulative profit or APR computed\n", len(report.Windows))
	for _, w := range report.Warnings {
		fmt.Println("NOTE:", w)
	}
	if *reportDir != "" {
		if e = os.MkdirAll(*reportDir, 0700); e != nil {
			return e
		}
		envelope := struct {
			From, To    time.Time
			Manifest    dex.Hash
			CapitalUSDT string
			Head        bool
			Scenario    *dexarb.CostScenario
			Report      dexarb.Report
			Costs       map[dex.Hash]dexarb.CostResult
		}{from.UTC(), to.UTC(), ethereum.ManifestHash(), capital.String(), *head, scenario, report, costResults}
		if e = writeJSON(filepath.Join(*reportDir, "report.json"), envelope); e != nil {
			return e
		}
		if e = writeWindows(filepath.Join(*reportDir, "windows.csv"), report.Windows, costResults); e != nil {
			return e
		}
		if e = writeCandidates(filepath.Join(*reportDir, "candidates.csv"), report.Candidates); e != nil {
			return e
		}
		fmt.Println("Report:", *reportDir)
	}
	return nil
}
func writeJSON(path string, v any) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
}
func sampleBlock(ctx context.Context, cfg config.Config, tag, dir string) error {
	c, e := ethereum.NewClient(cfg.DEXRPCURL, filepath.Join(dir, "evidence"))
	if e != nil {
		return e
	}
	if _, e = c.Archive.Put(ethereum.ManifestJSON); e != nil {
		return e
	}
	if e = c.VerifyChain(ctx); e != nil {
		return e
	}
	h, e := c.Header(ctx, tag)
	if e != nil {
		return e
	}
	start := time.Now()
	b, e := c.CollectMode(ctx, h, "research")
	if e != nil {
		return e
	}
	if tag == "finalized" {
		b.Block.Finality = "finalized"
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	if e = writeJSON(filepath.Join(dir, "sample.json"), b); e != nil {
		return e
	}
	ok := 0
	best := (*big.Int)(nil)
	reasons := map[string]int{}
	for _, q := range b.Quotes {
		if q.Status == "ok" {
			ok++
			if q.Role == "strategy" {
				g := new(big.Int).Sub(q.AmountOut, q.Requested)
				if best == nil || g.Cmp(best) > 0 {
					best = g
				}
			}
		} else {
			reasons[q.Reason]++
		}
	}
	fmt.Printf("research block=%d hash=%s quotes_ok=%d/%d logs=%d log_coverage=%s receipts=%d receipt_coverage=%s elapsed=%s best_gross_usdc=%s\n", b.Block.Number, b.Block.Hash, ok, len(b.Quotes), len(b.Logs), b.Block.LogCoverage, len(b.Receipts), b.Block.ReceiptCoverage, time.Since(start).Round(time.Millisecond), dexarb.Units(best, 6))
	if b.Sky != nil {
		fmt.Printf("sky_complete=%t identity_ok=%t reason=%s\n", b.Sky.Complete, b.Sky.IdentityOK, b.Sky.Reason)
	}
	fmt.Printf("quote_failure_reasons=%v\narchived=%s\n", reasons, dir)
	return nil
}
func writeWindows(path string, ws []dexarb.Window, costs map[dex.Hash]dexarb.CostResult) error {
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"window_id", "route", "start_utc", "end_utc", "observations", "confirmed_new", "continuity_unknown", "best_block_hash", "amount_usdc", "gross_usdc", "cost_status", "net_usdc", "cost_reason", "shared_resource"})
	for _, v := range ws {
		amount := v.Best.Quote.Requested
		gross := v.Best.Gross
		status := "gross_candidate"
		net := "unknown"
		reason := "costs_unspecified"
		if c, ok := costs[v.ID]; ok {
			status = c.Status
			net = dexarb.Units(c.NetUSDC, 6)
			reason = c.Reason
			if c.SelectedAmountUSDC != nil {
				amount = c.SelectedAmountUSDC
				gross = c.SelectedGrossUSDC
			}
		}
		w.Write([]string{v.ID.String(), v.Route, v.Start.Format(time.RFC3339), v.End.Format(time.RFC3339), strconv.FormatUint(v.Observations, 10), strconv.FormatBool(v.ConfirmedNew), strconv.FormatBool(v.ContinuityUnknown), v.Best.Block.Hash.String(), dexarb.Units(amount, 6), dexarb.Units(gross, 6), status, net, reason, "sky_litepsm_usdc"})
	}
	w.Flush()
	return w.Error()
}
func writeCandidates(path string, cs []dexarb.Candidate) error {
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"block", "hash", "finality", "available_at_utc", "route", "amount_usdc", "gross_usdc", "budget_ok_before_cost_reserve", "route_coverage_complete", "reason"})
	for _, c := range cs {
		w.Write([]string{strconv.FormatUint(c.Block.Number, 10), c.Block.Hash.String(), c.Block.Finality, c.Block.AvailableAt.Format(time.RFC3339Nano), c.Quote.Route, dexarb.Units(c.Quote.Requested, 6), dexarb.Units(c.Gross, 6), strconv.FormatBool(c.BudgetOK), strconv.FormatBool(c.Complete), c.Reason})
	}
	w.Flush()
	return w.Error()
}
