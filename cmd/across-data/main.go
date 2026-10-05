package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

var buildVersion = "development"
var buildUTC = "unknown"

func run(ctx context.Context, args []string) error {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		fmt.Printf("across-data %s built_utc=%s\n", buildVersion, buildUTC)
		return nil
	}
	if len(args) == 0 {
		return errors.New("usage: across-data init-schema|preflight|backfill|history|watch|report|prune-responses|migrate-transfers [flags]")
	}
	cmd := args[0]
	if cmd != "init-schema" && cmd != "preflight" && cmd != "backfill" && cmd != "history" && cmd != "watch" && cmd != "report" && cmd != "prune-responses" && cmd != "migrate-transfers" {
		return errors.New("unknown command")
	}
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	manifest := f.String("manifest", "config/across-research.json", "public chain/implementation whitelist")
	database := f.String("database", "crypto_market_info_across", "isolated Across research database")
	address := f.String("clickhouse", "127.0.0.1:9000", "ClickHouse native address")
	evidence := f.String("evidence", "var/across/evidence", "content-addressed evidence directory")
	once := f.Bool("once", false, "one watch cycle")
	days := f.Int("days", 30, "backfill days based on finalized header timestamps")
	chainID := f.Uint64("chain", 0, "8453 or 42161; 0 selects both")
	fromBlock := f.Uint64("from-block", 0, "inclusive explicit backfill start; requires --chain")
	toBlock := f.Uint64("to-block", 0, "inclusive explicit finalized backfill end; requires --chain")
	maxRanges := f.Int("max-ranges", 0, "bounded backfill ranges per chain; 0 is unlimited")
	fromText := f.String("from", "", "report start UTC RFC3339 inclusive")
	toText := f.String("to", "", "report end UTC RFC3339 exclusive")
	out := f.String("out", "var/across/reports/latest", "report output directory")
	noPrices := f.Bool("no-prices", false, "skip optional Binance.com public valuation prices")
	var fixedRanges historyRanges
	f.Var(&fixedRanges, "range", "fixed history range chain:from:to; repeat for both chains")
	quotaDir := f.String("rpc-quota-dir", "var/across/rpc-quota", "shared host quota directory for live/history")
	quotaInterval := f.Duration("rpc-quota-interval", 400*time.Millisecond, "cooperating processes minimum member pacing")
	quotaBurst := f.Int("rpc-burst", 3, "maximum source burst and batch members")
	rpcMinInterval := f.Duration("rpc-min-interval", 0, "minimum per-chain cooldown between single-member RPC requests; 0 preserves batching")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if *quotaInterval <= 0 || *quotaBurst < 1 || *quotaBurst > 10 {
		return errors.New("invalid_rpc_quota")
	}
	if cmd == "history" && len(fixedRanges) == 0 {
		return errors.New("history_requires_fixed_ranges")
	}
	if cmd != "history" && len(fixedRanges) > 0 {
		return errors.New("ranges_only_for_history")
	}
	if *rpcMinInterval < 0 {
		return errors.New("rpc_min_interval_must_be_nonnegative")
	}
	if *database == "crypto_market_info" || *database == "crypto_market_info_fullcheck" || *database == "crypto_market_info_reserve" {
		return errors.New("requires_separate_across_database")
	}
	cfg := clickhouse.Config{Addresses: []string{*address}, Database: *database, Username: os.Getenv("ACROSS_CLICKHOUSE_USER"), Password: os.Getenv("ACROSS_CLICKHOUSE_PASSWORD")}
	if cmd != "report" && cmd != "prune-responses" {
		// A per-database lock remains stable across evidence-directory choices.
		lockDir := filepath.Join(os.TempDir(), "crypto-market-info-across-locks")
		if e := os.MkdirAll(lockDir, 0700); e != nil {
			return e
		}
		key := across.Hex(across.ID(struct{ Addr, DB string }{*address, *database}))[2:]
		lock, e := os.OpenFile(filepath.Join(lockDir, key+".lock"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return errors.New("across_writer_already_running")
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	}
	var store *clickhouse.Client
	var e error
	if cmd == "report" || cmd == "prune-responses" {
		store, e = clickhouse.OpenDEXReader(ctx, cfg)
	} else {
		store, e = clickhouse.Open(ctx, cfg)
	}
	if e != nil {
		return e
	}
	defer store.Close()
	if cmd != "report" && cmd != "prune-responses" {
		if e = store.InitAcrossSchema(ctx); e != nil {
			return e
		}
	}
	if cmd == "init-schema" {
		fmt.Printf("Across database %s ready (8 tables)\n", *database)
		return nil
	}
	m, e := across.LoadManifest(*manifest)
	if e != nil {
		return e
	}
	archive := ethereum.Archive{Dir: *evidence}
	if cmd == "prune-responses" {
		ctx = across.WithLoadProgress(ctx, func(p across.LoadProgress) {
			if p.Phase == "fact_chunk_finished" || strings.HasPrefix(p.Phase, "prune_") {
				log.Printf("%s checked=%d total=%d", p.Phase, p.Completed, p.Total)
			}
		})
		stats, err := across.PruneStoredResponses(ctx, store, m, archive)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(stats)
	}
	if cmd == "migrate-transfers" {
		ctx = across.WithLoadProgress(ctx, func(p across.LoadProgress) {
			if p.Phase == "transfer_rows_written" {
				log.Printf("%s count=%d", p.Phase, p.Completed)
			}
		})
		stats, err := across.MigrateReceiptTransfers(ctx, store, m, archive)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(stats)
	}
	if cmd == "report" {
		end := time.Now().UTC()
		if *toText != "" {
			end, e = time.Parse(time.RFC3339, *toText)
			if e != nil {
				return e
			}
		}
		start := end.Add(-30 * 24 * time.Hour)
		if *fromText != "" {
			start, e = time.Parse(time.RFC3339, *fromText)
			if e != nil {
				return e
			}
		}
		if !start.Before(end) {
			return errors.New("invalid_report_window")
		}
		return across.BuildReport(ctx, store, m, archive, start, end, *out)
	}
	if _, e = archive.Put(m.Raw); e != nil {
		return e
	}
	c := &across.Collector{Manifest: m, Readers: map[uint64]*across.Reader{}, Store: store, Archive: archive}
	for _, ch := range m.Chains {
		rpc, e := ethereum.NewClient(ch.Endpoint(), *evidence)
		if e != nil {
			return e
		}
		quota, e := across.NewSourceQuota(*quotaDir, rpc.SourceID, *quotaInterval, *quotaBurst)
		if e != nil {
			return e
		}
		rpc.Archive.HashOnly = true
		rpc.BeforeRequest = quota.Before
		rpc.AfterRequest = quota.After
		rpc.AuditFailures = true
		c.Readers[ch.ChainID] = across.NewReader(rpc, ch)
		c.Readers[ch.ChainID].BatchLimit = *quotaBurst
		c.Readers[ch.ChainID].MaxLogs = m.MaxLogs
		c.Readers[ch.ChainID].RPCMinInterval = *rpcMinInterval
	}
	if !*noPrices {
		c.Prices = &across.Prices{URL: m.BinanceURL, Archive: ethereum.Archive{HashOnly: true}}
	}
	if cmd == "history" {
		log.Printf("build=%s built_utc=%s", buildVersion, buildUTC)
		return c.History(ctx, fixedRanges, func(s string) { log.Print(s) })
	}
	if cmd == "watch" {
		log.Printf("build=%s built_utc=%s", buildVersion, buildUTC)
		return c.Watch(ctx, *once, func(s string) { log.Print(s) })
	}
	if *days < 1 || *days > 365 || *maxRanges < 0 {
		return errors.New("invalid_backfill_limits")
	}
	if (*fromBlock != 0 || *toBlock != 0) && *chainID == 0 {
		return errors.New("explicit_heights_require_chain")
	}
	successes := 0
	var failures []error
	for _, ch := range m.Chains {
		if *chainID != 0 && *chainID != ch.ChainID {
			continue
		}
		r := c.Readers[ch.ChainID]
		end, e := r.Header(ctx, "finalized")
		if e == nil {
			if cmd == "preflight" {
				e = r.Preflight(ctx, end)
			} else {
				e = r.PreflightIdentity(ctx, end)
			}
		}
		if e != nil {
			failures = append(failures, fmt.Errorf("chain=%d preflight: %w", ch.ChainID, e))
			continue
		}
		log.Printf("chain=%d preflight_ok block=%d hash=%s", ch.ChainID, end.Number, across.Hex(end.Hash))
		if cmd == "preflight" {
			successes++
			continue
		}
		first, last := *fromBlock, *toBlock
		if last == 0 {
			last = end.Number
		}
		if first == 0 {
			first, e = c.BlockAtTime(ctx, ch.ChainID, end.Time.Add(-time.Duration(*days)*24*time.Hour), end)
			if e != nil {
				failures = append(failures, e)
				continue
			}
		}
		e = c.Backfill(ctx, ch.ChainID, first, last, *maxRanges, func(s string) { log.Print(s) })
		if e != nil {
			failures = append(failures, fmt.Errorf("chain=%d backfill: %w", ch.ChainID, e))
			continue
		}
		successes++
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if successes == 0 {
		return errors.New("no_chain_selected")
	}
	return nil
}

type historyRanges []across.HistoryRange

func (r *historyRanges) String() string { return fmt.Sprint([]across.HistoryRange(*r)) }
func (r *historyRanges) Set(s string) error {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return errors.New("range_requires_chain_from_to")
	}
	var nums [3]uint64
	for i, p := range parts {
		n, e := strconv.ParseUint(p, 10, 64)
		if e != nil {
			return errors.New("invalid_history_range")
		}
		nums[i] = n
	}
	if (nums[0] != 8453 && nums[0] != 42161) || nums[1] > nums[2] || nums[2] == ^uint64(0) {
		return errors.New("invalid_history_range")
	}
	for _, old := range *r {
		if old.ChainID == nums[0] {
			return errors.New("duplicate_history_chain")
		}
	}
	*r = append(*r, across.HistoryRange{ChainID: nums[0], From: nums[1], To: nums[2]})
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil && !errors.Is(e, context.Canceled) {
		log.Print(e)
		if strings.Contains(e.Error(), "network_issue_stop") {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
