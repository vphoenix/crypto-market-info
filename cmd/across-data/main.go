package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: across-data init-schema|preflight|backfill|watch|report [flags]")
	}
	cmd := args[0]
	if cmd != "init-schema" && cmd != "preflight" && cmd != "backfill" && cmd != "watch" && cmd != "report" {
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
	rpcMinInterval := f.Duration("rpc-min-interval", 0, "minimum per-chain cooldown between single-member RPC requests; 0 preserves batching")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if *rpcMinInterval < 0 {
		return errors.New("rpc_min_interval_must_be_nonnegative")
	}
	if *database == "crypto_market_info" || *database == "crypto_market_info_fullcheck" || *database == "crypto_market_info_reserve" {
		return errors.New("requires_separate_across_database")
	}
	cfg := clickhouse.Config{Addresses: []string{*address}, Database: *database, Username: os.Getenv("ACROSS_CLICKHOUSE_USER"), Password: os.Getenv("ACROSS_CLICKHOUSE_PASSWORD")}
	if cmd != "report" {
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
	if cmd == "report" {
		store, e = clickhouse.OpenDEXReader(ctx, cfg)
	} else {
		store, e = clickhouse.Open(ctx, cfg)
	}
	if e != nil {
		return e
	}
	defer store.Close()
	if cmd != "report" {
		if e = store.InitAcrossSchema(ctx); e != nil {
			return e
		}
	}
	if cmd == "init-schema" {
		fmt.Printf("Across database %s ready (7 tables)\n", *database)
		return nil
	}
	m, e := across.LoadManifest(*manifest)
	if e != nil {
		return e
	}
	archive := ethereum.Archive{Dir: *evidence}
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
		c.Readers[ch.ChainID] = across.NewReader(rpc, ch)
		c.Readers[ch.ChainID].MaxLogs = m.MaxLogs
		c.Readers[ch.ChainID].RPCMinInterval = *rpcMinInterval
	}
	if !*noPrices {
		c.Prices = &across.Prices{URL: m.BinanceURL, Archive: archive}
	}
	if cmd == "watch" {
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
func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil && !errors.Is(e, context.Canceled) {
		log.Print(e)
		os.Exit(1)
	}
}
