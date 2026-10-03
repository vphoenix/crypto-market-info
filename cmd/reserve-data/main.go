package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reserve-data init-schema|watch|backfill|report [flags]")
	}
	cmd := args[0]
	if cmd != "init-schema" && cmd != "watch" && cmd != "backfill" && cmd != "report" {
		return errors.New("unknown command")
	}
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	manifest := f.String("manifest", "config/reserve-ethereum.json", "fixed public address/path whitelist")
	db := f.String("database", "crypto_market_info_reserve", "isolated research database")
	addr := f.String("clickhouse", "127.0.0.1:9000", "native ClickHouse address")
	evidence := f.String("evidence", "var/reserve/evidence", "gzip SHA256 RPC evidence directory")
	once := f.Bool("once", false, "one live capture then exit")
	from := f.Uint64("from", 0, "first backfill block (inclusive)")
	to := f.Uint64("to", 0, "last finalized backfill block (inclusive)")
	days := f.Int("days", 30, "backfill days using block timestamps")
	chunk := f.Uint64("chunk", 512, "log range size, <=512")
	maxRanges := f.Int("max-ranges", 0, "optional bounded backfill run")
	finalized := f.Bool("finalized-only", true, "report finalized captures only")
	gas := f.Uint64("gas-units", 0, "explicit hypothetical tx gas; 0 means unknown")
	tip := f.String("tip-wei", "0", "explicit hypothetical priority fee in wei")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional argument")
	}
	cfg := clickhouse.Config{Addresses: []string{*addr}, Database: *db, Username: os.Getenv("RESERVE_CLICKHOUSE_USER"), Password: os.Getenv("RESERVE_CLICKHOUSE_PASSWORD")}
	var store *clickhouse.Client
	var e error
	if cmd == "report" {
		store, e = clickhouse.OpenDEXReader(ctx, cfg)
	} else {
		if e = os.MkdirAll(*evidence, 0700); e != nil {
			return e
		}
		lock, e := os.OpenFile(filepath.Join(*evidence, "writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return errors.New("reserve_writer_already_running")
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		store, e = clickhouse.Open(ctx, cfg)
	}
	if e != nil {
		return e
	}
	defer store.Close()
	if cmd != "report" {
		if e = store.InitReserveSchema(ctx); e != nil {
			return e
		}
	}
	if cmd == "init-schema" {
		fmt.Println("Reserve schema ready: " + *db + " (5 tables)")
		return nil
	}
	m, e := reserve.LoadManifest(*manifest)
	if e != nil {
		return e
	}
	if cmd == "report" {
		n, ok := new(big.Int).SetString(*tip, 10)
		if !ok || !reserve.Uint256(n) {
			return errors.New("invalid_tip_wei")
		}
		report, e := reserve.BuildReport(ctx, store, m, *finalized, *gas, n)
		if e != nil {
			return e
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	endpoint := os.Getenv("RESERVE_RPC_URL")
	if endpoint == "" {
		endpoint = "https://ethereum-rpc.publicnode.com"
	}
	rpc, e := ethereum.NewClient(endpoint, *evidence)
	if e != nil {
		return e
	}
	pin, err := rpc.Archive.Put(m.Raw)
	if err != nil {
		return err
	}
	if pin != dex.Digest(m.Raw) {
		return errors.New("manifest_evidence_hash_mismatch")
	}
	identity, e := rpc.Archive.PutObject(m.Identity())
	if e != nil {
		return e
	}
	if identity != m.Hash {
		return errors.New("manifest_identity_hash_mismatch")
	}
	collector := &reserve.Collector{RPC: rpc, Manifest: m, Store: store}
	progress := func(s string) { log.Print(s) }
	if cmd == "watch" {
		return collector.Watch(ctx, *once, progress)
	}
	end, e := collector.Header(ctx, "finalized")
	if e != nil {
		return e
	}
	if *to == 0 {
		*to = end.Number
	}
	if *days <= 0 || *days > 365 {
		return errors.New("invalid_days")
	}
	if *from == 0 {
		*from, e = collector.BlockAtTime(ctx, end.Time.Add(-time.Duration(*days)*24*time.Hour), end)
		if e != nil {
			return fmt.Errorf("historical_header_range_unavailable: %w", e)
		}
	}
	return collector.Backfill(ctx, *from, *to, *chunk, *maxRanges, progress)
}
func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := run(ctx, os.Args[1:]); e != nil && !errors.Is(e, context.Canceled) {
		log.Print(e)
		os.Exit(1)
	}
}
