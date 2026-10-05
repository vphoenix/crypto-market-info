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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reserve-data init-schema|migrate-receipts|watch|backfill|simulate|report [flags]")
	}
	cmd := args[0]
	if cmd != "init-schema" && cmd != "migrate-receipts" && cmd != "watch" && cmd != "backfill" && cmd != "simulate" && cmd != "report" {
		return errors.New("unknown command")
	}
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	manifest := f.String("manifest", "config/reserve-ethereum.json", "fixed public address/path whitelist")
	db := f.String("database", "crypto_market_info_reserve", "isolated research database")
	addr := f.String("clickhouse", "127.0.0.1:9000", "native ClickHouse address")
	evidence := f.String("evidence", "var/reserve/evidence", "writer lock directory; migrate-receipts also reads legacy gzip here")
	rules := f.String("rules", "var/reserve/rules", "small versioned public configuration/artifacts only; no RPC responses")
	once := f.Bool("once", false, "one live capture then exit")
	from := f.Uint64("from", 0, "first backfill block (inclusive)")
	to := f.Uint64("to", 0, "last finalized backfill block (inclusive)")
	days := f.Int("days", 30, "backfill days using block timestamps")
	chunk := f.Uint64("chunk", 512, "log range size, <=512")
	maxRanges := f.Int("max-ranges", 0, "optional bounded backfill run")
	finalized := f.Bool("finalized-only", true, "report finalized captures only")
	gas := f.Uint64("gas-units", 0, "explicit hypothetical tx gas; 0 means unknown")
	tip := f.String("tip-wei", "0", "explicit hypothetical priority fee in wei")
	policyDir := f.String("rpc-state", "var/rpc-state", "persistent source-host quota directory, shared by cooperating processes")
	interval := f.Duration("rpc-interval", 500*time.Millisecond, "minimum interval between HTTP requests")
	perMember := f.Duration("rpc-per-member", 100*time.Millisecond, "source quota time per RPC member")
	batchSize := f.Int("rpc-batch", 10, "maximum members per HTTP request, <=20")
	rpcTimeout := f.Duration("rpc-timeout", 8*time.Second, "HTTP timeout after source admission")
	cooldown := f.Duration("rpc-cooldown", 60*time.Second, "minimum 429 cooldown; persisted and exponentially extended")
	snapshotBudget := f.Duration("snapshot-budget", 35*time.Second, "snapshot wall time including quota waits")
	reconcileManifest := f.String("reconcile-manifest", "", "previous manifest hash whose tail must be finalized during version migration")
	simulateEvery := f.Duration("simulate-every", 0, "watch: simulate two smallest complete routes at this interval; 0 disables")
	outDir := f.String("out", "", "report: optional directory for coverage/activity/candidates CSV")
	simulationLimit := f.Int("simulation-limit", 2, "simulate: maximum complete routes, 1..6")
	reportManifest := f.String("manifest-hash", "", "report an explicitly selected historical manifest identity")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional argument")
	}
	if *simulateEvery < 0 {
		return errors.New("invalid_watch_cadence")
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
		fmt.Println("Reserve schema ready: " + *db + " (7 tables)")
		return nil
	}
	if cmd == "migrate-receipts" {
		count, e := reserve.MigrateReceiptData(ctx, store, ethereum.Archive{Dir: *evidence})
		if e != nil {
			return e
		}
		fmt.Printf("Typed receipt data ready: database=%s receipts=%d\n", *db, count)
		return nil
	}
	m, e := reserve.LoadManifest(*manifest)
	if e != nil {
		return e
	}
	if cmd == "report" {
		if *reportManifest != "" {
			m.Hash, e = dex.ParseHash(*reportManifest)
			if e != nil {
				return e
			}
		}
		n, ok := new(big.Int).SetString(*tip, 10)
		if !ok || !reserve.Uint256(n) {
			return errors.New("invalid_tip_wei")
		}
		report, e := reserve.BuildReport(ctx, store, m, *finalized, *gas, n)
		if e != nil {
			return e
		}
		if *outDir != "" {
			if e = reserve.ExportCSV(ctx, store, m, *finalized, *outDir); e != nil {
				return e
			}
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
	rpc.Archive.HashOnly = true
	var diagnosticMu sync.Mutex
	var lastDiagnostic time.Time
	rpc.Diagnostic = func(d ethereum.RPCDiagnostic) {
		diagnosticMu.Lock()
		defer diagnosticMu.Unlock()
		if time.Since(lastDiagnostic) < 10*time.Second {
			return
		}
		lastDiagnostic = time.Now()
		b, e := json.Marshal(d)
		if e == nil {
			log.Print("rpc_diagnostic " + string(b))
		}
	}
	if e = rpc.ConfigurePolicy(ethereum.Policy{Directory: *policyDir, MinInterval: *interval, PerMember: *perMember, BatchSize: *batchSize, Timeout: *rpcTimeout, Cooldown: *cooldown}); e != nil {
		return e
	}
	if *snapshotBudget <= 0 {
		return errors.New("invalid_snapshot_budget")
	}
	if e = rpc.WaitReady(ctx); e != nil {
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
	if e = reserve.SaveRules(*rules, m); e != nil {
		return e
	}
	collector := &reserve.Collector{RPC: rpc, Manifest: m, Store: store, StartBlock: *from, SnapshotBudget: *snapshotBudget, SimulateEvery: *simulateEvery}
	if *reconcileManifest != "" {
		h, e := dex.ParseHash(*reconcileManifest)
		if e != nil {
			return e
		}
		if h == m.Hash {
			return errors.New("reconcile_manifest_must_differ")
		}
		collector.RelatedManifests = []dex.Hash{h}
	}
	progress := func(s string) { log.Print(s) }
	if cmd == "simulate" {
		if *simulationLimit < 1 || *simulationLimit > 6 {
			return errors.New("invalid_simulation_limit")
		}
		head, e := collector.Header(ctx, "latest")
		if e != nil {
			return e
		}
		reader := reserve.NewReader(rpc, m)
		reader.Limit = 1000
		if e = reader.Preflight(ctx, head); e != nil {
			return e
		}
		b, e := collector.Snapshot(ctx, head, "research")
		if e != nil {
			return e
		}
		if e = store.WriteReserveBatch(ctx, b); e != nil {
			return e
		}
		return collector.SimulateBatch(ctx, head, b, *simulationLimit, progress)
	}
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
		if strings.HasPrefix(e.Error(), "network_issue_stop:") {
			os.Exit(4)
		}
		if errors.Is(e, reserve.ErrArchiveAuthorization) {
			os.Exit(3)
		}
		os.Exit(1)
	}
}
