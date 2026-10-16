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
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/backend"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

var buildVersion = "development"
var buildUTC = "unknown"

func run(ctx context.Context, args []string) (result error) {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		fmt.Printf("across-data %s built_utc=%s\n", buildVersion, buildUTC)
		return nil
	}
	if len(args) == 0 {
		return errors.New("usage: across-data init-schema|preflight|backfill|history|watch|report|prune-responses|migrate-transfers|runtime-convert|runtime-verify|runtime-rollback|runtime-handoff [flags]")
	}
	cmd := args[0]
	if cmd != "init-schema" && cmd != "preflight" && cmd != "backfill" && cmd != "history" && cmd != "watch" && cmd != "report" && cmd != "prune-responses" && cmd != "migrate-transfers" && cmd != "runtime-convert" && cmd != "runtime-verify" && cmd != "runtime-rollback" && cmd != "runtime-handoff" {
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
	runtimeDir := f.String("runtime-state-dir", "var/across/runtime", "verified local runtime state; never implicitly rebuilt")
	handoffPlan := f.String("runtime-handoff-plan", "", "frozen finite operation manifest for stopped-service maintenance")
	runtimeInput := f.String("runtime-input-inventory", "", "frozen migrated/pending/unreadable input inventory; required for conversion")
	epochFile := f.String("runtime-epoch-file", os.Getenv("ACROSS_RUNTIME_EPOCH_FILE"), "database restore epoch outside database backups")
	runtimeExport := f.String("runtime-export", "", "new destination for a current-state backup during verify/rollback")
	legacyRecovery := f.Bool("legacy-history-recovery", false, "explicit controlled rollback: rebuild watch state from current database history")
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
	if *runtimeDir == "" || *legacyRecovery && cmd != "watch" || *runtimeExport != "" && cmd != "runtime-verify" && cmd != "runtime-rollback" {
		return errors.New("invalid_runtime_state_options")
	}
	if cmd == "runtime-handoff" && *handoffPlan == "" || cmd != "runtime-handoff" && *handoffPlan != "" {
		return errors.New("runtime_handoff_requires_explicit_plan")
	}
	if cmd == "runtime-convert" && *runtimeInput == "" {
		return errors.New("runtime_conversion_requires_input_inventory")
	}
	if cmd == "runtime-rollback" && *runtimeExport == "" {
		return errors.New("runtime_rollback_requires_current_state_export")
	}
	if *database == "crypto_market_info" || *database == "crypto_market_info_fullcheck" || *database == "crypto_market_info_reserve" {
		return errors.New("requires_separate_across_database")
	}
	cfg := clickhouse.Config{Addresses: []string{*address}, Database: *database, Username: os.Getenv("ACROSS_CLICKHOUSE_USER"), Password: os.Getenv("ACROSS_CLICKHOUSE_PASSWORD")}
	storage, e := backend.Environment("ACROSS_", cfg, cmd == "report" || cmd == "prune-responses")
	if e != nil {
		return e
	}
	useRuntime := storage.Name == "mysql" && cmd == "watch" && !*legacyRecovery || strings.HasPrefix(cmd, "runtime-")
	if useRuntime && (storage.Name != "mysql" || *epochFile == "") {
		return errors.New("runtime_state_requires_mysql_and_explicit_epoch_file")
	}
	if storage.Name == "mysql" && (cmd == "history" || cmd == "backfill" || cmd == "migrate-transfers") {
		if _, err := os.Stat(*runtimeDir); err == nil {
			return errors.New("active_runtime_requires_frozen_maintenance_handoff_before_external_writes")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
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
	var store contracts.AcrossStore
	store, e = backend.OpenAcross(ctx, storage)
	if e != nil {
		return e
	}
	defer store.Close()
	if cmd == "report" {
		if e = backend.RequireCompleteHistory(ctx, store); e != nil {
			return e
		}
	}
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
	c := &across.Collector{Manifest: m, Readers: map[uint64]*across.Reader{}, ReceiptReaders: map[uint64]*across.Reader{}, Store: store, Archive: archive}
	if useRuntime {
		provider, ok := store.(interface {
			RuntimeBinding(context.Context, string, string, uint32) (runtimecheckpoint.Binding, error)
		})
		if !ok {
			return errors.New("backend_lacks_runtime_binding")
		}
		check := func(ctx context.Context) (runtimecheckpoint.Binding, error) {
			return provider.RuntimeBinding(ctx, across.Hex(m.Hash), *epochFile, 1)
		}
		binding, err := check(ctx)
		if err != nil {
			return err
		}
		targetLock, err := runtimecheckpoint.LockTarget(binding)
		if err != nil {
			return err
		}
		defer targetLock.Close()
		progress := func(line string) { log.Print(line) }
		if cmd == "runtime-convert" {
			report, err := c.ConvertRuntime(ctx, *runtimeDir, binding, *runtimeInput, check, progress)
			if printErr := json.NewEncoder(os.Stdout).Encode(report); err == nil {
				err = printErr
			}
			return err
		}
		if cmd == "runtime-handoff" {
			ctx = runtimecheckpoint.MaintenanceContext(ctx)
		}
		if err = c.OpenRuntime(ctx, *runtimeDir, binding, progress); err != nil {
			return err
		}
		defer func() { result = errors.Join(result, c.CloseRuntime()) }()
		if cmd == "runtime-handoff" {
			report, err := c.HandoffRuntime(ctx, *handoffPlan)
			if err != nil {
				return err
			}
			actual, err := check(ctx)
			if err != nil {
				return err
			}
			if actual != binding {
				return runtimecheckpoint.ErrConflict
			}
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		if cmd == "runtime-verify" {
			report, err := c.InspectRuntime(ctx, *runtimeExport)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		if cmd == "runtime-rollback" {
			report, err := c.RollbackRuntime(ctx, *runtimeExport)
			if err != nil {
				return err
			}
			actual, err := check(ctx)
			if err != nil {
				return err
			}
			if actual != binding {
				return runtimecheckpoint.ErrConflict
			}
			return json.NewEncoder(os.Stdout).Encode(report)
		}
	}
	makeReader := func(ch across.ChainConfig, endpoint string) (*across.Reader, error) {
		rpc, e := ethereum.NewClient(endpoint, *evidence)
		if e != nil {
			return nil, e
		}
		quota, e := across.NewSourceQuota(*quotaDir, rpc.SourceID, *quotaInterval, *quotaBurst)
		if e != nil {
			return nil, e
		}
		rpc.Archive.HashOnly = true
		rpc.BeforeRequest = quota.Before
		rpc.AfterRequest = quota.After
		rpc.AuditFailures = true
		r := across.NewReader(rpc, ch)
		r.SourceQuota, r.BatchLimit, r.MaxLogs, r.RPCMinInterval = quota, *quotaBurst, m.MaxLogs, *rpcMinInterval
		return r, nil
	}
	for _, ch := range m.Chains {
		r, e := makeReader(ch, ch.Endpoint())
		if e != nil {
			return e
		}
		c.Readers[ch.ChainID] = r
		if endpoint := ch.ReceiptEndpoint(); endpoint != ch.Endpoint() {
			r, e := makeReader(ch, endpoint)
			if e != nil {
				return e
			}
			c.ReceiptReaders[ch.ChainID] = r
		}
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
	if e := run(ctx, os.Args[1:]); e != nil {
		code := commandExitCode(ctx, e)
		if code == 0 {
			return
		}
		log.Print(e)
		os.Exit(code)
	}
}

// Only cancellation of the signal context is an intentional shutdown. A worker
// or database request can return context.Canceled while that context is still
// live; treating it as success prevents systemd's on-failure restart.
func commandExitCode(ctx context.Context, err error) int {
	if err == nil {
		return 0
	}
	if strings.Contains(err.Error(), "network_issue_stop") {
		return 2
	}
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}

func storageConfig(ch clickhouse.Config, readOnly bool) backend.Config {
	cfg, e := backend.Environment("ACROSS_", ch, readOnly)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	return cfg
}
