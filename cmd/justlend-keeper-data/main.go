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
	"strings"
	"syscall"
	"time"

	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/backend"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) (result error) {
	if len(args) == 0 {
		return errors.New("usage: justlend-keeper-data init-schema|migrate-pages|preflight|backfill|watch|export|report|resume-source|runtime-convert|runtime-verify|runtime-rollback|runtime-handoff [flags]")
	}
	command := args[0]
	switch command {
	case "init-schema", "migrate-pages", "preflight", "backfill", "watch", "export", "report", "resume-source", "runtime-convert", "runtime-verify", "runtime-rollback", "runtime-handoff":
	default:
		return errors.New("unknown_command")
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	conf := f.String("config", "config/justlend-keeper-tron.json", "public data source config")
	db := f.String("database", "crypto_market_info_justlend_keeper", "isolated keeper research database")
	addr := f.String("clickhouse", "127.0.0.1:9000", "ClickHouse native address")
	stateDir := f.String("state", "var/justlend-keeper/state", "persistent local state; retain across restarts")
	evidence := f.String("evidence", "var/justlend-keeper/evidence", "legacy archive, used only by migrate-pages; collection/export never use it")
	duration := f.Duration("duration", 24*time.Hour, "collection time limit including warmup")
	days := f.Int("days", 30, "explicit backfill days, 1..30")
	historyDays := f.Int("history-days", 0, "watch: freeze 1..30 complete UTC days for bounded background liquidation backfill; 0 disables new history")
	maxPages := f.Uint("max-pages", 0, "maximum additional committed backfill pages; 0 finishes the window")
	fromText := f.String("from", "", "inclusive UTC RFC3339 start")
	toText := f.String("to", "", "exclusive UTC RFC3339 end")
	out := f.String("out", "var/justlend-keeper/exports/"+time.Now().UTC().Format("20060102T150405.000000000Z"), "new directory for authenticated data export")
	source := f.String("source", "", "source to unblock explicitly: publicnode, trongrid or binance")
	runtimeDir := f.String("runtime-state-dir", "var/justlend-keeper/runtime", "verified local runtime state")
	handoffPlan := f.String("runtime-handoff-plan", "", "frozen finite operation manifest for stopped-service maintenance")
	runtimeInput := f.String("runtime-input-inventory", "", "frozen migrated/pending/unreadable input inventory; required for conversion")
	epochFile := f.String("runtime-epoch-file", os.Getenv("JUSTLEND_KEEPER_RUNTIME_EPOCH_FILE"), "external database restore epoch")
	runtimeExport := f.String("runtime-export", "", "new current-state export directory for verify/rollback")
	legacyRecovery := f.Bool("legacy-history-recovery", false, "explicit watch using current legacy recovery state")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected_arguments")
	}
	if *historyDays < 0 || *historyDays > 30 || (*historyDays != 0 && command != "watch") {
		return errors.New("invalid_history_days")
	}
	if !strings.HasPrefix(*db, "crypto_market_info_justlend_keeper") || *duration <= 0 || *days < 1 || *days > 30 || *maxPages > 100000 {
		return errors.New("invalid_database_or_limits")
	}
	cfg, e := keeper.LoadConfig(*conf)
	if e != nil {
		return e
	}
	chcfg := clickhouse.Config{Addresses: []string{*addr}, Database: *db, Username: os.Getenv("JUSTLEND_KEEPER_CLICKHOUSE_USER"), Password: os.Getenv("JUSTLEND_KEEPER_CLICKHOUSE_PASSWORD")}
	if *runtimeDir == "" || *legacyRecovery && command != "watch" || *runtimeExport != "" && command != "runtime-verify" && command != "runtime-rollback" {
		return errors.New("invalid_runtime_options")
	}
	if command == "runtime-handoff" && *handoffPlan == "" || command != "runtime-handoff" && *handoffPlan != "" {
		return errors.New("runtime_handoff_requires_explicit_plan")
	}
	if command == "runtime-convert" && *runtimeInput == "" {
		return errors.New("runtime_conversion_requires_input_inventory")
	}
	if command == "runtime-rollback" && *runtimeExport == "" {
		return errors.New("runtime_rollback_requires_current_state_export")
	}
	storage, e := backend.Environment("JUSTLEND_KEEPER_", chcfg, command == "export" || command == "report")
	if e != nil {
		return e
	}
	useRuntime := storage.Name == "mysql" && (command == "watch" && !*legacyRecovery || command == "resume-source" || command == "preflight") || strings.HasPrefix(command, "runtime-")
	if useRuntime && (storage.Name != "mysql" || *epochFile == "") {
		return errors.New("runtime_state_requires_mysql_and_epoch_file")
	}
	if command == "backfill" || command == "migrate-pages" {
		if _, err := os.Stat(*runtimeDir); err == nil {
			return errors.New("runtime_state_requires_explicit_maintenance_handoff")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	now := time.Now().UTC()
	end := now
	if command == "backfill" {
		end = end.Add(-120 * time.Second)
	}
	start := end.Add(-time.Duration(*days) * 24 * time.Hour)
	if *fromText != "" {
		start, e = time.Parse(time.RFC3339, *fromText)
		if e != nil {
			return e
		}
	}
	if *toText != "" {
		end, e = time.Parse(time.RFC3339, *toText)
		if e != nil {
			return e
		}
	}
	start = keeper.UTC(start)
	end = keeper.UTC(end)
	if !start.Before(end) || end.After(time.Now().UTC().Add(time.Second)) {
		return errors.New("invalid_time_window")
	}
	if command == "backfill" && end.After(time.Now().UTC().Add(-120*time.Second)) {
		return errors.New("backfill_end_requires_120_seconds_index_delay")
	}
	if command == "backfill" && end.Sub(start) > 30*24*time.Hour {
		return errors.New("backfill_maximum_30_days")
	}
	archive := keeper.Archive{Dir: *evidence}
	if command == "export" || command == "report" {
		store, e := backend.OpenKeeper(ctx, storage)
		if e != nil {
			return e
		}
		defer store.Close()
		if e = backend.RequireCompleteHistory(ctx, store); e != nil {
			return e
		}
		return keeper.Export(ctx, store, archive, cfg, start, end, *out)
	}
	lockPath := filepath.Join(os.TempDir(), fmt.Sprintf("crypto-market-info-justlend-keeper-%d.lock", os.Getuid()))
	lock, e := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errors.New("keeper_command_already_running")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if command == "resume-source" && !useRuntime {
		if *source != "publicnode" && *source != "trongrid" && *source != "binance" {
			return errors.New("invalid_resume_source")
		}
		s, er := keeper.LoadState(keeper.StateFile(*stateDir), *addr+"/"+*db, cfg)
		if er != nil {
			return er
		}
		r := s.Sources[*source]
		r.Blocked = false
		s.Sources[*source] = r
		if er = keeper.SaveState(keeper.StateFile(*stateDir), s); er != nil {
			return er
		}
		fmt.Printf("source unblocked: %s; budget and cooldown retained\n", *source)
		return nil
	}
	if command == "init-schema" {
		store, e := backend.OpenKeeper(ctx, storage)
		if e != nil {
			return e
		}
		defer store.Close()
		if e = store.InitKeeperSchema(ctx); e != nil {
			return e
		}
		fmt.Printf("keeper schema ready: %s (7 tables)\n", *db)
		return nil
	}
	store, e := backend.OpenKeeper(ctx, storage)
	if e != nil {
		return e
	}
	defer store.Close()
	if command == "migrate-pages" {
		count, err := keeper.MigrateIndexPages(ctx, store, archive, store)
		if err != nil {
			return err
		}
		fmt.Printf("legacy page progress migrated: %d; captures and members unchanged\n", count)
		return nil
	}
	s := keeper.NewState(*addr+"/"+*db, cfg)
	if !useRuntime || command == "runtime-convert" {
		if command == "runtime-convert" {
			if _, err := os.Stat(keeper.StateFile(*stateDir)); err != nil {
				return err
			}
		}
		s, e = keeper.LoadState(keeper.StateFile(*stateDir), *addr+"/"+*db, cfg)
		if e != nil {
			return e
		}
	}
	c := keeper.NewCollector(cfg, &s, keeper.StateFile(*stateDir), archive, store)
	c.Log = func(s string) { log.Print(s) }
	if useRuntime {
		provider, ok := store.(interface {
			RuntimeBinding(context.Context, string, string, uint32) (runtimecheckpoint.Binding, error)
		})
		if !ok {
			return errors.New("backend_lacks_runtime_binding")
		}
		check := func(ctx context.Context) (runtimecheckpoint.Binding, error) {
			return provider.RuntimeBinding(ctx, keeper.Hex(cfg.Hash()), *epochFile, 1)
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
		if command == "runtime-convert" {
			report, err := c.ConvertRuntime(ctx, *runtimeDir, binding, *runtimeInput, check, c.Log)
			if printErr := json.NewEncoder(os.Stdout).Encode(report); err == nil {
				err = printErr
			}
			return err
		}
		if command == "runtime-handoff" {
			ctx = runtimecheckpoint.MaintenanceContext(ctx)
		}
		if err = c.OpenRuntime(ctx, *runtimeDir, binding); err != nil {
			return err
		}
		defer func() { result = errors.Join(result, c.CloseRuntime()) }()
		if command == "runtime-handoff" {
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
		if command == "runtime-verify" {
			report, err := c.InspectRuntime(ctx, *runtimeExport)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		if command == "runtime-rollback" {
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
		if command == "resume-source" {
			if *source != "publicnode" && *source != "trongrid" && *source != "binance" {
				return errors.New("invalid_resume_source")
			}
			limit := s.Sources[*source]
			limit.Blocked = false
			s.Sources[*source] = limit
			if err = c.Save(); err != nil {
				return err
			}
			fmt.Printf("source unblocked: %s; budget and cooldown retained\n", *source)
			return nil
		}
	}
	for _, o := range s.Ops {
		if command == "watch" && o.Batch.Capture.CaptureMode == "backfill" {
			return errors.New("pending_backfill_resume_with_backfill_before_watch")
		}
		if command == "backfill" && o.Batch.Capture.CaptureMode != "backfill" {
			return errors.New("pending_watch_tasks_resume_with_watch_before_backfill")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	if command == "watch" {
		c.EnsureHistory(*historyDays)
	}
	if command == "preflight" {
		if !c.Active("identity") {
			c.AddIdentity()
		}
		if e = c.Save(); e != nil {
			return e
		}
		for c.Active("identity") {
			progress, next, e := c.Step(ctx)
			if e != nil {
				return e
			}
			if !progress {
				d := next.Sub(time.Now())
				if d > time.Second {
					d = time.Second
				}
				if e = keeper.Sleep(ctx, d); e != nil {
					return e
				}
			}
		}
		if s.IdentityStatus != "verified_manifest" {
			return errors.New("identity_preflight_not_verified")
		}
		fmt.Printf("keeper preflight verified; implementation=%s\n", keeper.Hex(s.Implementation))
		return nil
	}
	if command == "backfill" {
		if s.BackfillActive {
			if (*fromText != "" || *toText != "") && (!start.Equal(s.BackfillFrom) || !end.Equal(s.BackfillTo)) {
				return errors.New("pending_backfill_window_mismatch")
			}
			c.AddBackfill(s.BackfillFrom, s.BackfillTo)
		} else {
			c.AddBackfill(start, end)
		}
	}
	if e = c.Save(); e != nil {
		return e
	}
	e = c.Run(ctx, command == "watch", uint32(*maxPages))
	if errors.Is(e, context.DeadlineExceeded) && ctx.Err() != nil {
		log.Print("duration ended; progress and pending batches retained")
		return nil
	}
	return e
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil && !errors.Is(e, context.Canceled) {
		log.Print(e)
		os.Exit(1)
	}
}
