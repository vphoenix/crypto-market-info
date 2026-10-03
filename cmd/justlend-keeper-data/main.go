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
	"strings"
	"syscall"
	"time"

	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: justlend-keeper-data init-schema|preflight|backfill|watch|report|resume-source [flags]")
	}
	command := args[0]
	switch command {
	case "init-schema", "preflight", "backfill", "watch", "report", "resume-source":
	default:
		return errors.New("unknown_command")
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	conf := f.String("config", "config/justlend-keeper-tron.json", "public data source config")
	db := f.String("database", "crypto_market_info_justlend_keeper", "isolated keeper research database")
	addr := f.String("clickhouse", "127.0.0.1:9000", "ClickHouse native address")
	stateDir := f.String("state", "var/justlend-keeper/state", "persistent local state; retain across restarts")
	evidence := f.String("evidence", "var/justlend-keeper/evidence", "content-addressed raw evidence")
	duration := f.Duration("duration", 24*time.Hour, "collection time limit including warmup")
	days := f.Int("days", 30, "explicit backfill days, 1..30")
	maxPages := f.Uint("max-pages", 0, "maximum additional committed backfill pages; 0 finishes the window")
	fromText := f.String("from", "", "inclusive UTC RFC3339 start")
	toText := f.String("to", "", "exclusive UTC RFC3339 end")
	out := f.String("out", "var/justlend-keeper/reports/latest", "offline report output")
	source := f.String("source", "", "source to unblock explicitly: publicnode, trongrid or binance")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected_arguments")
	}
	if !strings.HasPrefix(*db, "crypto_market_info_justlend_keeper") || *duration <= 0 || *days < 1 || *days > 30 || *maxPages > 100000 {
		return errors.New("invalid_database_or_limits")
	}
	cfg, e := keeper.LoadConfig(*conf)
	if e != nil {
		return e
	}
	chcfg := clickhouse.Config{Addresses: []string{*addr}, Database: *db, Username: os.Getenv("JUSTLEND_KEEPER_CLICKHOUSE_USER"), Password: os.Getenv("JUSTLEND_KEEPER_CLICKHOUSE_PASSWORD")}
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
	if command == "report" {
		store, e := clickhouse.OpenDEXReader(ctx, chcfg)
		if e != nil {
			return e
		}
		defer store.Close()
		return keeper.Report(ctx, store, archive, cfg, start, end, time.Now().UTC(), *out)
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
	if command == "resume-source" {
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
		store, e := clickhouse.Open(ctx, chcfg)
		if e != nil {
			return e
		}
		defer store.Close()
		if e = store.InitKeeperSchema(ctx); e != nil {
			return e
		}
		fmt.Printf("keeper schema ready: %s (5 tables)\n", *db)
		return nil
	}
	store, e := clickhouse.OpenKeeperWriter(ctx, chcfg)
	if e != nil {
		return e
	}
	defer store.Close()
	s, e := keeper.LoadState(keeper.StateFile(*stateDir), *addr+"/"+*db, cfg)
	if e != nil {
		return e
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
	c := keeper.NewCollector(cfg, &s, keeper.StateFile(*stateDir), archive, store)
	c.Log = func(s string) { log.Print(s) }
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
