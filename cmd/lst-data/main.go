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

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: lst-data init-schema|probe|watch|backfill|report [flags]")
	}
	command := args[0]
	switch command {
	case "init-schema", "probe", "watch", "backfill", "report":
	default:
		return errors.New("unknown_command")
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	manifest := fs.String("manifest", "config/lst-lido-ethereum.json", "fixed reviewed public contracts and code hashes")
	database := fs.String("database", "crypto_market_info_lst", "isolated research database")
	address := fs.String("clickhouse", "127.0.0.1:9000", "native ClickHouse address")
	state := fs.String("state-dir", "var/lst/state", "persistent shared limiter, lock and frozen pending batch")
	evidence := fs.String("evidence", "var/lst/evidence", "persistent public source evidence")
	once := fs.Bool("once", false, "one market capture; no implicit backfill")
	days := fs.Int("days", 30, "primary event cohort days plus at most 30 context days")
	ranges := fs.Int("max-ranges", 0, "bounded historical log requests; 0 runs to fixed end")
	fromBlock := fs.Uint64("from-block", 0, "explicit inclusive historical range start (paired with to-block)")
	toBlock := fs.Uint64("to-block", 0, "explicit inclusive range end, at most 512 blocks")
	duration := fs.Duration("duration", 0, "optional maximum command runtime")
	from := fs.String("from", "", "report start UTC RFC3339")
	to := fs.String("to", "", "report end UTC RFC3339")
	out := fs.String("out", "var/lst-reports", "report directory or probe manifest output file")
	if e := fs.Parse(args[1:]); e != nil {
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected_arguments")
	}
	if *duration < 0 || *ranges < 0 {
		return errors.New("negative_limit")
	}
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	cfg := clickhouse.Config{Addresses: []string{*address}, Database: *database, Username: os.Getenv("LST_CLICKHOUSE_USER"), Password: os.Getenv("LST_CLICKHOUSE_PASSWORD")}
	if command == "report" {
		m, e := lst.LoadManifest(*manifest)
		if e != nil {
			return e
		}
		start := time.Now().UTC().Add(-30 * 24 * time.Hour)
		end := time.Now().UTC()
		if *from != "" {
			start, e = time.Parse(time.RFC3339, *from)
			if e != nil {
				return e
			}
		}
		if *to != "" {
			end, e = time.Parse(time.RFC3339, *to)
			if e != nil {
				return e
			}
		}
		store, e := clickhouse.OpenDEXReader(ctx, cfg)
		if e != nil {
			return e
		}
		defer store.Close()
		return lst.Report(ctx, store, m, start.UTC(), end.UTC(), *out)
	}
	if e := os.MkdirAll(*state, 0700); e != nil {
		return e
	}
	lock, e := os.OpenFile(filepath.Join(*state, "lst.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errors.New("lst_process_already_running_all_modes_share_lock")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if command == "init-schema" {
		store, e := clickhouse.Open(ctx, cfg)
		if e != nil {
			return e
		}
		defer store.Close()
		if e = store.InitLSTSchema(ctx); e != nil {
			return e
		}
		fmt.Printf("LST schema ready: %s (7 LST tables + instrument)\n", *database)
		return nil
	}
	m, e := lst.LoadManifest(*manifest)
	if e != nil {
		return e
	}
	transport, e := lst.NewTransport(lst.TransportConfig{StateDir: *state, ArchiveDir: *evidence, Backfill: command == "backfill"})
	if e != nil {
		return e
	}
	defer func() { log.Printf("http_stats=%v", transport.Stats()) }()
	endpoint := os.Getenv("LST_RPC_URL")
	if endpoint == "" {
		endpoint = "https://eth.drpc.org"
	}
	rpc := &lst.RPC{Transport: transport, URL: endpoint}
	archive := ethereum.Archive{Dir: *evidence}
	if _, e = archive.Put(m.Raw); e != nil {
		return e
	}
	if command == "probe" {
		if *out == "var/lst-reports" {
			return errors.New("probe_requires_out_manifest_file")
		}
		head, e := rpc.Header(ctx, "finalized")
		if e != nil {
			return e
		}
		pinned, responses, e := rpc.VerifyIdentity(ctx, m, head, true)
		if e != nil {
			return e
		}
		raw, e := json.MarshalIndent(pinned, "", "  ")
		if e != nil {
			return e
		}
		raw = append(raw, '\n')
		if e = os.MkdirAll(filepath.Dir(*out), 0700); e != nil {
			return e
		}
		if e = os.WriteFile(*out, raw, 0600); e != nil {
			return e
		}
		proof := struct {
			BlockNumber             uint64
			BlockHash, ManifestHash string
			Responses               []string
		}{head.Number, lst.Hex(head.Hash), lst.Hex(lst.HashBytes(raw)), nil}
		for _, r := range responses {
			if r.PayloadHash != "" {
				proof.Responses = append(proof.Responses, lst.Hex(r.PayloadHash))
			}
		}
		h, e := archive.PutObject(proof)
		if e != nil {
			return e
		}
		fmt.Printf("verified pinned manifest=%s block=%d evidence=%s\n", *out, head.Number, h.String())
		return nil
	}
	if !m.Pinned() {
		return errors.New("manifest_not_pinned_run_probe_first")
	}
	// Collection opens the existing research database; only init-schema performs DDL.
	store, e := clickhouse.OpenLSTWriter(ctx, cfg)
	if e != nil {
		return e
	}
	defer store.Close()
	cexEndpoint := os.Getenv("LST_BINANCE_URL")
	if cexEndpoint == "" {
		cexEndpoint = "https://fapi.binance.com"
	}
	cex, e := lst.NewCEX(transport, cexEndpoint)
	if e != nil {
		return e
	}
	c := &lst.Collector{RPC: rpc, CEX: cex, Manifest: m, Store: store, Archive: archive, StateDir: *state}
	// Validate the target before trusting persisted cursors/pending batches.
	targetFile := filepath.Join(*state, "database-target")
	target := *address + "/" + *database
	previous, readErr := os.ReadFile(targetFile)
	if readErr == nil && strings.TrimSpace(string(previous)) != target {
		return errors.New("state_directory_database_target_changed")
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		return readErr
	}
	if os.IsNotExist(readErr) {
		if e = os.WriteFile(targetFile, []byte(target+"\n"), 0600); e != nil {
			return e
		}
	}
	if e = c.FlushPending(ctx); e != nil {
		return e
	}
	for {
		head, he := rpc.Header(ctx, "finalized")
		var proof []lst.Response
		if he == nil {
			_, proof, he = rpc.VerifyIdentity(ctx, m, head, false)
		}
		if he == nil {
			c.Metadata, he = cex.Metadata(ctx)
		}
		if he == nil {
			registered, re := store.RegisterInstruments(ctx, []model.Instrument{c.Metadata.Instrument})
			he = re
			if he == nil {
				c.Metadata.Instrument = registered[0]
				c.IdentityResponses = append(proof, head.Response)
				c.IdentityResponses = append(c.IdentityResponses, c.Metadata.ExchangeResponse, c.Metadata.FundingInfoResponse)
			}
		}
		if he == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, permanent := range []string{"source_disabled", "wrong_chain", "unknown_implementation", "code_hash_mismatch", "pool_changed", "pool_missing", "wrong_identity", "wrong_decimals", "manifest_not_pinned", "state_unavailable", "state_unreadable", "state_invalid"} {
			if strings.Contains(he.Error(), permanent) {
				return he
			}
		}
		log.Printf("initialization incomplete: %s; waiting 60s (host cooldown still applies)", he)
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	transport.MarkInitialized()
	progress := func(s string) { log.Print(s) }
	if command == "watch" {
		return c.Watch(ctx, *once, progress)
	}
	if *fromBlock != 0 || *toBlock != 0 {
		if *fromBlock == 0 || *toBlock < *fromBlock || *toBlock-*fromBlock >= 512 {
			return errors.New("explicit_range_must_be_1_to_512_blocks")
		}
		b, be := c.Logs(ctx, *fromBlock, *toBlock, "backfill")
		if ve := lst.Validate(b); ve != nil {
			return ve
		}
		if e = c.Commit(ctx, b); e != nil {
			return e
		}
		progress(fmt.Sprintf("explicit_logs status=%s requests=%d finalized=%d claims=%d", b.Capture.Status, len(b.Requests), len(b.Finalizations), len(b.Claims)))
		return be
	}
	return c.Backfill(ctx, *days, *ranges, progress)
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
