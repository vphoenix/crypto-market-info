package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/dexruntime"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/backend"
)

func run(ctx context.Context, args []string) (result error) {
	if len(args) == 0 {
		return errors.New("usage: dex-runtime-state convert|verify|rollback|handoff [--state-dir --epoch-file --export]")
	}
	command := args[0]
	if command != "convert" && command != "verify" && command != "rollback" && command != "handoff" {
		return errors.New("unknown runtime command")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	dir := f.String("state-dir", cfg.DEXRuntimeDir, "verified DEX runtime state")
	handoff := f.String("handoff-plan", "", "frozen finite operation manifest for stopped-service maintenance")
	input := f.String("input-inventory", "", "frozen migrated/pending/unreadable input inventory; required for convert")
	epoch := f.String("epoch-file", cfg.DEXRuntimeEpochFile, "database restore epoch")
	export := f.String("export", "", "new current-state export directory")
	if err = f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || *dir == "" || *epoch == "" || cfg.Storage.Name != "mysql" || command == "rollback" && *export == "" || command == "convert" && *export != "" {
		return errors.New("invalid DEX runtime options")
	}
	if command == "convert" && *input == "" {
		return errors.New("runtime_conversion_requires_input_inventory")
	}
	if command == "handoff" && *handoff == "" || command != "handoff" && *handoff != "" {
		return errors.New("runtime_handoff_requires_explicit_plan")
	}
	// This explicit tool never constructs a chain client or sends source requests.
	db, err := backend.OpenDEX(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer db.Close()
	provider, ok := db.(interface {
		RuntimeBinding(context.Context, string, string, uint32) (runtimecheckpoint.Binding, error)
	})
	if !ok {
		return errors.New("runtime binding unavailable")
	}
	manifest := ethereum.ManifestHash()
	check := func(ctx context.Context) (runtimecheckpoint.Binding, error) {
		return provider.RuntimeBinding(ctx, manifest.String(), *epoch, 1)
	}
	binding, err := check(ctx)
	if err != nil {
		return err
	}
	lock, err := runtimecheckpoint.LockTarget(binding)
	if err != nil {
		return err
	}
	defer lock.Close()
	if command == "convert" {
		report, err := dexruntime.Convert(ctx, *dir, binding, *input, manifest, db, check, func(s string) { log.Print(s) })
		if e := json.NewEncoder(os.Stdout).Encode(report); err == nil {
			err = e
		}
		return err
	}
	if command == "handoff" {
		ctx = runtimecheckpoint.MaintenanceContext(ctx)
	}
	owned, err := dexruntime.Open(ctx, *dir, binding, manifest, db, nil)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, owned.CloseRuntime()) }()
	if command == "handoff" {
		report, err := owned.Handoff(ctx, *handoff)
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
	var report dexruntime.Inspection
	if command == "rollback" {
		report, err = owned.Rollback(ctx, *export)
	} else {
		report, err = owned.Inspect(ctx, *export)
	}
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
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		log.Print(err)
		os.Exit(1)
	}
}
