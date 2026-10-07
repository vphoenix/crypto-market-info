package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/app"
	"github.com/vphoenix/crypto-market-info/internal/config"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func main() {
	initDEX := flag.Bool("init-dex-schema", false, "create the five DEX tables only and exit; does not start any collector")
	printDDL := flag.Bool("print-ddl", false, "print concrete ClickHouse DDL and exit")
	printOptions := flag.Bool("print-options-plan", false, "fetch public Deribit catalog and print collection scope without database or websocket connections")
	printUniverse := flag.Bool("print-perp-universe", false, "fetch public catalogs and print validated perpetual universe without database or websocket connections")
	printCatalogs := flag.Bool("print-perp-catalogs", false, "print complete eligible public catalogs for alias maintenance without database or websocket connections")
	printPairs := flag.Bool("print-okx-pairs", false, "fetch public paired spot/perpetual catalog without database or websocket connections")
	replayInstrument := flag.Uint("replay-instrument", 0, "instrument_id to replay")
	replayTime := flag.String("replay-time", "", "UTC RFC3339 second to replay")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		fatal(err)
	}
	if *printDDL {
		statements, schemaErr := chstore.SchemaStatements(cfg.ClickHouse.Database)
		if schemaErr != nil {
			fatal(schemaErr)
		}
		dexDDL, dexErr := chstore.DEXSchemaStatements(cfg.ClickHouse.Database)
		if dexErr != nil {
			fatal(dexErr)
		}
		statements = append(statements, dexDDL...)
		fmt.Println(strings.Join(statements, ";\n\n") + ";")
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *initDEX {
		store, e := chstore.Open(ctx, cfg.ClickHouse)
		if e != nil {
			fatal(e)
		}
		defer store.Close()
		if e = store.InitDEXSchema(ctx); e != nil {
			fatal(e)
		}
		fmt.Println("DEX schema ready: 5 tables in " + cfg.ClickHouse.Database)
		return
	}
	if *printOptions {
		if err = app.PrintOptionsPlan(ctx, cfg, os.Stdout); err != nil {
			fatal(err)
		}
		return
	}
	if *printPairs {
		if err = app.PrintOKXPairedCatalog(ctx, cfg, os.Stdout); err != nil {
			fatal(err)
		}
		return
	}
	if *printCatalogs {
		if err = app.PrintPerpetualCatalogs(ctx, cfg, os.Stdout, slog.Default()); err != nil {
			fatal(err)
		}
		return
	}
	if *printUniverse {
		if err = app.PrintPerpetualUniverse(ctx, cfg, os.Stdout, slog.Default()); err != nil {
			fatal(err)
		}
		return
	}
	if *replayInstrument != 0 || *replayTime != "" {
		if *replayInstrument == 0 || *replayTime == "" {
			fatal(fmt.Errorf("replay requires both -replay-instrument and -replay-time"))
		}
		if uint64(*replayInstrument) > math.MaxUint32 {
			fatal(fmt.Errorf("replay instrument_id exceeds UInt32"))
		}
		at, parseErr := time.Parse(time.RFC3339, *replayTime)
		if parseErr != nil {
			fatal(parseErr)
		}
		store, openErr := chstore.Open(ctx, cfg.ClickHouse)
		if openErr != nil {
			fatal(openErr)
		}
		defer store.Close()
		snapshot, valid, replayErr := store.ReplayBook(ctx, uint32(*replayInstrument), at)
		if replayErr != nil {
			fatal(replayErr)
		}
		output := struct {
			Valid    bool `json:"valid"`
			Snapshot any  `json:"snapshot,omitempty"`
		}{Valid: valid}
		if valid {
			output.Snapshot = snapshot
		}
		encoded, _ := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(encoded))
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err = app.Run(ctx, cfg, logger); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
