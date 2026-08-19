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
	printDDL := flag.Bool("print-ddl", false, "print concrete ClickHouse DDL and exit")
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
		fmt.Println(strings.Join(statements, ";\n\n") + ";")
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
