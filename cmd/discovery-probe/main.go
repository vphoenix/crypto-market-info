package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/app"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"log/slog"
	"os"
	"strings"
	"time"
)

func main() {
	database := flag.String("database", "", "isolated discovery_validation_ database")
	assets := flag.String("assets", "BTC,ETH", "eligible public catalog bases to capture")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg.ClickHouse.Database = *database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err = app.RunDiscoveryProbe(ctx, cfg, strings.Split(*assets, ","), slog.Default()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
