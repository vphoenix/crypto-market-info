package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

// Explicitly opt-in. All observations are public; database writes and cleanup
// are confined to a fresh isolated database, never the running collector's DB.
func TestOKXPairedPublicSoak(t *testing.T) {
	if os.Getenv("OKX_PAIRED_PUBLIC_SOAK") != "1" {
		t.Skip("set OKX_PAIRED_PUBLIC_SOAK=1 for bounded public full-pair capture")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.ClickHouse.Database = fmt.Sprintf("crypto_okx_pair_soak_%d", time.Now().UnixNano())
	cfg.MarketBookDepth = 5
	cfg.OKXPairedEnabled = true
	cfg.OKXPairedRefresh = 30 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store, err := chstore.Open(ctx, cfg.ClickHouse)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := ch.Open(&ch.Options{Addr: cfg.ClickHouse.Addresses, Auth: ch.Auth{Database: cfg.ClickHouse.Database, Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = conn.Close()
		_ = store.Close()
		admin, e := ch.Open(&ch.Options{Addr: cfg.ClickHouse.Addresses, Auth: ch.Auth{Database: "default", Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password}})
		if e == nil {
			_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+cfg.ClickHouse.Database+"` SYNC")
			_ = admin.Close()
		}
	}()
	if err = store.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	client := okx.NewClient()
	client.BaseURL = cfg.OKXREST
	done := make(chan error, 1)
	go func() { done <- collectOKXPaired(ctx, cfg, store, client, nil, CapacityPlan{}, slog.Default()) }()
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			select {
			case <-done:
			case <-time.After(50 * time.Second):
				t.Error("paired public capture did not drain")
			}
		}
	}()
	var catalog model.OKXPairObservation
	var minute time.Time
	for ctx.Err() == nil {
		select {
		case err := <-done:
			stopped = true
			t.Fatalf("public capture exited before verification: %v", err)
		default:
		}
		catalog, err = store.LatestOKXPairCatalog(ctx, time.Now().UTC())
		if err == nil {
			minute = catalog.EffectiveMinute
			var count uint64
			err = conn.QueryRow(ctx, "SELECT uniqExact(instrument_id) FROM `"+cfg.ClickHouse.Database+"`.order_book_minute FINAL WHERE minute_time=?", minute).Scan(&count)
			if err == nil && int(count) == 2*len(catalog.Pairs) {
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	if ctx.Err() != nil {
		t.Fatal("no complete full-pair minute", ctx.Err(), err)
	}
	valid, missing, depthErrors := 0, 0, 0
	var invalid []string
	for _, pair := range catalog.Pairs {
		for _, instrument := range []model.Instrument{pair.Spot, pair.Perpetual} {
			m, e := store.LoadMinute(ctx, instrument.ID, minute)
			if e != nil {
				t.Fatal(e)
			}
			if m.StoredDepth != 5 {
				depthErrors++
			}
			d, e := store.LoadDeltas(ctx, m.ID, 59)
			if e != nil {
				t.Fatal(e)
			}
			for sec := uint8(0); sec < 60; sec++ {
				book, ok, e := replay.AtSecond(m, d, sec)
				if e != nil {
					t.Fatalf("%s sec %d replay: %v", instrument.ExchangeSymbol, sec, e)
				}
				if ok {
					valid++
					if len(book.Bids) > 5 || len(book.Asks) > 5 {
						depthErrors++
					}
				} else {
					missing++
				}
			}
			if m.ValidBitmap != model.MinuteMask {
				invalid = append(invalid, instrument.ExchangeSymbol)
			}
		}
	}
	var loanRows uint64
	if err = conn.QueryRow(ctx, "SELECT count() FROM `"+cfg.ClickHouse.Database+"`.okx_public_loan_policy FINAL").Scan(&loanRows); err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"observed_at": time.Now().UTC(), "database": cfg.ClickHouse.Database, "isolated_database_removed_after_test": true, "pairs": len(catalog.Pairs), "sources": 2 * len(catalog.Pairs), "first_minute": minute, "stored_depth": 5, "valid_seconds": valid, "missing_seconds": missing, "depth_errors": depthErrors, "not_fully_valid_symbols": invalid, "loan_policy_rows": loanRows, "source_hashes": catalog.SourceHashes, "funding_enabled": cfg.FundingEnabled, "funding_scope": "WS subscription started; soak did not cross an hour; deterministic tests verify hourly persistence"}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("OKX_PAIRED_SOAK_REPORT"); path != "" {
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(string(raw))
	cancel()
	select {
	case err = <-done:
		stopped = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Second):
		t.Fatal("public capture failed to drain")
	}
	if missing > 0 || depthErrors > 0 {
		t.Fatalf("unexpected first-minute gaps=%d depth_errors=%d", missing, depthErrors)
	}
}
