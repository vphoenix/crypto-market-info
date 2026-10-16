// cutover-recovery reads the stopped service's real recovery dependencies.
// It makes no public API requests and never writes production facts or state.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	store "github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 || !filepath.IsAbs(os.Args[2]) {
		return fmt.Errorf("usage: cutover-recovery justlend-keeper|reserve ABSOLUTE_SERVICE_DIRECTORY")
	}
	service, root := os.Args[1], os.Args[2]
	families := map[string]string{"justlend-keeper": "crypto_market_info_justlend_keeper", "reserve": "crypto_market_info_reserve", "collector": "crypto_market_info"}
	family, ok := families[service]
	if !ok {
		return fmt.Errorf("unsupported recovery service")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c, err := store.Open(ctx, store.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "cmi_query", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password", Family: family, Database: family, ReadOnly: true, MaxConnections: 1})
	if err != nil {
		return err
	}
	defer c.Close()
	out := map[string]any{"service": service, "checked_at_utc": time.Now().UTC(), "public_api_requests": 0, "production_writes": 0, "startup_recovery_passed": false}
	if service == "collector" {
		instruments, err := c.Instruments(ctx)
		if err != nil || len(instruments) == 0 {
			return fmt.Errorf("instrument recovery: %w", err)
		}
		plan, err := c.LatestOptionsPlan(ctx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		pending, err := c.LoadPendingFundingConfirmations(ctx, now.Add(-funding.StartupBackfillWindow), now)
		if err != nil {
			return err
		}
		manifest := ethereum.ManifestHash()
		head, err := c.DEXLastBlock(ctx, manifest, false)
		if err != nil {
			return err
		}
		final, err := c.DEXLastBlock(ctx, manifest, true)
		if err != nil || head == nil || final == nil {
			return fmt.Errorf("DEX anchors: %w", err)
		}
		tail, err := c.DEXBlockRange(ctx, manifest, final.Number, head.Number)
		if err != nil {
			return err
		}
		logs, err := c.DEXPendingLogBlocks(ctx, manifest)
		if err != nil {
			return err
		}
		receipts, err := c.DEXPendingReceiptBlocks(ctx, manifest)
		if err != nil {
			return err
		}
		older, err := c.DEXUnfinalizedBlocks(ctx, manifest, final.Number)
		if err != nil {
			return err
		}
		seen := map[dex.Hash]bool{}
		checked := 0
		for _, b := range append(append(append(tail, logs...), receipts...), older...) {
			if seen[b.Batch] {
				continue
			}
			seen[b.Batch] = true
			if _, err = c.DEXQuotes(ctx, []dex.Block{b}); err != nil {
				return err
			}
			checked++
		}
		out["instruments"] = len(instruments)
		out["latest_plan"] = plan.ID
		out["funding_pending"] = len(pending)
		out["dex_head"] = head.Number
		out["dex_finalized"] = final.Number
		out["dex_recovery_batches_checked"] = checked
	} else if service == "justlend-keeper" {
		cfg, err := keeper.LoadConfig("config/justlend-keeper-tron.json")
		if err != nil {
			return err
		}
		s, err := keeper.LoadState(keeper.StateFile(filepath.Join(root, "state-after-stop")), "127.0.0.1:9000/"+family, cfg)
		if err != nil {
			return err
		}
		parents := map[string]bool{}
		for _, op := range s.Ops {
			if op.Parent != nil && !parents[op.Parent.CaptureId.String()] {
				if _, err = c.KeeperBatch(ctx, *op.Parent); err != nil {
					return fmt.Errorf("pending enrichment parent: %w", err)
				}
				parents[op.Parent.CaptureId.String()] = true
			}
		}
		parent, err := c.KeeperNextEnrichment(ctx, cfg.Hash(), time.Now().UTC())
		if err != nil {
			return err
		}
		if parent != nil {
			if _, err = c.KeeperBatch(ctx, *parent); err != nil {
				return fmt.Errorf("next enrichment parent: %w", err)
			}
		}
		out["budget_used"] = s.Used
		out["candidates"] = len(s.Candidates)
		out["scans"] = len(s.Scans)
		out["pending_operations"] = len(s.Ops)
		out["pending_parents_checked"] = len(parents)
		out["next_enrichment_available"] = parent != nil
		out["state_identity_preserved"] = true
	} else {
		m, err := reserve.LoadManifest("config/reserve-ethereum.json")
		if err != nil {
			return err
		}
		caps, err := c.ReserveCaptures(ctx, string(m.Hash[:]))
		if err != nil {
			return err
		}
		checked := 0
		for i := len(caps) - 1; i >= 0 && checked < 5; i-- {
			if !caps[i].Committed {
				continue
			}
			if _, err = c.ReserveBatch(ctx, caps[i]); err != nil {
				return err
			}
			checked++
		}
		out["captures"] = len(caps)
		out["latest_batches_checked"] = checked
		out["covered_through"] = reserve.CoveredThrough(caps, 26113531)
		out["live_contiguous_through"] = reserve.ContiguousLiveCursor(caps, 26113530, false)
	}
	out["startup_recovery_passed"] = true
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(root, "startup-recovery.json"), append(b, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
