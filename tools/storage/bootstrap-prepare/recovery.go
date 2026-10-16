package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	store "github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func (p *preparation) reader(ctx context.Context, family string) (*store.Client, error) {
	cfg := p.target
	cfg.Database = family
	cfg.Family = family
	cfg.ReadOnly = true
	cfg.ReadTimeout = 0 // Exercise the production 30-second reader default.
	cfg.Username = "cmi_query"
	cfg.PasswordFile = "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password"
	return store.Open(ctx, cfg)
}
func (p *preparation) recovery(ctx context.Context, includeMain bool) error {
	out := map[string]any{"checked_at_utc": time.Now().UTC(), "network_requests": 0, "production_writes": 0, "scope": "Read-only recovery dependencies at the sealed snapshot; final tail refresh still required at cutover"}
	queryConfig := p.target
	queryConfig.Username = "cmi_query"
	queryConfig.PasswordFile = "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password"
	queryConfig.ReadOnly = true
	queryConfig.ReadTimeout = 0
	db, e := store.Connect(ctx, queryConfig)
	if e != nil {
		return e
	}
	defer db.Close()
	if includeMain {
		fmt.Println("recovery main begin")
		main, e := p.reader(ctx, "crypto_market_info")
		if e != nil {
			return e
		}
		instruments, e := main.Instruments(ctx)
		if e != nil {
			main.Close()
			return e
		}
		if len(instruments) == 0 {
			main.Close()
			return fmt.Errorf("instrument identities missing")
		}
		fmt.Printf("recovery instruments finished count=%d; latest plan begin\n", len(instruments))
		plan, e := main.LatestOptionsPlan(ctx)
		if e != nil {
			main.Close()
			return e
		}
		fmt.Println("recovery latest plan finished; funding window begin")
		// runMarketSources reads only this bounded funding window. Options
		// RunCatalog restores LatestOptionsPlan, not the latest historical run.
		now := time.Now().UTC()
		fundingPending, e := main.LoadPendingFundingConfirmations(ctx, now.Add(-funding.StartupBackfillWindow), now)
		if e != nil {
			main.Close()
			return e
		}
		fmt.Println("recovery funding window finished; DEX anchors begin")
		var manifestRaw []byte
		if e = db.QueryRowContext(ctx, "SELECT manifest_hash FROM crypto_market_info.dex_block ORDER BY block_number DESC LIMIT 1").Scan(&manifestRaw); e != nil {
			main.Close()
			return e
		}
		if len(manifestRaw) != 32 {
			main.Close()
			return fmt.Errorf("DEX manifest identity width")
		}
		var manifest dex.Hash
		copy(manifest[:], manifestRaw)
		last, e := main.DEXLastBlock(ctx, manifest, false)
		if e != nil {
			main.Close()
			return e
		}
		final, e := main.DEXLastBlock(ctx, manifest, true)
		if e != nil {
			main.Close()
			return e
		}
		pendingReceipts, e := main.DEXPendingReceiptBlocks(ctx, manifest)
		if e != nil {
			main.Close()
			return e
		}
		pendingLogs, e := main.DEXPendingLogBlocks(ctx, manifest)
		if e != nil {
			main.Close()
			return e
		}
		gapStart, e := main.DEXGapStart(ctx, manifest)
		if e != nil {
			main.Close()
			return e
		}
		if last == nil || final == nil {
			main.Close()
			return fmt.Errorf("DEX recovery anchors missing")
		}
		fmt.Println("recovery DEX anchors finished; tail members begin")
		// Reconcile can revise every block from its finalized checkpoint onward;
		// all those members must already exist, even though history is closed.
		tail, e := main.DEXBlockRange(ctx, manifest, final.Number, last.Number)
		if e != nil {
			main.Close()
			return e
		}
		if _, e = main.DEXQuotes(ctx, tail); e != nil {
			main.Close()
			return fmt.Errorf("DEX recovery tail quote members: %w", e)
		}
		out["main"] = map[string]any{"instruments": len(instruments), "latest_options_plan_id": plan.ID, "funding_startup_window_hours": 72, "pending_funding_confirmations": len(fundingPending), "dex_last_block": last.Number, "dex_final_block": final.Number, "dex_recovery_tail_batches_checked": len(tail), "pending_receipt_blocks": len(pendingReceipts), "pending_log_blocks": len(pendingLogs), "dex_gap_start": gapStart}
		main.Close()
	}
	rm, e := reserve.LoadManifest("config/reserve-ethereum.json")
	if e != nil {
		return e
	}
	rc, e := p.reader(ctx, "crypto_market_info_reserve")
	if e != nil {
		return e
	}
	fmt.Println("recovery Reserve captures begin")
	rcaps, e := rc.ReserveCaptures(ctx, string(rm.Hash[:]))
	if e != nil {
		rc.Close()
		return e
	}
	if len(rcaps) == 0 {
		rc.Close()
		return fmt.Errorf("Reserve captures missing")
	}
	through := reserve.ContiguousLiveCursor(rcaps, 26113530, false)
	complete := reserve.CoveredThrough(rcaps, 26113531)
	var checked int
	for i := len(rcaps) - 1; i >= 0 && checked < 3; i-- {
		if rcaps[i].Committed {
			if _, e = rc.ReserveBatch(ctx, rcaps[i]); e != nil {
				rc.Close()
				return e
			}
			checked++
		}
	}
	out["reserve"] = map[string]any{"captures": len(rcaps), "live_contiguous_through": through, "covered_through": complete, "latest_batches_rechecked": checked}
	fmt.Println("recovery Reserve finished")
	rc.Close()
	lm, e := lst.LoadManifest("config/lst-lido-ethereum.json")
	if e != nil {
		return e
	}
	lc, e := p.reader(ctx, "crypto_market_info_lst")
	if e != nil {
		return e
	}
	fmt.Println("recovery LST captures begin")
	lcaps, e := lc.LSTCaptures(ctx, lm.Hash)
	if e != nil {
		lc.Close()
		return e
	}
	if len(lcaps) == 0 {
		lc.Close()
		return fmt.Errorf("LST captures missing")
	}
	for _, cap := range lcaps[max(0, len(lcaps)-3):] {
		if _, e = lc.LSTBatch(ctx, cap); e != nil {
			lc.Close()
			return e
		}
	}
	out["lst"] = map[string]any{"captures": len(lcaps), "latest_batches_rechecked": min(3, len(lcaps))}
	fmt.Println("recovery LST finished")
	lc.Close()
	kcfg, e := keeper.LoadConfig("config/justlend-keeper-tron.json")
	if e != nil {
		return e
	}
	stateFile := keeper.StateFile(filepath.Join(p.root, "rollback", "justlend-keeper-state"))
	if _, e = os.Stat(stateFile); e != nil {
		return e
	}
	state, e := keeper.LoadState(stateFile, "127.0.0.1:9000/crypto_market_info_justlend_keeper", kcfg)
	if e != nil {
		return e
	}
	kc, e := p.reader(ctx, "crypto_market_info_justlend_keeper")
	if e != nil {
		return e
	}
	var latest time.Time
	fmt.Println("recovery Keeper latest capture begin")
	if e = db.QueryRowContext(ctx, "SELECT MAX(capture_started_at) FROM crypto_market_info_justlend_keeper.jl_keeper_capture").Scan(&latest); e != nil {
		kc.Close()
		return e
	}
	kcaps, e := kc.KeeperCaptures(ctx, latest.Add(-time.Minute), latest.Add(time.Microsecond))
	if e != nil {
		kc.Close()
		return e
	}
	checked = 0
	for _, cap := range kcaps {
		if cap.Committed {
			if _, e = kc.KeeperBatch(ctx, cap); e != nil {
				kc.Close()
				return e
			}
			checked++
		}
	}
	for _, at := range []string{"2026-10-10T08:51:22.401925Z", "2026-10-10T08:52:20.313777Z"} {
		lo, e := time.Parse(time.RFC3339Nano, at)
		if e != nil {
			kc.Close()
			return e
		}
		caps, e := kc.KeeperCaptures(ctx, lo, lo.Add(time.Microsecond))
		if e != nil || len(caps) != 1 {
			kc.Close()
			return fmt.Errorf("original Keeper gap marker missing: %v", e)
		}
		if _, e = kc.KeeperBatch(ctx, caps[0]); e == nil {
			kc.Close()
			return fmt.Errorf("abandoned Keeper cost batch became valid")
		}
	}
	out["keeper"] = map[string]any{"state_target": state.Target, "config_identity_preserved": true, "request_budget_used": state.Used, "candidate_count": len(state.Candidates), "scan_count": len(state.Scans), "latest_batches_rechecked": checked, "two_abandoned_batches_remain_invalid": true}
	fmt.Println("recovery Keeper finished")
	kc.Close()
	file := "startup-reader-recovery.json"
	if !includeMain {
		file = "lowfrequency-reader-recovery.json"
		out["scope"] = "Read-only Reserve/LST/Keeper dependencies; main recovery is not included"
	}
	return writeJSON(filepath.Join(p.root, file), out)
}
