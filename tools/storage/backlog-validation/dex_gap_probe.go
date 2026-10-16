package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// A finite read-only comparison on one physical connection. Only heights and
// session counters are read; no facts, source settings or queues are changed.
func probeDEXGap(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cfg := config(false)
	cfg.ReadTimeout, cfg.SelectTimeout, cfg.MaxConnections = 10*time.Second, 5*time.Second, 1
	db, err := mysql.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	m := ethereum.ManifestHash()
	table := "`crypto_market_info`.`dex_block` FORCE INDEX (`cmi_dex_committed`)"
	scope := " WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1"
	out := map[string]any{"utc": time.Now().UTC(), "read_only": true, "scope": "production Ethereum manifest, canonical committed heights"}
	defer func() { _ = save(workspaceEvidence+"/dex-gap-query-comparison.json", out) }()
	stats := func() (map[string]uint64, error) {
		rows, e := conn.QueryContext(ctx, "SHOW SESSION STATUS WHERE Variable_name IN ('Created_tmp_tables','Created_tmp_disk_tables','Sort_rows','Bytes_sent')")
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		r := map[string]uint64{}
		for rows.Next() {
			var name string
			var n uint64
			if e = rows.Scan(&name, &n); e != nil {
				return nil, e
			}
			r[name] = n
		}
		return r, rows.Err()
	}
	before, err := stats()
	if err != nil {
		return err
	}
	started := time.Now()
	var old uint64
	err = conn.QueryRowContext(ctx, "SELECT COALESCE(MIN(block_number+1),0) FROM (SELECT block_number,LEAD(block_number) OVER(ORDER BY block_number) AS next_block FROM "+table+scope+") AS coverage WHERE next_block>block_number+1", m[:]).Scan(&old)
	out["old_elapsed_ms"] = time.Since(started).Milliseconds()
	if err != nil {
		out["old_error"] = err.Error()
		return err
	}
	afterOld, err := stats()
	if err != nil {
		return err
	}
	started = time.Now()
	rows, err := conn.QueryContext(ctx, "SELECT block_number FROM "+table+scope+" ORDER BY block_number", m[:])
	if err != nil {
		return err
	}
	gap, count, err := scanGapProbe(rows)
	rows.Close()
	out["stream_elapsed_ms"] = time.Since(started).Milliseconds()
	if err != nil {
		return err
	}
	afterNew, err := stats()
	if err != nil {
		return err
	}
	delta := func(a, b map[string]uint64) map[string]uint64 {
		r := map[string]uint64{}
		for k, v := range b {
			r[k] = v - a[k]
		}
		return r
	}
	out["old_session_delta"], out["stream_session_delta"] = delta(before, afterOld), delta(afterOld, afterNew)
	out["old_gap"], out["stream_gap"], out["stream_heights"] = old, gap, count
	// Separate statements may observe an intervening valid block repair;
	// equality is diagnostic evidence, not a transactional source invariant.
	out["same_observed_gap"] = old == gap
	fmt.Printf("read-only gap comparison: old=%d stream=%d heights=%d\n", old, gap, count)
	return save(workspaceEvidence+"/dex-gap-query-comparison.json", out)
}

func scanGapProbe(rows *sql.Rows) (uint64, int, error) {
	var prev, gap uint64
	count := 0
	for rows.Next() {
		var h uint64
		if err := rows.Scan(&h); err != nil {
			return 0, count, err
		}
		if count > 0 && h < prev {
			return 0, count, fmt.Errorf("unstable height order")
		}
		if count > 0 && gap == 0 && h > prev && h-prev > 1 {
			gap = prev + 1
		}
		prev = h
		count++
	}
	return gap, count, rows.Err()
}
