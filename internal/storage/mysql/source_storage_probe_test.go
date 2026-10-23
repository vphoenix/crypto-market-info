package mysql

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

// Optional, bounded read-only deployment probe, deliberately separate from the
// DDL fixtures. No table scans over legacy history or mutable methods are used.
func TestSourceStorageProductionReadOnlyProbe(t *testing.T) {
	if os.Getenv("CMI_SOURCE_READONLY_PROBE") != "1" {
		t.Skip("explicit production read-only probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Open(ctx, Config{Socket: os.Getenv("CMI_SOURCE_READONLY_SOCKET"), Username: os.Getenv("CMI_SOURCE_READONLY_USER"), PasswordFile: os.Getenv("CMI_SOURCE_READONLY_PASSWORD_FILE"), Database: "crypto_market_info", MaxConnections: 1, ReadOnly: true, SelectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var server string
	if err = c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		t.Fatal("unexpected production identity", err)
	}
	if !c.sourceStorageAvailable {
		t.Fatal("extension not detected")
	}
	checked := 0
	for _, kind := range []string{"cex_book_minute_commit", "funding_forecast_observation", "cex_trade_bar_1m", "okx_public_capital_parameters"} {
		tab, _ := c.mapping("source_batch_status")
		rows, err := readRecords(ctx, c.db, c.table("source_batch_status_v2"), tab, "WHERE source_kind=? ORDER BY _storage_id DESC LIMIT 4", []byte(kind))
		if err != nil || len(rows) == 0 {
			t.Fatal("new source publications missing", kind, err)
		}
		for _, r := range rows {
			p, err := scanProof(r)
			if err != nil {
				t.Fatal(err)
			}
			want, err := scanPublication(r)
			if err != nil {
				t.Fatal(err)
			}
			intents, err := c.records(ctx, c.db, "source_publication_intents", "WHERE source_kind=? AND source_id=?", []byte(kind), []byte(p.ID))
			if err != nil || len(intents) != 1 {
				t.Fatal("intent missing/duplicate", err)
			}
			got, err := scanPublication(intents[0])
			if err != nil || got != want {
				t.Fatal("intent/status mismatch", err)
			}
			if r["complete"] != uint8(1) || p.KnownMS > p.PublishedMS || p.ObservedMS > p.KnownMS || p.SourceMS > p.ObservedMS {
				t.Fatal("publication ordering changed")
			}
			if kind == "cex_book_minute_commit" {
				id, err := strconv.ParseUint(p.ID, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				at := time.UnixMilli(want.EffectiveMS).UTC()
				b, err := c.loadCommittedBook(ctx, p.InstrumentID, at)
				if err != nil || b.Minute.ID != id || model.DiscoveryHash(model.TradingMinuteBody(b)) != p.Hash {
					t.Fatal("CEX membership/digest mismatch", err)
				}
			} else {
				if _, err = c.discoverySources(ctx, p); err != nil {
					t.Fatal("source evidence changed", kind, err)
				}
			}
			checked++
		}
		t.Logf("new source exact checks: %s=%d", kind, len(rows))
	}
	// Select a known pre-upgrade CEX identity through the existing business index.
	legacyAt := time.Date(2026, 10, 19, 13, 33, 0, 0, time.UTC)
	id, err := model.MinuteID(1, legacyAt)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND source_id=?", []byte("cex_book_minute_commit"), []byte(strconv.FormatUint(id, 10)))
	if err != nil || len(rows) != 1 {
		t.Fatal("legacy source proof missing", len(rows), err)
	}
	p, err := scanProof(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.loadCommittedBook(ctx, 1, legacyAt)
	if err != nil || model.DiscoveryHash(model.TradingMinuteBody(b)) != p.Hash {
		t.Fatal("legacy CEX changed", err)
	}
	t.Logf("checked %d new publications and one legacy committed minute; read-only", checked)
}
