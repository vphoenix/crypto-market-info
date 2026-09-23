package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Opt-in capacity measurement only. These are explicitly synthetic day-shaped
// fixtures based on real public samples, never historical observations or P&L.
// Facts are seeded in 500-block chunks to avoid a rapid small-part storm.
func TestDEXDailyCapacity(t *testing.T) {
	if os.Getenv("DEX_CAPACITY") != "1" {
		t.Skip("set DEX_CAPACITY=1 with CLICKHOUSE_INTEGRATION=1 for synthetic daily measurement")
	}
	root := os.Getenv("DEX_CAPACITY_SAMPLES")
	if root == "" {
		t.Fatal("DEX_CAPACITY_SAMPLES required")
	}
	for _, name := range []string{"sample-with-events", "sample-no-events"} {
		t.Run(name, func(t *testing.T) {
			raw, e := os.ReadFile(filepath.Join(root, name, "sample.json"))
			if e != nil {
				t.Fatal(e)
			}
			var seed dex.Batch
			if e = json.Unmarshal(raw, &seed); e != nil {
				t.Fatal(e)
			}
			c := dexIntegrationClient(t)
			ctx := context.Background()
			start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			const count = 7200
			for offset := 0; offset < count; offset += 500 {
				var blockRows, skyRows, quoteRows, logRows, receiptRows [][]any
				for i := offset; i < min(offset+500, count); i++ {
					a := dex.Anchor{ChainID: 1, Number: uint64(100000 + i), Hash: dex.ObjectHash([]any{"synthetic block", i}), Manifest: dex.Digest([]byte("synthetic daily capacity only")), Batch: dex.ObjectHash([]any{"synthetic batch", i}), Payload: dex.ObjectHash([]any{"synthetic source", i}), Time: start.Add(time.Duration(i) * 12 * time.Second)}
					qs := make([]dex.Quote, len(seed.Quotes))
					for j, orig := range seed.Quotes {
						q := orig
						q.Anchor = a
						q.Payload = dex.ObjectHash([]any{"synthetic quote response group", i, j / 20})
						q.AvailableAt = a.Time.Add(time.Second)
						q.SetID()
						qs[j] = q
						quoteRows = append(quoteRows, quoteValues(q))
					}
					logs := make([]dex.Log, len(seed.Logs))
					for j, orig := range seed.Logs {
						l := orig
						l.Anchor = a
						l.TxHash = dex.ObjectHash([]any{"synthetic tx", i, l.TxIndex})
						l.Payload = dex.ObjectHash([]any{"synthetic log", i, j})
						logs[j] = l
						logRows = append(logRows, logValues(l))
					}
					receipts := make([]dex.Receipt, len(seed.Receipts))
					for j, orig := range seed.Receipts {
						r := orig
						r.Anchor = a
						r.TxHash = dex.ObjectHash([]any{"synthetic tx", i, r.TxIndex})
						r.Batch = dex.ObjectHash([]any{"synthetic receipt batch", i, j})
						r.Payload = dex.ObjectHash([]any{"synthetic receipt payload", i, j})
						r.ReceiptHash = r.Payload
						r.AvailableAt = a.Time.Add(time.Second)
						receipts[j] = r
						receiptRows = append(receiptRows, receiptValues(r))
					}
					s := *seed.Sky
					s.Anchor = a
					skyRows = append(skyRows, append(anchorValues(s.Anchor), s.Module, s.Tin, s.Tout, s.Buf, s.DAICash, s.USDCCash, s.Allowance, s.VatLive, s.DAIJoinLive, s.DAIJoinWard, s.USDSJoinWard, s.IdentityOK, s.Complete, s.Reason))
					b := seed.Block
					b.Anchor = a
					b.Parent = dex.ObjectHash([]any{"synthetic block", i - 1})
					b.ReceivedAt = a.Time
					b.AvailableAt = a.Time.Add(time.Second)
					b.Capture = "research"
					b.Finality = "finalized"
					b.Revision = uint64(b.AvailableAt.UnixMicro())
					b.ActualQuotes = uint32(len(qs))
					b.QuoteMembers = dex.QuoteDigest(qs)
					b.LogCount = uint32(len(logs))
					b.LogMembers = dex.LogDigest(logs)
					b.ReceiptCount = uint32(len(receipts))
					b.ReceiptMembers = dex.ReceiptDigest(receipts)
					blockRows = append(blockRows, blockValues(b))
				}
				for _, op := range []struct {
					table, columns string
					rows           [][]any
				}{
					{"dex_sky_state", dexAnchorColumns + ",module_id,tin,tout,buf,dai_cash,usdc_pocket_cash,pocket_allowance,vat_live,dai_join_live,dai_join_ward,usds_join_ward,identity_ok,state_complete,reason", skyRows},
					{"dex_route_quote", dexQuoteColumns, quoteRows}, {"dex_log", dexLogColumns, logRows}, {"dex_tx_receipt", dexReceiptColumns, receiptRows}, {"dex_block", dexBlockColumns, blockRows},
				} {
					if e = c.dexInsert(ctx, op.table, op.columns, op.rows); e != nil {
						t.Fatal(e)
					}
				}
			}
			for _, table := range []string{"dex_block", "dex_sky_state", "dex_route_quote", "dex_log", "dex_tx_receipt"} {
				if e = c.conn.Exec(ctx, "OPTIMIZE TABLE "+c.table(table)+" FINAL"); e != nil {
					t.Fatal(e)
				}
			}
			var compressed uint64
			if e = c.conn.QueryRow(ctx, "SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = ? AND active", c.database).Scan(&compressed); e != nil {
				t.Fatal(e)
			}
			began := time.Now()
			blocks, e := c.DEXBlocks(ctx, start, start.Add(24*time.Hour))
			if e != nil || len(blocks) != count {
				t.Fatal("day header query", len(blocks), e)
			}
			quotes, e := c.DEXQuotes(ctx, blocks)
			if e != nil || len(quotes) != count {
				t.Fatal("day quote query", len(quotes), e)
			}
			dayRead := time.Since(began)
			samples := make([]time.Duration, 20)
			for i := range samples {
				index := i * count / len(samples)
				now := time.Now()
				qs, e := c.DEXQuotes(ctx, blocks[index:index+1])
				if e != nil || len(qs[blocks[index].Batch]) != 58 {
					t.Fatal("single block query", e)
				}
				samples[i] = time.Since(now)
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			t.Logf("SYNTHETIC capacity only: blocks=%d quotes=%d logs=%d receipts=%d compressed_bytes=%d all_day_read_ms=%d one_block_read_p95_us=%d", count, count*len(seed.Quotes), count*len(seed.Logs), count*len(seed.Receipts), compressed, dayRead.Milliseconds(), samples[18].Microseconds())
			if dir := os.Getenv("DEX_CAPACITY_OUTPUT"); dir != "" {
				result := map[string]any{"kind": "synthetic_daily_capacity_not_live_history", "source_sample": name, "blocks": count, "quotes": count * len(seed.Quotes), "logs": count * len(seed.Logs), "receipts": count * len(seed.Receipts), "compressed_table_bytes": compressed, "all_day_read_ms": dayRead.Milliseconds(), "single_block_read_p95_us": samples[18].Microseconds(), "evidence_archive_bytes_included": false}
				encoded, _ := json.MarshalIndent(result, "", "  ")
				if e = os.WriteFile(filepath.Join(dir, fmt.Sprintf("capacity-%s.json", name)), append(encoded, '\n'), 0600); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
