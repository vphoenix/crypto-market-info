package clickhouse

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

// These are full synthetic days, not projections of a short public soak.
// The opt-in test uses fresh databases and also checks the production replay.
func TestClickHouseFiveTenSyntheticDayMeasurement(t *testing.T) {
	if os.Getenv("FIVE_DEPTH_DAY_MEASUREMENT") != "1" {
		t.Skip("set FIVE_DEPTH_DAY_MEASUREMENT=1")
	}
	for _, active := range []bool{true, false} {
		for _, depth := range []int{5, 10} {
			t.Run(fmt.Sprintf("active_%t_depth_%d", active, depth), func(t *testing.T) {
				c := commonUniverseTestClient(t)
				ctx := context.Background()
				day := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
				var minutes []model.MinuteBook
				var deltas []model.BookDelta
				for m := 0; m < 1440; m++ {
					at := day.Add(time.Duration(m) * time.Minute)
					b, err := sampler.NewMinuteBufferWithDepth(1, at, depth)
					if err != nil {
						t.Fatal(err)
					}
					for sec := 0; sec < 60; sec++ {
						offset := 0
						if active {
							offset = (m*60 + sec) % 100
						}
						s := model.BookSnapshot{InstrumentID: 1, SourceTime: at.Add(time.Duration(sec) * time.Second)}
						for level := 0; level < depth; level++ {
							s.Bids = append(s.Bids, model.Level{PriceTick: int64(100000 + offset - level), QtyLot: uint64(100 + offset + level)})
							s.Asks = append(s.Asks, model.Level{PriceTick: int64(100101 + offset + level), QtyLot: uint64(110 + offset + level)})
						}
						if err = b.Sample(s.SourceTime, s, true); err != nil {
							t.Fatal(err)
						}
					}
					batch, ok := b.Batch()
					if !ok {
						t.Fatal("missing minute")
					}
					if err := validateBatch(batch); err != nil {
						t.Fatal(err)
					}
					minutes = append(minutes, batch.Minute)
					deltas = append(deltas, batch.Deltas...)
				}
				if err := c.insertDeltas(ctx, deltas); err != nil {
					t.Fatal(err)
				}
				if err := c.insertMinutes(ctx, minutes); err != nil {
					t.Fatal(err)
				}
				for _, table := range []string{"order_book_minute", "order_book_second_delta"} {
					if err := c.conn.Exec(ctx, "OPTIMIZE TABLE "+c.table(table)+" FINAL"); err != nil {
						t.Fatal(err)
					}
				}
				var bytes uint64
				if err := c.conn.QueryRow(ctx, "SELECT sum(data_compressed_bytes) FROM system.parts WHERE active AND database=? AND table IN ('order_book_minute','order_book_second_delta')", c.database).Scan(&bytes); err != nil {
					t.Fatal(err)
				}
				latency := make([]time.Duration, 100)
				for n := range latency {
					at := day.Add(time.Duration((n*137)%1440) * time.Minute)
					start := time.Now()
					minute, err := c.LoadMinute(ctx, 1, at)
					if err != nil {
						t.Fatal(err)
					}
					ds, err := c.LoadDeltas(ctx, minute.ID, 59)
					if err != nil {
						t.Fatal(err)
					}
					book, valid, err := replay.AtSecond(minute, ds, uint8(n%60))
					if err != nil || !valid || len(book.Bids) != depth || len(book.Asks) != depth {
						t.Fatalf("depth replay: %v", err)
					}
					latency[n] = time.Since(start)
				}
				sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
				t.Logf("full synthetic day active=%t depth=%d minutes=1440 compressed_bytes=%d query_replay_p50=%s p95=%s", active, depth, bytes, latency[49], latency[94])
			})
		}
	}
}
