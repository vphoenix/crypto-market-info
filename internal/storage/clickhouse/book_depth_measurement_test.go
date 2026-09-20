package clickhouse

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

// Opt-in only. The source connection is read-only; all writes, merges and
// cleanup use unique integration-test databases created by this test.
func TestClickHouseBookDepthRealHistoryMeasurement(t *testing.T) {
	database := os.Getenv("BOOK_DEPTH_MEASURE_DATABASE")
	ids := os.Getenv("BOOK_DEPTH_MEASURE_INSTRUMENTS")
	if database == "" || ids == "" || os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1, BOOK_DEPTH_MEASURE_DATABASE and BOOK_DEPTH_MEASURE_INSTRUMENTS for a read-only historical comparison")
	}
	if !identifierPattern.MatchString(database) {
		t.Fatal("invalid source database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	conn, err := ch.Open(&ch.Options{Addr: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Auth: ch.Auth{Database: database, Username: "default"},
		Settings: ch.Settings{"readonly": 1, "max_threads": 2, "max_execution_time": 60}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	source := &Client{conn: conn, database: database}
	var hasDepth uint64
	if err = conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database=? AND table='order_book_minute' AND name='stored_depth'", database).Scan(&hasDepth); err != nil {
		t.Fatal(err)
	}
	columns := MinuteColumns()
	if hasDepth == 0 {
		columns[4] = "toUInt8(50) AS stored_depth"
	}
	var through time.Time
	if err = conn.QueryRow(ctx, "SELECT max(minute_time) FROM "+source.table("order_book_minute")).Scan(&through); err != nil {
		t.Fatal(err)
	}
	through = through.UTC().Add(time.Minute)
	since := through.Add(-24 * time.Hour)
	for _, value := range strings.Split(ids, ",") {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil || parsed == 0 {
			t.Fatalf("invalid instrument ID %q", value)
		}
		id := uint32(parsed)
		t.Run(value, func(t *testing.T) {
			var exchange, symbol string
			if err := conn.QueryRow(ctx, "SELECT exchange,exchange_symbol FROM "+source.table("instrument")+" FINAL WHERE instrument_id=?", id).Scan(&exchange, &symbol); err != nil {
				t.Fatal(err)
			}
			rows, err := conn.Query(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+source.table("order_book_minute")+" FINAL WHERE instrument_id=? AND minute_time>=? AND minute_time<? ORDER BY minute_time", id, since, through)
			if err != nil {
				t.Fatal(err)
			}
			var minutes []model.MinuteBook
			for rows.Next() {
				var minute model.MinuteBook
				dest := []any{&minute.ID, &minute.InstrumentID, &minute.MinuteTime, &minute.ValidBitmap, &minute.StoredDepth}
				for level := range model.LegacyBookDepth {
					dest = append(dest, &minute.Bids[level].PriceTick, &minute.Bids[level].QtyLot)
				}
				for level := range model.LegacyBookDepth {
					dest = append(dest, &minute.Asks[level].PriceTick, &minute.Asks[level].QtyLot)
				}
				if err = rows.Scan(dest...); err != nil {
					t.Fatal(err)
				}
				minute.MinuteTime = minute.MinuteTime.UTC()
				minutes = append(minutes, minute)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if len(minutes) < 1200 {
				t.Fatalf("only %d minutes in 24h; select a better-covered source", len(minutes))
			}
			// Read each source range once. Per-minute source queries repeatedly
			// decompress wide parts and unnecessarily load the running database.
			// instrument_id is encoded in the primary key, so PREWHERE is safe
			// across FINAL versions and avoids reading other instruments' arrays.
			rows, err = conn.Query(ctx, "SELECT minute_id,second_offset,bid_change_prices,bid_change_qtys,ask_change_prices,ask_change_qtys FROM "+source.table("order_book_second_delta")+" FINAL PREWHERE minute_id>=? AND minute_id<? AND toUInt32(minute_id)=? ORDER BY minute_id,second_offset", uint64(since.Unix()/60)<<32, uint64(through.Unix()/60)<<32, id)
			if err != nil {
				t.Fatal(err)
			}
			deltasByMinute := make(map[uint64][]model.BookDelta, len(minutes))
			for rows.Next() {
				var delta model.BookDelta
				if err = rows.Scan(&delta.MinuteID, &delta.SecondOffset, &delta.BidChangePrice, &delta.BidChangeQty, &delta.AskChangePrice, &delta.AskChangeQty); err != nil {
					t.Fatal(err)
				}
				deltasByMinute[delta.MinuteID] = append(deltasByMinute[delta.MinuteID], delta)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			full, reduced := commonUniverseTestClient(t), commonUniverseTestClient(t)
			var fullMinutes, reducedMinutes []model.MinuteBook
			var fullDeltas, reducedDeltas []model.BookDelta
			var validSeconds uint64
			for _, minute := range minutes {
				at := minute.MinuteTime
				if err := minute.ValidateDepth(); err != nil || minute.StoredDepth != 50 {
					t.Fatalf("expected a legacy 50-level minute: %v", err)
				}
				deltas := deltasByMinute[minute.ID]
				buffer, err := sampler.NewMinuteBuffer(id, at)
				if err != nil {
					t.Fatal(err)
				}
				var expected [60]model.BookSnapshot
				for second := range 60 {
					book, valid, err := replay.AtSecond(minute, deltas, uint8(second))
					if err != nil {
						t.Fatal(err)
					}
					if valid {
						validSeconds++
						book.Bids = book.Bids[:min(len(book.Bids), model.BookDepth)]
						book.Asks = book.Asks[:min(len(book.Asks), model.BookDepth)]
						book.SourceTime = at.Add(time.Duration(second) * time.Second)
						expected[second] = book
					}
					if err = buffer.Sample(at.Add(time.Duration(second)*time.Second), book, valid); err != nil {
						t.Fatal(err)
					}
				}
				batch, ok := buffer.Batch()
				if !ok || batch.Minute.ValidBitmap != minute.ValidBitmap {
					t.Fatal("re-encoding changed valid seconds")
				}
				for second, want := range expected {
					got, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, uint8(second))
					if err != nil || valid != (want.InstrumentID != 0) || (valid && (!reflect.DeepEqual(got.Bids, want.Bids) || !reflect.DeepEqual(got.Asks, want.Asks))) {
						t.Fatalf("re-encoded %s second %d differs: %v", at, second, err)
					}
				}
				if err = validateBatch(batch); err != nil {
					t.Fatal(err)
				}
				fullMinutes, reducedMinutes = append(fullMinutes, minute), append(reducedMinutes, batch.Minute)
				fullDeltas, reducedDeltas = append(fullDeltas, deltas...), append(reducedDeltas, batch.Deltas...)
			}
			for _, variant := range []struct {
				depth   int
				client  *Client
				minutes []model.MinuteBook
				deltas  []model.BookDelta
			}{{50, full, fullMinutes, fullDeltas}, {10, reduced, reducedMinutes, reducedDeltas}} {
				if len(variant.deltas) > 0 {
					if err = variant.client.insertDeltas(ctx, variant.deltas); err != nil {
						t.Fatal(err)
					}
				}
				if err = variant.client.insertMinutes(ctx, variant.minutes); err != nil {
					t.Fatal(err)
				}
				for _, table := range []string{"order_book_minute", "order_book_second_delta"} {
					if err = variant.client.conn.Exec(ctx, "OPTIMIZE TABLE "+variant.client.table(table)+" FINAL"); err != nil {
						t.Fatal(err)
					}
				}
				var compressed, disk uint64
				if err = variant.client.conn.QueryRow(ctx, "SELECT sum(data_compressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE active AND database=?", variant.client.database).Scan(&compressed, &disk); err != nil {
					t.Fatal(err)
				}
				started := time.Now()
				for index := range 100 {
					// Deterministic samples, always valid anchored seconds.
					minute := variant.minutes[index*len(variant.minutes)/100]
					second := uint8((index * 17) % 60)
					if minute.ValidBitmap&(uint64(1)<<second) == 0 {
						second = 0
					}
					got, valid, err := variant.client.ReplayBook(ctx, id, minute.MinuteTime.Add(time.Duration(second)*time.Second))
					if err != nil || !valid || int(got.StoredDepth) != variant.depth {
						t.Fatalf("stored replay failed: %v", err)
					}
				}
				t.Logf("%s %s id=%d window=[%s,%s) depth=%d minutes=%d valid_seconds=%d delta_rows=%d compressed_bytes=%d disk_bytes=%d 100_replays=%s", exchange, symbol, id, since.Format(time.RFC3339), through.Format(time.RFC3339), variant.depth, len(minutes), validSeconds, len(variant.deltas), compressed, disk, time.Since(started))
			}
		})
	}
}
