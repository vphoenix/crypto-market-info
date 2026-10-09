package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"strings"
	"sync"
	"testing"
	"time"
)

// Keep real ClickHouse persistence while injecting a durable prefix followed
// by a lost acknowledgement. Only the first selected metadata INSERT fails.
type prefixFailureConn struct {
	driver.Conn
	table string
	fired bool
}

func (c *prefixFailureConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	b, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil || c.fired || !strings.Contains(query, ".`"+c.table+"` ") {
		return b, err
	}
	c.fired = true
	return &prefixFailureBatch{Batch: b}, nil
}

type prefixFailureBatch struct {
	driver.Batch
	appended int
}

func (b *prefixFailureBatch) Append(values ...any) error {
	b.appended++
	if b.appended <= 50 {
		return b.Batch.Append(values...)
	}
	return nil
}

func (b *prefixFailureBatch) Send() error {
	if err := b.Batch.Send(); err != nil {
		return err
	}
	return fmt.Errorf("50-row prefix durable; acknowledgement lost")
}

func TestDiscoveryMinuteBatchDurablePrefixRetry(t *testing.T) {
	for _, table := range []string{"source_publication_intents", "cex_book_minute_commit", "source_batch_status"} {
		t.Run(table, func(t *testing.T) {
			c := commonUniverseTestClient(t)
			ctx := context.Background()
			if err := c.InitOKXPairSchema(ctx); err != nil {
				t.Fatal(err)
			}
			c.maxAttempts = 1
			fault := &prefixFailureConn{Conn: c.conn, table: table}
			c.conn = fault
			minute := publicationMinuteFixture(t, 101, time.Now().UTC().Truncate(time.Minute).Add(-3*time.Minute), true)
			if err := c.WriteCompletedMinute(ctx, minute); err == nil || !fault.fired {
				t.Fatal("prefix fault did not interrupt", err)
			}
			var prefixCount uint64
			if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table(table)).Scan(&prefixCount); err != nil || prefixCount != 50 {
				t.Fatal("durable prefix", prefixCount, err)
			}
			if table != "source_batch_status" {
				var complete uint64
				if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("source_batch_status")).Scan(&complete); err != nil || complete != 0 {
					t.Fatal("partial dependencies published", complete, err)
				}
			}
			type clock struct{ known, published int64 }
			clocks := map[string]clock{}
			rows, err := c.conn.Query(ctx, "SELECT source_id,known_from_ms FROM "+c.table("source_publication_intents"))
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				var known int64
				if err := rows.Scan(&id, &known); err != nil {
					t.Fatal(err)
				}
				clocks[id] = clock{known: known}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			rows, err = c.conn.Query(ctx, "SELECT source_id,published_ms FROM "+c.table("source_batch_status"))
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				var published int64
				if err := rows.Scan(&id, &published); err != nil {
					t.Fatal(err)
				}
				v := clocks[id]
				v.published = published
				clocks[id] = v
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			time.Sleep(3 * time.Millisecond)
			if err := c.WriteCompletedMinute(ctx, minute); err != nil {
				t.Fatal(err)
			}
			minuteMetadataCounts(t, c, 101)
			rows, err = c.conn.Query(ctx, "SELECT source_id,known_from_ms,published_ms FROM "+c.table("source_batch_status"))
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				var known, published int64
				if err := rows.Scan(&id, &known, &published); err != nil {
					t.Fatal(err)
				}
				if original, ok := clocks[id]; ok && (known != original.known || (original.published != 0 && published != original.published)) {
					t.Fatal("durable first clock changed", id, original, known, published)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if err := c.WriteCompletedMinute(ctx, minute); err != nil {
				t.Fatal(err)
			}
			minuteMetadataCounts(t, c, 101)
		})
	}
}

func publicationMinuteFixture(t *testing.T, n int, at time.Time, changes bool) model.CompletedMinute {
	t.Helper()
	out := model.CompletedMinute{MinuteTime: at}
	for index := 1; index <= n; index++ {
		id := uint32(index)
		mid, err := model.MinuteID(id, at)
		if err != nil {
			t.Fatal(err)
		}
		m := model.MinuteBook{ID: mid, InstrumentID: id, MinuteTime: at, StoredDepth: 5, ValidBitmap: (uint64(1) << 60) - 1}
		for d := 0; d < 5; d++ {
			m.Bids[d] = model.Level{PriceTick: int64(100 - d), QtyLot: 100}
			m.Asks[d] = model.Level{PriceTick: int64(101 + d), QtyLot: 100}
		}
		b := model.MinuteBatch{Minute: m}
		if changes {
			for second := uint8(1); second < 60; second++ {
				delta := model.BookDelta{MinuteID: mid, SecondOffset: second}
				for d := 0; d < 5; d++ {
					delta.BidChangePrice = append(delta.BidChangePrice, int64(100-d))
					delta.AskChangePrice = append(delta.AskChangePrice, int64(101+d))
					delta.BidChangeQty = append(delta.BidChangeQty, uint64(100+second))
					delta.AskChangeQty = append(delta.AskChangeQty, uint64(100+second))
				}
				b.Deltas = append(b.Deltas, delta)
			}
		}
		out.Batches = append(out.Batches, b)
	}
	return out
}

func minuteMetadataCounts(t *testing.T, c *Client, want uint64) {
	t.Helper()
	for _, name := range []string{"source_publication_intents", "cex_book_minute_commit", "source_batch_status"} {
		var count, unique uint64
		where := ""
		if name != "cex_book_minute_commit" {
			where = " WHERE source_kind='cex_book_minute_commit'"
		}
		if err := c.conn.QueryRow(context.Background(), "SELECT count(),uniqExact(source_id) FROM "+c.table(name)+where).Scan(&count, &unique); err != nil {
			t.Fatal(err)
		}
		if count != want || unique != want {
			t.Fatalf("%s count=%d unique=%d want=%d", name, count, unique, want)
		}
	}
}

func TestDiscoveryMinuteBatchDurabilityAndRetry(t *testing.T) {
	for _, table := range []string{"source_publication_intents", "cex_book_minute_commit", "source_batch_status"} {
		t.Run(table, func(t *testing.T) {
			c := commonUniverseTestClient(t)
			ctx := context.Background()
			if err := c.InitOKXPairSchema(ctx); err != nil {
				t.Fatal(err)
			}
			c.maxAttempts = 1
			minute := publicationMinuteFixture(t, 101, time.Now().UTC().Truncate(time.Minute).Add(-3*time.Minute), true)
			failed := false
			c.derivativeAfterInsert = func(name string) error {
				if name == table && !failed {
					failed = true
					return fmt.Errorf("durable insert ACK lost")
				}
				return nil
			}
			if err := c.WriteCompletedMinute(ctx, minute); err == nil || !failed {
				t.Fatal("fault did not interrupt")
			}
			if table != "source_batch_status" {
				var count uint64
				if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("source_batch_status")).Scan(&count); err != nil || count != 0 {
					t.Fatal("incomplete data published", count, err)
				}
			}
			var known int64
			if err := c.conn.QueryRow(ctx, "SELECT min(known_from_ms) FROM "+c.table("source_publication_intents")).Scan(&known); err != nil {
				t.Fatal(err)
			}
			var published int64
			if table == "source_batch_status" {
				if err := c.conn.QueryRow(ctx, "SELECT min(published_ms) FROM "+c.table("source_batch_status")).Scan(&published); err != nil {
					t.Fatal(err)
				}
			}
			c.derivativeAfterInsert = nil
			if err := c.WriteCompletedMinute(ctx, minute); err != nil {
				t.Fatal(err)
			}
			minuteMetadataCounts(t, c, 101)
			var retained int64
			if err := c.conn.QueryRow(ctx, "SELECT min(known_from_ms) FROM "+c.table("source_publication_intents")).Scan(&retained); err != nil || retained != known {
				t.Fatal("first knowledge renewed", retained, known, err)
			}
			if table == "source_batch_status" {
				if err := c.conn.QueryRow(ctx, "SELECT min(published_ms) FROM "+c.table("source_batch_status")).Scan(&retained); err != nil || retained != published {
					t.Fatal("first publication renewed", err)
				}
			}
			if err := c.WriteCompletedMinute(ctx, minute); err != nil {
				t.Fatal(err)
			}
			minuteMetadataCounts(t, c, 101)
			changed := minute
			changed.Batches = append([]model.MinuteBatch(nil), minute.Batches...)
			changed.Batches[100].Minute.Bids[0].QtyLot++
			if err := c.WriteCompletedMinute(ctx, changed); err == nil {
				t.Fatal("changed batch reused identity")
			}
			minuteMetadataCounts(t, c, 101)
		})
	}
}

func TestDiscoveryMinuteBatchPublicationFollowsCommit(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	var finished int64
	c.derivativeAfterInsert = func(name string) error {
		if name == "cex_book_minute_commit" {
			time.Sleep(10 * time.Millisecond)
			finished = model.CeilMilliseconds(time.Now().UTC())
		}
		return nil
	}
	if err := c.WriteCompletedMinute(ctx, publicationMinuteFixture(t, 4, time.Now().UTC().Truncate(time.Minute).Add(-time.Minute), true)); err != nil {
		t.Fatal(err)
	}
	var published int64
	if err := c.conn.QueryRow(ctx, "SELECT min(published_ms) FROM "+c.table("source_batch_status")).Scan(&published); err != nil || published < finished {
		t.Fatal("publication precedes durable commit completion", published, finished, err)
	}
}

func TestDiscoveryMinuteBatchRejectsCorruptCommit(t *testing.T) {
	for _, assignment := range []string{"stored_depth=10", "delta_seconds=[1]"} {
		t.Run(assignment, func(t *testing.T) {
			c := commonUniverseTestClient(t)
			ctx := context.Background()
			if err := c.InitOKXPairSchema(ctx); err != nil {
				t.Fatal(err)
			}
			minute := publicationMinuteFixture(t, 4, time.Now().UTC().Truncate(time.Minute).Add(-time.Minute), true)
			if err := c.WriteCompletedMinute(ctx, minute); err != nil {
				t.Fatal(err)
			}
			if err := c.conn.Exec(ctx, "ALTER TABLE "+c.table("cex_book_minute_commit")+" UPDATE "+assignment+" WHERE 1 SETTINGS mutations_sync=2"); err != nil {
				t.Fatal(err)
			}
			if err := c.WriteCompletedMinute(ctx, minute); err == nil {
				t.Fatal("corrupt commit accepted")
			}
		})
	}
}

func TestDiscoveryMinuteBatch334WithinSamplerDeadline(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	start := time.Now()
	if err := c.WriteCompletedMinute(ctx, publicationMinuteFixture(t, 334, at, true)); err != nil {
		t.Fatal(err)
	}
	t.Logf("without discovery: streams=334 depth=5 seconds=60 delta_rows=19706 elapsed=%v", time.Since(start))
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	start = time.Now()
	if err := c.WriteCompletedMinute(bounded, publicationMinuteFixture(t, 334, at.Add(time.Minute), true)); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("with batched discovery: streams=334 depth=5 seconds=60 delta_rows=19706 elapsed=%v deadline=45s", elapsed)
	if elapsed >= 15*time.Second {
		t.Fatal("insufficient headroom under 45s deadline", elapsed)
	}
	minuteMetadataCounts(t, c, 334)
	var wait sync.WaitGroup
	errs := make(chan error, 3)
	start = time.Now()
	for n := 0; n < 3; n++ {
		minute := publicationMinuteFixture(t, 334, at.Add(time.Duration(n+2)*time.Minute), true)
		wait.Add(1)
		go func() { defer wait.Done(); errs <- c.WriteCompletedMinute(bounded, minute) }()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	elapsed = time.Since(start)
	t.Logf("concurrent batched discovery: groups=3 streams=1002 depth=5 seconds=60 delta_rows=59118 elapsed=%v deadline=45s", elapsed)
	if elapsed >= 30*time.Second {
		t.Fatal("concurrent load lacks deadline headroom", elapsed)
	}
	minuteMetadataCounts(t, c, 1336)
}
