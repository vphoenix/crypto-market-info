package mysql

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func TestBookOutageDurableRecoveryPreservesKnowledge(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if e := c.InitDiscoverySchema(ctx); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"options_live", "options_catalog"} {
		// An options scope still requires its minute proof if its last frame
		// contains no derivative members.
		ok, err := c.PreviewCommitted(ctx, contracts.PreviewFrame{Kind: kind, Scope: uuid.NewString(), At: time.Now().UTC()})
		if err != nil || ok {
			t.Fatal("empty options preview bypassed minute proof", kind, ok, err)
		}
	}
	disk, e := spool.Open(t.TempDir(), 32<<20)
	if e != nil {
		t.Fatal(e)
	}
	queues := []contracts.DurableQueue{redisbuffer.Queue{Spool: disk}}
	var redis *redisbuffer.Buffer
	if os.Getenv("REDIS_INTEGRATION") == "1" {
		redis, e = redisbuffer.Open(ctx, redisbuffer.Config{Socket: os.Getenv("CMI_REDIS_TEST_SOCKET"), Prefix: "cmi:test:" + uuid.NewString(), WaitTimeout: 2 * time.Second})
		if e != nil {
			t.Fatal(e)
		}
		defer redis.Close()
		queues = append(queues, redis)
	}
	if e = c.db.Close(); e != nil {
		t.Fatal(e)
	}
	var deliveries []contracts.Delivery
	var batches []model.MinuteBatch
	var knownTimes []int64
	for i, queue := range queues {
		at := time.Date(2026, 10, 1, 0, i, 0, 0, time.UTC)
		known := at.Add(65 * time.Second).UnixMilli()
		batch := testBatch(t, uint32(i+1), at, 5)
		completed := model.CompletedMinute{MinuteTime: at, KnownFromMS: known, Batches: []model.MinuteBatch{batch}}
		c.SetDeliveryQueue(queue, nil)
		if e = c.WriteCompletedMinute(ctx, completed); e != nil {
			t.Fatal("queue blocked by MySQL outage", e)
		}
		var d contracts.Delivery
		var ok bool
		if i == 0 {
			// Reopen the spool to simulate a process restart, not an in-memory retry.
			reopened, err := spool.Open(disk.Directory, 32<<20)
			if err != nil {
				t.Fatal(err)
			}
			d, ok, e = reopened.Next(ctx)
		} else {
			d, ok, e = redis.Next(ctx, c.database)
		}
		if e != nil || !ok {
			t.Fatal("durable delivery missing", e)
		}
		decoded, err := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
		if err != nil || decoded.KnownFromMS != known {
			t.Fatal("first knowledge lost", err)
		}
		prepared, err := c.PrepareBookDeliveries(ctx, completed)
		if err != nil || prepared[0].ID != d.ID || !bytes.Equal(prepared[0].Payload, d.Payload) {
			t.Fatal("retry identity changed", err)
		}
		deliveries = append(deliveries, d)
		batches = append(batches, batch)
		knownTimes = append(knownTimes, known)
	}
	c.db, e = Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: c.database, MaxConnections: 2})
	if e != nil {
		t.Fatal(e)
	}
	// A fresh writer must honor v2 publication metadata even before schema checks.
	c.discoveryEnabled.Store(false)
	for i, d := range deliveries {
		for range 2 {
			if e = c.Commit(ctx, d); e != nil {
				t.Fatal("recovery commit", e)
			}
		}
		rows, err := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND source_id=?", []byte("cex_book_minute_commit"), []byte(strconv.FormatUint(batches[i].Minute.ID, 10)))
		if err != nil || len(rows) != 1 || rows[0]["known_from_ms"] != knownTimes[i] {
			t.Fatal("recovery changed first knowledge", rows, err)
		}
		batch := batches[i]
		for sec := uint8(0); sec < 60; sec++ {
			want, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, sec)
			if err != nil {
				t.Fatal(err)
			}
			at := batch.Minute.MinuteTime.Add(time.Duration(sec) * time.Second)
			got, gotValid, err := c.ReplayBook(ctx, batch.Minute.InstrumentID, at)
			want.SourceTime = got.SourceTime
			if err != nil || valid != gotValid || !reflect.DeepEqual(want, got) {
				t.Fatalf("second %d recovery mismatch: %v", sec, err)
			}
		}
		if i == 0 {
			if e = disk.Ack(ctx, d, c.Committed); e != nil {
				t.Fatal(e)
			}
		} else {
			if e = redis.Ack(ctx, d, c.Committed); e != nil {
				t.Fatal(e)
			}
			if e = redis.Cleanup(ctx, c.database, time.Now().UTC(), 100); e != nil {
				t.Fatal(e)
			}
		}
	}
}

func TestHistoricalPublicationFailureBlocksFinalizeAndResumes(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	run := uuid.New()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var facts []MigrationRow
	for _, id := range []uint32{1, 2} {
		b := testBatch(t, id, at, 5)
		b.Deltas = nil
		facts = append(facts, MigrationRow(minuteRecord(b.Minute)))
	}
	if _, e := migrateRows(t, c, "order_book_minute", facts, run); e != nil {
		t.Fatal(e)
	}
	var tables []Table
	for _, name := range []string{"order_book_minute", "order_book_second_delta"} {
		tab, _ := c.mapping(name)
		if name == "order_book_second_delta" {
			if e := PlanMigrationTable(ctx, c.db, tab, c.database, run, nil); e != nil {
				t.Fatal(e)
			}
		}
		if e := MarkMigrationTableCopied(ctx, c.db, tab, c.database, run); e != nil {
			t.Fatal(e)
		}
		if e := MarkMigrationTableVerified(ctx, c.db, tab, c.database, run); e != nil {
			t.Fatal(e)
		}
		// Isolated catalog for the same finalizer used by the public all-table API.
		tab.Database = c.database
		tables = append(tables, tab)
	}
	if _, e := c.db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET source_database=? WHERE run_id=?", c.database, run[:]); e != nil {
		t.Fatal(e)
	}
	publisher := MigrationPublisher(c.db, c.database, c.family)
	// Fail on the second minute after the first has already become visible.
	if _, e := c.db.ExecContext(ctx, "UPDATE "+c.table("order_book_minute")+" SET stored_depth=3 WHERE instrument_id=2"); e != nil {
		t.Fatal(e)
	}
	if e := publisher.CompleteMigratedBooks(ctx, run); e == nil {
		t.Fatal("invalid second minute accepted")
	}
	if _, e := c.LoadMinute(ctx, 1, at); e != nil {
		t.Fatal("first minute did not commit before failure", e)
	}
	if e := finalizeMigrationTables(ctx, c.db, run, tables); e == nil {
		t.Fatal("partial publication finalized")
	}
	// A legacy premature state=2 must also be rejected by coverage validation.
	if _, e := c.db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET state=2 WHERE run_id=?", run[:]); e != nil {
		t.Fatal(e)
	}
	if e := finalizeMigrationTables(ctx, c.db, run, tables); !errors.Is(e, contracts.ErrHistoryNotReady) {
		t.Fatal("legacy state bypassed coverage", e)
	}
	if _, e := c.db.ExecContext(ctx, "UPDATE "+c.table("order_book_minute")+" SET stored_depth=5 WHERE instrument_id=2"); e != nil {
		t.Fatal(e)
	}
	if e := publisher.CompleteMigratedBooks(ctx, run); e != nil {
		t.Fatal("resume", e)
	}
	if e := publisher.CompleteMigratedBooks(ctx, run); e != nil {
		t.Fatal("repeat resume", e)
	}
	if e := finalizeMigrationTables(ctx, c.db, run, tables); e != nil {
		t.Fatal("complete publication cannot finalize", e)
	}
	for _, id := range []uint32{1, 2} {
		if _, e := c.LoadMinute(ctx, id, at); e != nil {
			t.Fatal(e)
		}
	}
	t.Cleanup(func() {
		c.db.ExecContext(ctx, "DELETE FROM cmi_storage.history_coverage WHERE database_name=?", c.database)
	})
}
