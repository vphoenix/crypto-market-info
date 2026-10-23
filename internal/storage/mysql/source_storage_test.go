package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestSourceStorageSchemaPreservesLogicalIdentity(t *testing.T) {
	for _, name := range sourceStorageNames {
		old, err := Mapping("crypto_market_info", name)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := Mapping("crypto_market_info", name+"_v2")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(old.Columns, fresh.Columns) || !reflect.DeepEqual(old.Key, fresh.Key) || old.Multiplicity != fresh.Multiplicity || !fresh.HashedKey {
			t.Fatal("logical identity changed", name)
		}
		if !strings.Contains(fresh.DDL, "PRIMARY KEY (`_storage_id`)") || !strings.Contains(fresh.DDL, "UNIQUE KEY `immutable_identity`") || !strings.Contains(fresh.DDL, "AUTO_INCREMENT") {
			t.Fatal("append identity layout missing", name)
		}
		if strings.Contains(old.DDL, "_storage_id") {
			t.Fatal("frozen migration mapping changed")
		}
	}
}

func TestMySQLSourceStorageMixedHistoryRetryAndRollback(t *testing.T) {
	if os.Getenv("CMI_MYSQL_TEST_DATABASE") != "" {
		t.Skip("extension test needs a private database")
	}
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	f := discoveryFixture(t, c)
	if err := c.WriteDiscoveryFact(ctx, f); err != nil {
		t.Fatal(err)
	}
	oldAt := time.Now().Add(time.Millisecond)
	_, oldProof, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, oldAt)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := InstallSourceStorageSchema(ctx, c.db, c.database); err != nil {
			t.Fatal(err)
		}
	}
	c.sourceStorageV2 = true
	if err := c.discoverSourceStorage(ctx); err != nil {
		t.Fatal(err)
	}
	c = c.WithProducer(c.producer)
	if !c.sourceStorageV2 || !c.sourceStorageAvailable {
		t.Fatal("per-job client lost source capabilities")
	}
	c.prefetchMinuteIndexes = true
	if err := c.WriteDiscoveryFact(ctx, f); err != nil {
		t.Fatal("legacy retry", err)
	}
	fresh := f
	fresh.ID = "zz-new-source"
	if err := c.WriteDiscoveryFact(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(time.Millisecond)
	got, proof, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, at)
	if err != nil || (model.DiscoveryHash(got.Body()) != model.DiscoveryHash(fresh.Body()) || !reflect.DeepEqual(got.Sources, fresh.Sources)) {
		t.Fatal("new/global latest read", got.ID, err)
	}
	reader, err := Open(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: c.database, Family: "crypto_market_info", ReadOnly: true, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, _, err = reader.LoadDiscoveryForecast(ctx, f.InstrumentID, at)
	if err != nil || (model.DiscoveryHash(got.Body()) != model.DiscoveryHash(fresh.Body()) || !reflect.DeepEqual(got.Sources, fresh.Sources)) {
		t.Fatal("automatic read capability", err)
	}
	_, oldAgain, err := reader.LoadDiscoveryForecast(ctx, f.InstrumentID, oldAt)
	if err != nil || oldAgain != oldProof {
		t.Fatal("as-of history changed", err)
	}
	// Switching the write target cannot duplicate a previously published row.
	c.sourceStorageV2 = false
	if err := c.WriteDiscoveryFact(ctx, fresh); err != nil {
		t.Fatal("rollback-mode retry", err)
	}
	_, retryProof, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, at)
	if err != nil || retryProof != proof {
		t.Fatal("first knowledge/publication moved", err)
	}
	for _, name := range sourceStorageNames {
		var old, current int
		if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)).Scan(&old); err != nil {
			t.Fatal(err)
		}
		if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name+"_v2")).Scan(&current); err != nil {
			t.Fatal(err)
		}
		want := 1
		if name == "public_observation_provenance" {
			want = 4
		}
		if old != want || current != want {
			t.Fatal("duplicate storage", name, old, current)
		}
	}
	c.sourceStorageV2 = true
	// A later provenance failure must roll back fact/status but retain intent.
	failing := f
	failing.ID = "rollback-source"
	trigger := "`" + c.database + "`.`fail_source_provenance`"
	if _, err := c.db.ExecContext(ctx, "CREATE TRIGGER "+trigger+" BEFORE INSERT ON "+c.table("public_observation_provenance_v2")+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='source rollback test'"); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryFact(ctx, failing); err == nil {
		t.Fatal("fault ignored")
	}
	intents, err := c.records(ctx, c.db, "source_publication_intents", "WHERE source_id=?", []byte(failing.ID))
	if err != nil || len(intents) != 1 {
		t.Fatal("durable intent missing", err)
	}
	for _, name := range []string{"public_observation_provenance", "source_batch_status"} {
		rows, err := c.records(ctx, c.db, name, "WHERE source_id=?", []byte(failing.ID))
		if err != nil || len(rows) != 0 {
			t.Fatal("partial publication", name, err)
		}
	}
	if _, err := c.db.ExecContext(ctx, "DROP TRIGGER "+trigger); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryFact(ctx, failing); err != nil {
		t.Fatal(err)
	}
	statuses, err := c.records(ctx, c.db, "source_batch_status", "WHERE source_id=?", []byte(failing.ID))
	if err != nil || len(statuses) != 1 || statuses[0]["known_from_ms"] != intents[0]["known_from_ms"] {
		t.Fatal("retry lost first knowledge", err)
	}
	// Existing provenance content is checked even when the new write switch is off.
	if _, err := c.db.ExecContext(ctx, "UPDATE "+c.table("public_observation_provenance_v2")+" SET payload_hash=? WHERE source_id=? AND source_order=0", strings.Repeat("0", 64), fresh.ID); err != nil {
		t.Fatal(err)
	}
	c.sourceStorageV2 = false
	if err := c.WriteDiscoveryFact(ctx, fresh); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("corrupt provenance accepted", err)
	}
	if _, _, err := reader.LoadDiscoveryForecast(ctx, f.InstrumentID, at); err == nil {
		t.Fatal("corrupt provenance read")
	}
	// Cross-format and same-format physical duplicates must remain ambiguous.
	row := statuses[0]
	if err := c.transaction(ctx, func(tx *sql.Tx) error {
		tab, _ := c.mapping("source_batch_status")
		return insertAbsentImmutableRows(ctx, tx, c.table(tab.Name), tab, []record{row})
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryFact(ctx, failing); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("cross-format duplicate accepted", err)
	}
	tab, _ := c.mapping("source_publication_intents_v2")
	cols := columns(tab) + ",`_key_hash`"
	if _, err := c.db.ExecContext(ctx, "INSERT INTO "+c.table(tab.Name)+" ("+cols+",`_occurrence`) SELECT "+cols+",1 FROM "+c.table(tab.Name)+" WHERE source_id=? AND _occurrence=0", failing.ID); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryFact(ctx, failing); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("duplicate occurrence accepted", err)
	}
}

func TestMySQLSourceStorageCEXDelivery(t *testing.T) {
	if os.Getenv("CMI_MYSQL_TEST_DATABASE") != "" {
		t.Skip("extension test needs a private database")
	}
	c := integrationClient(t)
	ctx := context.Background()
	if err := InstallSourceStorageSchema(ctx, c.db, c.database); err != nil {
		t.Fatal(err)
	}
	c.sourceStorageV2 = true
	if err := c.discoverSourceStorage(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.InitMinuteSchema(ctx); err != nil {
		t.Fatal(err)
	}
	c.prefetchMinuteIndexes = true
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for n, depth := range []int{5, 10, 50} {
		b := testBatch(t, uint32(n+1), at, min(depth, 10))
		if depth == 50 {
			b = wideCEXMinute(t).Batches[n]
		}
		all := model.CompletedMinute{MinuteTime: b.Minute.MinuteTime, KnownFromMS: b.Minute.MinuteTime.Add(65 * time.Second).UnixMilli(), Batches: []model.MinuteBatch{b}}
		deliveries, err := c.PrepareBookDeliveries(ctx, all)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range deliveries {
			job := c.WithProducer(c.producer)
			prepared, err := job.PrepareDelivery(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			if err := job.CommitPrepared(ctx, prepared); err != nil {
				prepared.Close()
				t.Fatal(err)
			}
			prepared.Close()
			job.sourceStorageV2 = false
			if err := job.Commit(ctx, d); err != nil {
				t.Fatal("delivery retry", err)
			}
		}
		for sec := uint8(0); sec < 60; sec++ {
			want, valid, err := replay.AtSecond(b.Minute, b.Deltas, sec)
			if err != nil {
				t.Fatal(err)
			}
			got, actual, err := c.ReplayBook(ctx, b.Minute.InstrumentID, b.Minute.MinuteTime.Add(time.Duration(sec)*time.Second))
			want.SourceTime = got.SourceTime
			if err != nil || valid != actual || !reflect.DeepEqual(want, got) {
				t.Fatal("replay", depth, sec, err)
			}
		}
	}
	var n int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("source_batch_status_v2")).Scan(&n); err != nil || n != 3 {
		t.Fatal("new publications", n, err)
	}
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("source_batch_status")).Scan(&n); err != nil || n != 0 {
		t.Fatal("legacy publication duplicated", n, err)
	}
}
