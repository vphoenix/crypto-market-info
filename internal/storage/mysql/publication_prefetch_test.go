package mysql

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestCEXPrefetchDoesNotFenceGenerationAndRechecksPublication(t *testing.T) {
	c := integrationClient(t)
	c.prefetchMinuteIndexes = true
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{testBatch(t, 1, at, 5)}})
	if err != nil {
		t.Fatal(err)
	}
	// Use the fixture's checked pool for the lock and an independent connection
	// for the generation probe; preparation must not acquire that fence first.
	blocker, err := c.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err = blocker.ExecContext(ctx, "LOCK TABLES "+c.table("source_batch_status")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")
	probeDB, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer probeDB.Close()
	type result struct {
		p   *PreparedDelivery
		err error
	}
	done := make(chan result, 1)
	go func() { p, e := c.PrepareDelivery(ctx, ds[0]); done <- result{p, e} }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		if err = probeDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=? AND STATE='Waiting for table metadata lock' AND INFO LIKE '%source_batch_status%'", c.database).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("preparation did not reach the blocked index prefetch")
		}
		time.Sleep(20 * time.Millisecond)
	}
	tx, err := probeDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = tx.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	tx.Rollback()
	if err != nil || generation != c.producer.Generation {
		t.Fatal("index preparation pinned the producer fence", generation, err)
	}
	if _, err = blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.p.Close()
	p := r.p.cexPublications[0]
	status, err := namedRecord(statusColumns, []any{p.Kind, p.ID, p.Hash, p.SourceMS, p.ObservedMS, p.KnownMS, p.KnownMS, uint8(0), p.Codec, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
	if err != nil {
		t.Fatal(err)
	}
	mapping, _ := c.mapping("source_batch_status")
	if err = insertRecord(ctx, c.db, c.table(mapping.Name), mapping, status); err != nil {
		t.Fatal(err)
	}
	assertRejected := func(reason string) {
		t.Helper()
		if err := c.CommitPrepared(ctx, r.p); !errors.Is(err, contracts.ErrConflict) {
			t.Fatal(reason, err)
		}
		if committed, err := c.Committed(ctx, ds[0]); err != nil || committed {
			t.Fatal("rejected publication left a durable receipt", committed, err)
		}
		for _, name := range []string{"order_book_minute", "order_book_second_delta", "source_publication_intents"} {
			var count int
			if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)).Scan(&count); err != nil || count != 0 {
				t.Fatal("rejected publication left partial facts", name, count, err)
			}
		}
	}
	assertRejected("incomplete publication added after preparation was accepted")
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table(mapping.Name)+" SET complete=1"); err != nil {
		t.Fatal(err)
	}
	cols := make([]string, 0, len(mapping.Columns)+1)
	for _, col := range mapping.Columns {
		cols = append(cols, "`"+col.Name+"`")
	}
	cols = append(cols, "`_key_hash`")
	columns := strings.Join(cols, ",")
	if _, err = c.db.ExecContext(ctx, "INSERT INTO "+c.table(mapping.Name)+" ("+columns+",`_occurrence`) SELECT "+columns+",1 FROM "+c.table(mapping.Name)); err != nil {
		t.Fatal(err)
	}
	assertRejected("physical duplicate added after preparation was accepted")
	if _, err = c.db.ExecContext(ctx, "DELETE FROM "+c.table(mapping.Name)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = c.CommitPrepared(ctx, r.p); err != nil {
			t.Fatal("unchanged original delivery could not retry", err)
		}
	}
}

func TestDiscoveryPrefetchPreservesDurableIntentWithoutGenerationFence(t *testing.T) {
	c := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	fact := discoveryFixture(t, c)
	admin, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	blocker, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err = blocker.ExecContext(ctx, "LOCK TABLES "+c.table("public_observation_provenance")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")
	done := make(chan error, 1)
	go func() { done <- c.WriteDiscoveryFact(ctx, fact) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		err = admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=? AND STATE='Waiting for table metadata lock' AND INFO LIKE '%public_observation_provenance%'", c.database).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("discovery did not reach blocked preparation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	probe, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = probe.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	probe.Rollback()
	if err != nil || generation != c.producer.Generation {
		t.Fatal("discovery preparation pinned the exclusive fence", generation, err)
	}
	var known int64
	if err = admin.QueryRowContext(ctx, "SELECT known_from_ms FROM "+c.table("source_publication_intents")+" WHERE source_kind=? AND source_id=?", []byte(fact.Kind()), []byte(fact.ID)).Scan(&known); err != nil {
		t.Fatal("first knowledge was not durable before preparation", err)
	}
	var statuses int
	if err = admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("source_batch_status")).Scan(&statuses); err != nil || statuses != 0 {
		t.Fatal("publication preceded complete facts", statuses, err)
	}
	if _, err = blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = c.WriteDiscoveryFact(ctx, fact); err != nil {
		t.Fatal(err)
	}
	var publishedKnown int64
	if err = admin.QueryRowContext(ctx, "SELECT known_from_ms FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=?", []byte(fact.Kind()), []byte(fact.ID)).Scan(&publishedKnown); err != nil || publishedKnown != known {
		t.Fatal("retry moved first knowledge", known, publishedKnown, err)
	}
}
