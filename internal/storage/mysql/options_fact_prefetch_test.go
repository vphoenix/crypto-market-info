package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestOptionsFactPrefetchDoesNotFenceGenerationAndRechecksFacts(t *testing.T) {
	c := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	run, envelope := catalogDBFixture(t, c)
	raw, err := codec.EncodeOptionsCatalog(envelope)
	if err != nil {
		t.Fatal(err)
	}
	d := c.Delivery("options_catalog", run.ID.String(), envelope.Live.MinuteTime, uint32(len(envelope.Live.Books)), raw)
	preparer, err := Open(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: c.database, Family: "crypto_market_info", Producer: c.producer, MaxConnections: 1, ReadOnly: true, PrefetchMinuteIndexes: true})
	if err != nil {
		t.Fatal(err)
	}
	defer preparer.Close()
	blocker, err := c.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err = blocker.ExecContext(ctx, "LOCK TABLES "+c.table("derivative_book_quality_minute")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")
	probe, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	type result struct {
		p   *PreparedDelivery
		err error
	}
	done := make(chan result, 1)
	go func() { p, e := preparer.PrepareDelivery(ctx, d); done <- result{p, e} }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err = probe.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=? AND STATE='Waiting for table metadata lock' AND INFO LIKE '%derivative_book_quality_minute%'", c.database).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("preparation did not reach the blocked fact prefetch")
		}
		time.Sleep(20 * time.Millisecond)
	}
	tx, err := probe.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = tx.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	tx.Rollback()
	if err != nil || generation != c.producer.Generation {
		t.Fatal("fact preparation pinned the generation fence", generation, err)
	}
	if _, err = blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	// Release the checked-pool slot before committing through that same pool.
	blocker.Close()
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.p.Close()
	// Facts arrive after the prefetch saw no rows. They still require complete
	// transactional comparison; the prefetch must never cache an absence proof.
	if err = c.transaction(ctx, func(tx *sql.Tx) error {
		return c.insertDerivativeBooks(ctx, tx, envelope.ID(), envelope.Live.Books)
	}); err != nil {
		t.Fatal(err)
	}
	key := []any{envelope.Live.Books[0].InstrumentID, envelope.Live.MinuteTime, digestBytes(envelope.ID())}
	where := " WHERE instrument_id=? AND minute_time=? AND compact_batch_id=?"
	var original []byte
	if err = c.db.QueryRowContext(ctx, "SELECT compact_row_hash FROM "+c.table("derivative_book_quality_minute")+where, key...).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("derivative_book_quality_minute")+" SET compact_row_hash=?"+where, append([]any{make([]byte, 32)}, key...)...); err != nil {
		t.Fatal(err)
	}
	if err = c.CommitPrepared(ctx, r.p); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("prefetch accepted changed facts", err)
	}
	if ok, err := c.Committed(ctx, d); err != nil || ok {
		t.Fatal("conflict left a durable receipt", ok, err)
	}
	var count int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_catalog_live_minute_commit")).Scan(&count); err != nil || count != 0 {
		t.Fatal("conflict published a partial minute", count, err)
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("derivative_book_quality_minute")+" SET compact_row_hash=?"+where, append([]any{original}, key...)...); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = c.CommitPrepared(ctx, r.p); err != nil {
			t.Fatal("original delivery could not retry", err)
		}
	}
	got, err := c.LoadOptionsCatalogMinute(ctx, run.ID, envelope.Live.MinuteTime)
	if err != nil || got.ID() != envelope.ID() {
		t.Fatal("prefetch changed the published envelope", err)
	}
}
