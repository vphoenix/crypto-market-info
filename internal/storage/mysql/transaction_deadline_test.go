package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestTransactionErrorPreservesCancellationCause(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := transactionError(ctx, fmt.Errorf("capture: %w", sql.ErrTxDone))
	var temporary interface{ Temporary() bool }
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, sql.ErrTxDone) || !errors.As(err, &temporary) || !temporary.Temporary() {
		t.Fatal("deadline rollback must remain retryable", err)
	}
	if err := transactionError(context.Background(), sql.ErrTxDone); errors.As(err, &temporary) {
		t.Fatal("unexpected completed transaction was made retryable", err)
	}
	if err := transactionError(ctx, nil); err != nil {
		t.Fatal("successful commit was changed by a later deadline", err)
	}
	conflict := errors.New("content conflict")
	if err := transactionError(ctx, conflict); err != conflict {
		t.Fatal("deadline hid a permanent failure", err)
	}
}

func TestMinuteDeadlineRollbackKeepsOriginalDelivery(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	batch := testBatch(t, 1, at, 5)
	deliveries, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{batch}})
	if err != nil {
		t.Fatal(err)
	}
	d := deliveries[0]
	p, err := c.PrepareDelivery(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	fault := context.WithValue(attempt, minuteFaultKey{}, func(stage string, tx *sql.Tx) error {
		if stage != "facts" {
			return nil
		}
		<-attempt.Done()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, err := tx.ExecContext(ctx, "SELECT 1")
			if errors.Is(err, sql.ErrTxDone) {
				return err
			}
			time.Sleep(time.Millisecond)
		}
		return errors.New("automatic minute rollback did not finish")
	})
	err = c.CommitPrepared(fault, p)
	var temporary interface{ Temporary() bool }
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, sql.ErrTxDone) || !errors.As(err, &temporary) || !temporary.Temporary() {
		t.Fatal("minute rollback lost its deadline", err)
	}
	if ok, err := c.Committed(ctx, d); err != nil || ok {
		t.Fatal("deadline exposed an ingest receipt", ok, err)
	}
	var count int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("order_book_minute")).Scan(&count); err != nil || count != 0 {
		t.Fatal("deadline exposed minute facts", count, err)
	}
	for range 2 {
		if err := c.CommitPrepared(ctx, p); err != nil {
			t.Fatal("same delivery could not retry", err)
		}
	}
	if ok, err := c.Committed(ctx, d); err != nil || !ok {
		t.Fatal("retried delivery lacks durable proof", ok, err)
	}
}

// Reproduce the real database/sql race: the transaction was rolled back by its
// context watcher, but the business callback still uses its parent context.
func TestMetadataDeadlineRollbackRetriesWithoutPartialFacts(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	table := c.table("deadline_retry_probe")
	if _, err := c.db.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.db.ExecContext(ctx, "DROP TABLE "+table) })
	attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := c.transaction(attempt, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" VALUES (1)"); err != nil {
			return err
		}
		<-attempt.Done()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, err := tx.ExecContext(ctx, "SELECT 1")
			if errors.Is(err, sql.ErrTxDone) {
				return fmt.Errorf("capture: %w", err)
			}
			time.Sleep(time.Millisecond)
		}
		return errors.New("automatic rollback did not finish")
	})
	var temporary interface{ Temporary() bool }
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, sql.ErrTxDone) || !errors.As(err, &temporary) || !temporary.Temporary() {
		t.Fatal("lost automatic rollback cause", err)
	}
	var count int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
		t.Fatal("cancelled transaction exposed a partial fact", count, err)
	}
	if err = c.transaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO "+table+" VALUES (1)")
		return err
	}); err != nil {
		t.Fatal("original identity could not retry", err)
	}
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 1 {
		t.Fatal("retry did not commit exactly once", count, err)
	}
}
