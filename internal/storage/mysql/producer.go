package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

var metadataDiagnostics = slog.New(slog.NewJSONHandler(os.Stderr, nil))

type Producer struct {
	Service    string
	Generation uint64
	Holder     uuid.UUID
	Scope      string
}

// ActivateProducer is explicit administration with compare-and-swap semantics.
// Starting an ordinary client cannot steal the active producer's generation.
func ActivateProducer(ctx context.Context, db *sql.DB, service string, expected uint64, holder uuid.UUID) (uint64, error) {
	if service == "" || len(service) > 160 || holder == uuid.Nil || expected == math.MaxUint64 {
		return 0, fmt.Errorf("invalid producer activation")
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	var generation uint64
	e = tx.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE", service).Scan(&generation)
	if e == sql.ErrNoRows {
		if expected != 0 {
			return 0, contracts.ErrFenced
		}
		_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.producer_generation(service,generation,holder,activated_at) VALUES(?,1,?,UTC_TIMESTAMP(6))", service, holder[:])
		generation = 1
	} else if e == nil {
		if generation != expected {
			return 0, contracts.ErrFenced
		}
		generation++
		_, e = tx.ExecContext(ctx, "UPDATE cmi_storage.producer_generation SET generation=?,holder=?,activated_at=UTC_TIMESTAMP(6) WHERE service=?", generation, holder[:], service)
	}
	if e != nil {
		return 0, e
	}
	if e = tx.Commit(); e != nil {
		return 0, e
	}
	return generation, nil
}
func (c *Client) fence(ctx context.Context, tx *sql.Tx) error {
	return c.fenceLock(ctx, tx, false)
}

func (c *Client) fenceLock(ctx context.Context, tx *sql.Tx, shared bool) error {
	p := c.producer
	if p.Service == "" || p.Generation == 0 || p.Holder == uuid.Nil {
		return fmt.Errorf("writer requires an explicitly activated producer")
	}
	// Serialize writers of this producer while pinning generation through commit.
	// LOCK TABLES on the control schema permits this read without UPDATE authority
	// on producer_generation. Independent services lock independent rows.
	var gen uint64
	var holder []byte
	lock := " FOR UPDATE"
	if shared {
		lock = " FOR SHARE"
	}
	if e := tx.QueryRowContext(ctx, "SELECT generation,holder FROM cmi_storage.producer_generation WHERE service=?"+lock, p.Service).Scan(&gen, &holder); e != nil {
		return e
	}
	if gen != p.Generation || !bytes.Equal(holder, p.Holder[:]) {
		return contracts.ErrFenced
	}
	return nil
}
func (c *Client) transaction(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	caller, _, _, _ := runtime.Caller(1)
	return c.transactionWithFence(ctx, fn, false, caller)
}

// Immutable rules do not modify the minute publication state. A shared fence
// pins their generation through the entire atomic batch without stopping minute
// commits. Producer activation still takes the exclusive generation lock.
func (c *Client) tradingRuleTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	caller, _, _, _ := runtime.Caller(1)
	return c.transactionWithFence(ctx, fn, true, caller)
}

func (c *Client) transactionWithFence(ctx context.Context, fn func(*sql.Tx) error, shared bool, caller uintptr) (err error) {
	var lockTime, bodyTime, commitTime time.Duration
	defer func() {
		name := "unknown"
		if function := runtime.FuncForPC(caller); function != nil {
			name = function.Name()
		}
		recordMetadataTotal(c.database, c.producer.Service, name, lockTime, bodyTime, commitTime, err != nil)
		if bodyTime < time.Second && commitTime < time.Second {
			return
		}
		metadataDiagnostics.Warn("slow storage metadata transaction", "database", c.database, "service", c.producer.Service,
			"operation", name, "lock_ms", lockTime.Milliseconds(), "body_ms", bodyTime.Milliseconds(), "commit_ms", commitTime.Milliseconds(), "error_present", err != nil)
	}()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	defer func() { err = transactionError(ctx, err) }()
	if c.readOnly {
		return fmt.Errorf("read-only MySQL client")
	}
	c.writeOnce.Do(func() { c.writeSlot = make(chan struct{}, 1) })
	select {
	case c.writeSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.writeSlot }()
	tx, e := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return e
	}
	defer tx.Rollback()
	started := time.Now()
	e = c.fenceLock(ctx, tx, shared)
	lockTime = time.Since(started)
	if e != nil {
		return e
	}
	started = time.Now()
	e = fn(tx)
	bodyTime = time.Since(started)
	if e != nil {
		return e
	}
	started = time.Now()
	err = tx.Commit()
	commitTime = time.Since(started)
	return err
}
