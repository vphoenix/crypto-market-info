package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func TestCatalogLifecycleBatchIdentityAndAmbiguousRetry(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	if err := c.InitOptionsCatalogSchema(ctx); err != nil {
		t.Fatal(err)
	}
	const count = 4096
	now, epoch := time.Now().UTC().Truncate(time.Microsecond), uuid.New()
	rows := make([]options.LifecycleObservation, count)
	for n := range rows {
		rows[n] = options.LifecycleObservation{ID: uuid.New(), Kind: "state", Channel: "instrument.state.option.BTC", Epoch: epoch,
			Sequence: uint64(n + 1), Symbol: "BTC-30OCT26-80000-C", State: "open", SourceTime: &now, ReceivedAt: now,
			PayloadHash: options.PayloadHash([]byte("batch fixture"))}
	}
	first := true
	c.derivativeAfterInsert = func(stage string) error {
		if stage == "lifecycle_observations" && first {
			first = false
			return errors.New("ambiguous lifecycle insert")
		}
		return nil
	}
	if err := c.WriteOptionsLifecycles(ctx, rows); err == nil {
		t.Fatal("ambiguous insert not simulated")
	}
	c.derivativeAfterInsert = nil
	if err := c.WriteOptionsLifecycles(ctx, rows); err != nil {
		t.Fatal("stable identity retry", err)
	}
	if err := c.WriteOptionsLifecycles(ctx, append(rows, rows[0])); err != nil {
		t.Fatal("same input duplicate", err)
	}
	var actual uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("options_lifecycle_observation")+" FINAL").Scan(&actual); err != nil || actual != count {
		t.Fatal("batch identity count", actual, err)
	}
	conflict := rows[0]
	conflict.State = "locked"
	fresh := rows[1]
	fresh.ID = uuid.New()
	if err := c.WriteOptionsLifecycles(ctx, []options.LifecycleObservation{fresh, conflict}); err == nil {
		t.Fatal("immutable conflict accepted")
	}
	var newRows uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("options_lifecycle_observation")+" FINAL WHERE observation_id=?", fresh.ID).Scan(&newRows); err != nil || newRows != 0 {
		t.Fatal("invalid batch partially wrote fresh observations", newRows, err)
	}
	if err := c.WriteOptionsLifecycles(ctx, []options.LifecycleObservation{fresh, conflict, rows[0]}); err == nil {
		t.Fatal("conflicting duplicate ID accepted")
	}
	t.Logf("batch_events=%d retry_identities=%d conflicting_batch_new_rows=%d", count, actual, newRows)
}
