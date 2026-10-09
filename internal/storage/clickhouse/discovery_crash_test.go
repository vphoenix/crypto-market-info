package clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDiscoveryFirstKnowledgeSurvivesFailureBeforePublication(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	f := discoveryFixture(t, c)
	c.maxAttempts = 1
	c.derivativeAfterInsert = func(table string) error {
		if table == f.Kind() {
			return fmt.Errorf("simulated crash after fact before final marker")
		}
		return nil
	}
	if err := c.WriteDiscoveryFact(ctx, f); err == nil {
		t.Fatal("fault not exercised")
	}
	var first int64
	if err := c.conn.QueryRow(ctx, "SELECT known_from_ms FROM "+c.table("source_publication_intents")+" WHERE source_kind=? AND source_id=?", f.Kind(), f.ID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now()); err == nil {
		t.Fatal("incomplete transaction visible")
	}
	c.derivativeAfterInsert = nil
	time.Sleep(3 * time.Millisecond)
	if err := c.WriteDiscoveryFact(ctx, f); err != nil {
		t.Fatal(err)
	}
	_, p, err := c.LoadDiscoveryForecast(ctx, f.InstrumentID, time.Now().Add(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if p.KnownMS != first || p.PublishedMS < first {
		t.Fatal("restart retry regenerated first knowledge")
	}
}
