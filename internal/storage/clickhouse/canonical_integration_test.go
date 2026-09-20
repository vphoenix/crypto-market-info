package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func commonUniverseTestClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 to run ClickHouse integration tests")
	}
	database := fmt.Sprintf("crypto_market_info_universe_it_%d", time.Now().UnixNano())
	client, err := Open(context.Background(), Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Database: database, Username: "default", WriteTimeout: 15 * time.Second, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// This database was created by this test, never the collector database.
		if err := client.conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+database+"` SYNC"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		_ = client.Close()
	})
	if err = client.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = client.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestClickHouseCommonUniverseMappingRollbackAndVisibility(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	definitions := []model.Instrument{perpDefinition("BTC"), perpDefinition("BTC")}
	definitions[0].Exchange, definitions[1].Exchange = "TestA", "TestB"
	registered, err := c.RegisterInstruments(ctx, definitions)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().UTC().Truncate(time.Millisecond)
	revisionA, revisionB := strings.Repeat("a", 64), strings.Repeat("b", 64)
	makeMappings := func(revision, base, kind string, at time.Time) []model.CanonicalMapping {
		var out []model.CanonicalMapping
		for _, instrument := range registered {
			out = append(out, model.CanonicalMapping{InstrumentID: instrument.ID, MappingRevision: revision,
				CanonicalMarketKey: base + "-USDT-PERP", CanonicalBaseAsset: base, CanonicalQuoteAsset: "USDT", CanonicalSettleAsset: "USDT",
				CanonicalBaseUnitsPerVenueBaseUnit: decimal.NewFromInt(1), MappingKind: kind, RecordedAt: at})
		}
		return out
	}
	for _, mappings := range [][]model.CanonicalMapping{
		makeMappings(revisionA, "BTC", "identity", when),
		makeMappings(revisionB, "WRONG", "alias", when.Add(time.Hour)),
		makeMappings(revisionA, "BTC", "identity", when.Add(2*time.Hour)),
	} {
		if err = c.WriteCanonicalMappings(ctx, mappings); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := c.CanonicalMappings(ctx, revisionA)
	if err != nil || len(stored) != 2 || !stored[0].RecordedAt.Equal(when) {
		t.Fatalf("revision A did not retain first prepared time: %+v %v", stored, err)
	}
	bad := makeMappings(revisionA, "OTHER", "alias", when)
	if err = c.WriteCanonicalMappings(ctx, bad); err == nil {
		t.Fatal("mapping revision conflict was overwritten")
	}
	run := model.PerpetualUniverseRun{RunID: uuid.New(), SelectionRevision: strings.Repeat("c", 64), MappingRevision: revisionA,
		StartedAt: when.Add(3 * time.Hour), SelectionConfigJSON: `{"venues":[{"name":"TestA"},{"name":"TestB"}]}`,
		EnabledVenues: []string{"TestA", "TestB"}, CanonicalGroupCount: 1, InstrumentCount: 2}
	members := []model.PerpetualUniverseMember{
		{RunID: run.RunID, InstrumentID: registered[0].ID, CanonicalMarketKey: "BTC-USDT-PERP"},
		{RunID: run.RunID, InstrumentID: registered[1].ID, CanonicalMarketKey: "BTC-USDT-PERP"},
	}
	if err = c.insertUniverseMembers(ctx, members[:1]); err != nil {
		t.Fatal(err)
	}
	if _, got, queryErr := c.PerpetualUniverse(ctx, run.RunID); !errors.Is(queryErr, sql.ErrNoRows) || got != nil {
		t.Fatalf("orphan membership became visible: %+v %v", got, queryErr)
	}
	for range 2 {
		if err = c.WritePerpetualUniverseRun(ctx, run, members); err != nil {
			t.Fatal(err)
		}
	}
	resolved, err := c.CanonicalMappingsForRun(ctx, run.RunID)
	if err != nil || len(resolved) != 2 || resolved[0].CanonicalBaseAsset != "BTC" || resolved[0].MappingRevision != revisionA {
		t.Fatalf("A -> B -> A used wrong interpretation: %+v %v", resolved, err)
	}
	changed := run
	changed.StartedAt = changed.StartedAt.Add(time.Millisecond)
	if err = c.WritePerpetualUniverseRun(ctx, changed, members); err == nil {
		t.Fatal("run identity changed on retry")
	}
	var mappingRows, runRows, memberRows uint64
	for table, target := range map[string]*uint64{"instrument_canonical_mapping": &mappingRows, "perpetual_universe_run": &runRows, "perpetual_universe_member": &memberRows} {
		if err = c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table(table)+" FINAL").Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if mappingRows != 4 || runRows != 1 || memberRows != 2 {
		t.Fatalf("not idempotent: mappings=%d runs=%d members=%d", mappingRows, runRows, memberRows)
	}
	badRun := run
	badRun.RunID = uuid.New()
	badMembers := append([]model.PerpetualUniverseMember(nil), members...)
	for index := range badMembers {
		badMembers[index].RunID = badRun.RunID
		badMembers[index].CanonicalMarketKey = "OTHER-USDT-PERP"
	}
	if err = c.WritePerpetualUniverseRun(ctx, badRun, badMembers); err == nil {
		t.Fatal("run without matching mappings committed")
	}
	if orphans, queryErr := c.universeMembers(ctx, badRun.RunID); queryErr != nil || len(orphans) != 0 {
		t.Fatalf("validation failure partially wrote members: %+v %v", orphans, queryErr)
	}
}

type tracedBatchConn struct {
	driver.Conn
	queries    []string
	failMinute bool
}

func (c *tracedBatchConn) PrepareBatch(ctx context.Context, query string, options ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.queries = append(c.queries, query)
	if c.failMinute && strings.Contains(query, "`order_book_minute`") {
		c.failMinute = false
		return nil, errors.New("injected minute visibility failure")
	}
	return c.Conn.PrepareBatch(ctx, query, options...)
}

func TestClickHouseCompletedMinuteChunkingRegistrationAndRetry(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	trace := &tracedBatchConn{Conn: c.conn}
	c.conn = trace
	definitions := make([]model.Instrument, 201)
	for index := range definitions {
		definitions[index] = spotDefinition(fmt.Sprintf("BATCH%03d", index))
	}
	invalid := append([]model.Instrument(nil), definitions...)
	invalid[len(invalid)-1].PriceTickSize = decimal.Zero
	if _, err := c.RegisterInstruments(ctx, invalid); err == nil || len(trace.queries) != 0 {
		t.Fatalf("invalid batch registered partial instruments: queries=%v err=%v", trace.queries, err)
	}
	registered, err := c.RegisterInstruments(ctx, definitions)
	if err != nil || len(trace.queries) != 1 {
		t.Fatalf("registration did not use one batch: queries=%v err=%v", trace.queries, err)
	}
	changed := definitions[0]
	changed.PriceTickSize = decimal.RequireFromString("0.01")
	if _, err = c.RegisterInstruments(ctx, []model.Instrument{definitions[0], changed}); err == nil {
		t.Fatal("conflicting live definitions accepted")
	}
	newVersion, err := c.RegisterInstruments(ctx, []model.Instrument{changed})
	if err != nil || newVersion[0].ID == registered[0].ID {
		t.Fatalf("historical tick change did not allocate a new ID: %+v %v", newVersion, err)
	}
	minute := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	completed := model.CompletedMinute{MinuteTime: minute}
	for _, instrument := range registered {
		completed.Batches = append(completed.Batches, measuredBatch(instrument.ID, minute, true))
	}
	trace.queries = nil
	invalidMinute := completed
	invalidMinute.Batches = append([]model.MinuteBatch(nil), completed.Batches...)
	invalidMinute.Batches[200].Minute.ValidBitmap = 0
	if err = c.WriteCompletedMinute(ctx, invalidMinute); err == nil || len(trace.queries) != 0 {
		t.Fatal("invalid final instrument permitted partial minute inserts")
	}
	if err = c.WriteCompletedMinute(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if len(trace.queries) != 6 {
		t.Fatalf("201 instruments should require six inserts, got %d", len(trace.queries))
	}
	for index, query := range trace.queries {
		want := "order_book_second_delta"
		if index%2 == 1 {
			want = "order_book_minute"
		}
		if !strings.Contains(query, want) {
			t.Fatalf("insert %d violated delta-before-visibility ordering: %s", index, query)
		}
	}
	for _, instrumentIndex := range []int{0, 100, 200} {
		for second := range 60 {
			book, valid, queryErr := c.ReplayBook(ctx, registered[instrumentIndex].ID, minute.Add(time.Duration(second)*time.Second))
			if queryErr != nil || !valid || book.Bids[0].QtyLot != uint64(second+1) {
				t.Fatalf("instrument=%d second=%d book=%+v valid=%v err=%v", instrumentIndex, second, book, valid, queryErr)
			}
		}
	}
	// Deltas survive a failed visibility insert; retry restores the same result.
	retryMinute := model.CompletedMinute{MinuteTime: minute.Add(time.Minute), Batches: []model.MinuteBatch{measuredBatch(registered[0].ID, minute.Add(time.Minute), true)}}
	c.maxAttempts, trace.failMinute = 1, true
	if err = c.WriteCompletedMinute(ctx, retryMinute); err == nil {
		t.Fatal("visibility fault was not injected")
	}
	if _, err = c.LoadMinute(ctx, registered[0].ID, retryMinute.MinuteTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("minute visible before successful insert: %v", err)
	}
	if err = c.WriteCompletedMinute(ctx, retryMinute); err != nil {
		t.Fatal(err)
	}
	deltas, err := c.LoadDeltas(ctx, retryMinute.Batches[0].Minute.ID, 59)
	if err != nil || len(deltas) != 59 {
		t.Fatalf("retry created logical duplicate deltas: %d %v", len(deltas), err)
	}
}
