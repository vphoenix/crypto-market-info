package mysql

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func integrationClient(t *testing.T) *Client { return familyClient(t, "crypto_market_info") }
func familyClient(t *testing.T, family string) *Client {
	t.Helper()
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("MYSQL_INTEGRATION=1 enables isolated MySQL tests")
	}
	cfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2}
	// Administrative fixture DDL may be slow under production-like fsync load.
	// Keep its bound separate from ordinary collector reads.
	cfg.ReadTimeout = 2 * time.Minute
	ctx := context.Background()
	db, e := Connect(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	cfg.Database = "cmi_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	// Targeted minute suites can reuse physical tables: DDL/fsync of partitioned
	// fixtures must not dominate each independent correctness case. The server
	// UUID, private name and advisory ownership prevent an accidental live reset.
	reuse := os.Getenv("CMI_MYSQL_TEST_DATABASE")
	if reuse != "" {
		if family != "crypto_market_info" || !identifier.MatchString(reuse) || !strings.HasPrefix(reuse, "cmi_test_minute_") {
			t.Fatal("invalid reusable minute fixture")
		}
		var server string
		if e = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); e != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
			t.Fatal("reusable fixture requires the pinned isolated server", e)
		}
		lease, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		lock := "cmi:test:" + reuse
		var owned int
		if err = lease.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", lock).Scan(&owned); err != nil || owned != 1 {
			lease.Close()
			t.Fatal("reusable fixture is owned by another case", err)
		}
		t.Cleanup(func() {
			lease.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", lock)
			lease.Close()
		})
		cfg.Database = reuse
	}
	cfg.Family = family
	if family != "crypto_market_info" {
		cfg.MaxConnections = 1
	}
	if e = InitSchema(ctx, db, cfg.Family, cfg.Database); e != nil {
		t.Fatal(e)
	}
	service := cfg.Database
	holder := uuid.New()
	gen, e := ActivateProducer(ctx, db, service, 0, holder)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Producer = Producer{Service: service, Generation: gen, Holder: holder, Scope: "test"}
	cfg.ReadTimeout = 0
	c, e := Open(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		c.Close()
		if reuse != "" {
			if t.Failed() {
				return // Leave the failed case for inspection; use -test.failfast.
			}
			catalog, err := Mappings()
			if err != nil {
				t.Error(err)
				return
			}
			for _, table := range catalog.Tables {
				if table.Database == family {
					if _, err := db.ExecContext(ctx, "DELETE FROM `"+cfg.Database+"`.`"+table.Name+"`"); err != nil {
						t.Error("reusable fixture cleanup", table.Name, err)
						return
					}
				}
			}
		} else {
			db.ExecContext(ctx, "DROP DATABASE `"+cfg.Database+"`")
		}
		cleanupOK := true
		cleanup := func(query string, args ...any) {
			if _, err := db.ExecContext(ctx, query, args...); err != nil && reuse != "" {
				cleanupOK = false
				t.Error("reusable fixture control cleanup", err)
			}
		}
		for _, name := range []string{"migration_conflict", "migration_resolution"} {
			cleanup("DELETE FROM cmi_storage.`"+name+"` WHERE chunk_id IN (SELECT chunk_id FROM cmi_storage.migration_chunk WHERE database_name=?)", cfg.Database)
		}
		cleanup("DELETE FROM cmi_storage.migration_conflict WHERE run_id IN (SELECT run_id FROM cmi_storage.migration_table WHERE database_name=?)", cfg.Database)
		cleanup("DELETE FROM cmi_storage.migration_chunk WHERE database_name=?", cfg.Database)
		cleanup("DELETE FROM cmi_storage.migration_run WHERE run_id IN (SELECT run_id FROM cmi_storage.migration_table WHERE database_name=?)", cfg.Database)
		cleanup("DELETE FROM cmi_storage.migration_table WHERE database_name=?", cfg.Database)
		cleanup("DELETE FROM cmi_storage.ingest_member WHERE database_name=?", cfg.Database)
		cleanup("DELETE FROM cmi_storage.ingest_batch WHERE database_name=?", cfg.Database)
		if reuse == "" {
			cleanup("DELETE FROM cmi_storage.schema_version WHERE database_name=?", cfg.Database)
		}
		cleanup("DELETE FROM cmi_storage.identity_allocator WHERE database_name=?", cfg.Database)
		if reuse == "" || cleanupOK {
			cleanup("DELETE FROM cmi_storage.producer_generation WHERE service=?", service)
		}
	})
	return c
}
func testBatch(t *testing.T, id uint32, at time.Time, depth int) model.MinuteBatch {
	t.Helper()
	buffer, e := sampler.NewMinuteBufferWithDepth(id, at, depth)
	if e != nil {
		t.Fatal(e)
	}
	snapshot := model.BookSnapshot{InstrumentID: id, SourceTime: at}
	for i := range depth {
		snapshot.Bids = append(snapshot.Bids, model.Level{PriceTick: int64(1000 - i), QtyLot: uint64(i + 1)})
		snapshot.Asks = append(snapshot.Asks, model.Level{PriceTick: int64(1001 + i), QtyLot: uint64(i + 2)})
	}
	for second := 0; second < 60; second++ {
		valid := second != 12 && second != 13
		if second == 14 {
			snapshot.Bids[0].QtyLot = 7
		}
		if second == 20 {
			snapshot.Bids = append(snapshot.Bids[1:], model.Level{PriceTick: 900, QtyLot: 9})
		}
		if e = buffer.Sample(at.Add(time.Duration(second)*time.Second), snapshot, valid); e != nil {
			t.Fatal(e)
		}
	}
	b, ok := buffer.Batch()
	if !ok {
		t.Fatal("no anchor")
	}
	return b
}
func TestMySQLCoreMixedDepthReplayRetryAndFencing(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for n, depth := range []int{5, 10, 50} {
		var batch model.MinuteBatch
		if depth != 50 {
			batch = testBatch(t, uint32(n+1), at, depth)
		} else {
			batch = testBatch(t, uint32(n+1), at, 10)
			batch.Minute.StoredDepth = 50
			for i := range 50 {
				batch.Minute.Bids[i] = model.Level{PriceTick: int64(1000 - i), QtyLot: uint64(i + 1)}
				batch.Minute.Asks[i] = model.Level{PriceTick: int64(1001 + i), QtyLot: uint64(i + 2)}
			}
			batch.Deltas = []model.BookDelta{{MinuteID: batch.Minute.ID, SecondOffset: 20, BidChangePrice: []int64{1000, 900}, BidChangeQty: []uint64{0, 9}}}
		}
		for range 2 {
			if e := c.WriteMinute(ctx, batch); e != nil {
				t.Fatal(e)
			}
		}
		for second := range 60 {
			want, valid, e := replay.AtSecond(batch.Minute, batch.Deltas, uint8(second))
			if e != nil {
				t.Fatal(e)
			}
			got, ok, e := c.ReplayBook(ctx, batch.Minute.InstrumentID, at.Add(time.Duration(second)*time.Second))
			if e != nil || valid != ok {
				t.Fatalf("second %d: %v", second, e)
			}
			want.SourceTime = got.SourceTime
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("depth %d second %d mismatch", depth, second)
			}
		}
		changed := batch
		changed.Minute.ValidBitmap ^= 1 << 59
		if e := c.WriteMinute(ctx, changed); !errors.Is(e, contracts.ErrConflict) {
			t.Fatalf("same identity mutated: %v", e)
		}
	}
	bad := model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{testBatch(t, 9, at, 5), testBatch(t, 10, at, 5)}}
	bad.Batches[1].Minute.ValidBitmap = 0
	if e := c.WriteCompletedMinute(ctx, bad); e == nil {
		t.Fatal("invalid tail accepted")
	}
	if _, e := c.LoadMinute(ctx, 9, at); !errors.Is(e, contracts.ErrNotFound) {
		t.Fatal("partial invalid envelope visible")
	}
	if _, e := ActivateProducer(ctx, c.db, c.producer.Service, c.producer.Generation, uuid.New()); e != nil {
		t.Fatal(e)
	}
	if e := c.WriteMinute(ctx, testBatch(t, 11, at, 5)); !errors.Is(e, contracts.ErrFenced) {
		t.Fatalf("stale producer accepted: %v", e)
	}
}
func TestMySQLConcurrentIdentityAndFundingPriority(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	d := model.Instrument{Exchange: "test", MarketType: model.MarketSpot, ExchangeSymbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.000000000000000001"), QuantityStepSize: decimal.RequireFromString("0.001")}
	var wg sync.WaitGroup
	ids := make(chan uint32, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, e := c.RegisterInstruments(ctx, []model.Instrument{d})
			if e != nil {
				t.Error(e)
				return
			}
			ids <- rows[0].ID
		}()
	}
	wg.Wait()
	close(ids)
	var first uint32
	for id := range ids {
		if first == 0 {
			first = id
		}
		if id != first {
			t.Fatal("identity collision")
		}
	}
	rows, e := c.Instruments(ctx)
	if e != nil || len(rows) != 1 {
		t.Fatal(e)
	}
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	actual := model.FundingRate{InstrumentID: first, HourTime: at, FundingTime: at, Rate: decimal.RequireFromString("-0.000000000000000001"), IsActual: true}
	if e = c.UpsertFundingRate(ctx, actual); e != nil {
		t.Fatal(e)
	}
	estimate := actual
	estimate.IsActual = false
	estimate.Rate = decimal.RequireFromString("0.99")
	if e = c.UpsertFundingRate(ctx, estimate); e != nil {
		t.Fatal(e)
	}
	pending, e := c.LoadPendingFundingConfirmations(ctx, at, at)
	if e != nil || len(pending) != 0 {
		t.Fatal("actual overwritten")
	}
	table, e := c.mapping("funding_rate_hourly")
	if e != nil {
		t.Fatal(e)
	}
	stored, e := readRecords(ctx, c.db, c.table(table.Name), table, "WHERE instrument_id=?", first)
	if e != nil || len(stored) != 1 {
		t.Fatal(e)
	}
	if !stored[0]["rate"].(decimal.Decimal).Equal(actual.Rate) {
		t.Fatal("decimal lost precision")
	}
}
