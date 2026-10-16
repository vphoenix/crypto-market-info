package writer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func TestEngineDrainsDiskWithRedisReadyAndConfirmsDuplicateCopies(t *testing.T) {
	testEngineDrain(t, 2, false)
}

func TestEngineDrainsCompositeQueueAndConfirmsDuplicateCopies(t *testing.T) {
	testEngineDrain(t, 2, true)
}

func testEngineDrain(t *testing.T, workerCount int, composite bool) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" || os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("isolated MySQL and Redis required")
	}
	ctx := context.Background()
	cfg := mysql.Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2, ReadTimeout: 2 * time.Minute}
	admin, e := mysql.Connect(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	target := "cmi_test_writer_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if e = mysql.InitSchema(ctx, admin, "crypto_market_info", target); e != nil {
		t.Fatal(e)
	}
	defer func() {
		admin.ExecContext(ctx, "DROP DATABASE `"+target+"`")
		for _, name := range []string{"ingest_batch", "ingest_member", "schema_version", "identity_allocator"} {
			admin.ExecContext(ctx, "DELETE FROM cmi_storage.`"+name+"` WHERE database_name=?", target)
		}
		admin.ExecContext(ctx, "DELETE FROM cmi_storage.producer_generation WHERE service=?", target)
	}()
	holder := uuid.New()
	gen, e := mysql.ActivateProducer(ctx, admin, target, 0, holder)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Database = target
	cfg.Family = "crypto_market_info"
	cfg.ReadTimeout = 0
	cfg.Producer = mysql.Producer{Service: target, Generation: gen, Holder: holder}
	c, e := mysql.Open(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if e = c.InitDiscoverySchema(ctx); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	q, _ := spool.Open(dir, 20<<30)
	rcfg := redisbuffer.Config{Socket: os.Getenv("CMI_REDIS_TEST_SOCKET"), Prefix: "cmi:test:writer:" + uuid.NewString(), WaitTimeout: 2 * time.Second}
	b, e := redisbuffer.Open(ctx, rcfg)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	acceptRedis := b.Accept
	if composite {
		queue := redisbuffer.Queue{Redis: b, Spool: q}
		acceptRedis = func(ctx context.Context, d contracts.Delivery) error {
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			return queue.Accept(attempt, d)
		}
	}
	makeDelivery := func(id uint32, minute time.Time) contracts.Delivery {
		t.Helper()
		buf, _ := sampler.NewMinuteBufferWithDepth(id, minute, 5)
		s := model.BookSnapshot{InstrumentID: id, SourceTime: minute, Bids: []model.Level{{PriceTick: 1000, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 1001, QtyLot: 2}}}
		for sec := range 60 {
			if err := buf.Sample(minute.Add(time.Duration(sec)*time.Second), s, sec != 12); err != nil {
				t.Fatal(err)
			}
		}
		batch, ok := buf.Batch()
		if !ok {
			t.Fatal("no batch")
		}
		ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: minute, Batches: []model.MinuteBatch{batch}})
		if err != nil {
			t.Fatal(err)
		}
		return ds[0]
	}
	minute := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	old := makeDelivery(1, minute.Add(-time.Hour))
	recent := makeDelivery(2, minute)
	if e = q.Accept(ctx, old); e != nil {
		t.Fatal(e)
	}
	if e = q.Accept(ctx, recent); e != nil {
		t.Fatal(e)
	}
	if e = acceptRedis(ctx, recent); e != nil {
		t.Fatal(e)
	}
	duplicateIDs := map[string]bool{recent.ID: true}
	// More than one worker wave, with overlap between both durable locations.
	additional := []contracts.Delivery{}
	for id := uint32(3); id < 12; id++ {
		at := minute
		if id%2 == 0 {
			at = at.Add(-time.Hour)
		}
		d := makeDelivery(id, at)
		if e = q.Accept(ctx, d); e != nil {
			t.Fatal(e)
		}
		if id%3 != 0 {
			if e = acceptRedis(ctx, d); e != nil {
				t.Fatal(e)
			}
			duplicateIDs[d.ID] = true
		}
		additional = append(additional, d)
	}
	// A disk fallback is a durable acceptance, but it alone cannot prove that
	// this test exercised duplicate copies. Require the actual Redis candidates
	// too, including a seal whose AOF confirmation may have timed out.
	if composite {
		candidates, err := b.Candidates(ctx, target, 0, 256)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range candidates {
			delete(duplicateIDs, candidate.Header.ID)
		}
		if len(duplicateIDs) != 0 {
			t.Fatal("fixture did not create both copies", len(duplicateIDs))
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(runCtx, Config{MySQL: cfg, Redis: rcfg, Spool: dir, Databases: []string{target}, Workers: workerCount, Preparers: 2, PrepareBytes: 64 << 20, CacheBytes: 32 << 20}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case e := <-done:
			if e != nil && !errors.Is(e, context.Canceled) {
				t.Error(e)
			}
		case <-time.After(15 * time.Second):
			t.Error("writer did not stop")
		}
	}()
	deadline := time.Now().Add(45 * time.Second)
	for {
		ok1, e1 := c.Committed(ctx, old)
		ok2, e2 := c.Committed(ctx, recent)
		_, f1 := os.Stat(filepath.Join(dir, spool.Name(old)))
		_, f2 := os.Stat(filepath.Join(dir, spool.Name(recent)))
		cs, e3 := b.Candidates(ctx, target, 0, 256)
		drained := true
		for _, d := range additional {
			ok, err := c.Committed(ctx, d)
			_, statErr := os.Stat(filepath.Join(dir, spool.Name(d)))
			drained = drained && err == nil && ok && os.IsNotExist(statErr)
		}
		if drained && e1 == nil && e2 == nil && e3 == nil && ok1 && ok2 && os.IsNotExist(f1) && os.IsNotExist(f2) && len(cs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("both ages or duplicate copies remained", ok1, ok2, e1, e2, e3)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
