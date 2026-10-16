package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMinuteConcurrentDuplicateAndFaultRollback(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	b := testBatch(t, 1, at, 5)
	ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{b}})
	if err != nil {
		t.Fatal(err)
	}
	d := ds[0]
	for _, stage := range []string{"claimed", "facts"} {
		p, err := c.PrepareDelivery(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		fault := context.WithValue(ctx, minuteFaultKey{}, func(at string, _ *sql.Tx) error {
			if at == stage {
				return context.DeadlineExceeded
			}
			return nil
		})
		if err = c.CommitPrepared(fault, p); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(stage, err)
		}
		if ok, e := c.Committed(ctx, d); e != nil || ok {
			t.Fatal("rollback exposed batch", stage, ok, e)
		}
		var n int
		if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("order_book_minute")+" WHERE id=?", b.Minute.ID).Scan(&n); e != nil || n != 0 {
			t.Fatal("partial facts visible", stage, n, e)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := c.WithProducer(c.producer)
			p, e := client.PrepareDelivery(ctx, d)
			if e == nil {
				e = client.CommitPrepared(ctx, p)
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if ok, e := c.Committed(ctx, d); e != nil || !ok {
		t.Fatal(ok, e)
	}
	changed := d
	changed.Members++
	if _, e := c.Committed(ctx, changed); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("different content accepted", e)
	}
}

func TestMinuteSharedGenerationBlocksActivationAndOldProducer(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	// Use separate pools so two held minute transactions do not prevent the
	// administrative connection from attempting activation.
	admin, e := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	errs := make(chan error, 2)
	for n := range 2 {
		b := testBatch(t, uint32(n+1), at, 5)
		ds, e := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{b}})
		if e != nil {
			t.Fatal(e)
		}
		p, e := c.PrepareDelivery(ctx, ds[0])
		if e != nil {
			t.Fatal(e)
		}
		go func(p *PreparedDelivery) {
			blocked := context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error {
				if stage == "claimed" {
					entered <- struct{}{}
					<-release
				}
				return nil
			})
			errs <- c.WithProducer(c.producer).CommitPrepared(blocked, p)
		}(p)
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(20 * time.Second):
			close(release)
			t.Fatal("minute transactions serialized")
		}
	}
	activated := make(chan error, 1)
	go func() {
		_, e := ActivateProducer(ctx, admin, c.producer.Service, c.producer.Generation, uuid.New())
		activated <- e
	}()
	select {
	case e := <-activated:
		close(release)
		t.Fatal("activation passed active minutes", e)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	if e := <-activated; e != nil {
		t.Fatal(e)
	}
	b := testBatch(t, 3, at, 5)
	ds, _ := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{b}})
	p, e := c.PrepareDelivery(ctx, ds[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = c.CommitPrepared(ctx, p); !errors.Is(e, contracts.ErrFenced) {
		t.Fatal("old producer accepted", e)
	}
}
