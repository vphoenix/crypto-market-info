package mysql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMinuteFactCollisionRollsBackBeforeCompleteRetry(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	for n, scenario := range []string{"identical", "different_content", "changed_after_rollback"} {
		t.Run(scenario, func(t *testing.T) {
			at := time.Date(2026, 1, 1, 0, n, 0, 0, time.UTC)
			existing, fresh := testBatch(t, 2, at, 5), testBatch(t, 1, at, 10)
			seed := existing
			if scenario == "different_content" {
				seed.Minute.Bids[0].QtyLot++
			}
			if err := c.WriteMinute(ctx, seed); err != nil {
				t.Fatal(err)
			}
			ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{fresh, existing}})
			if err != nil {
				t.Fatal(err)
			}
			p, err := c.PrepareDelivery(ctx, ds[0])
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			claims, retries := 0, 0
			trace := &CommitTrace{}
			fault := context.WithValue(WithCommitTrace(ctx, trace), minuteFaultKey{}, func(stage string, tx *sql.Tx) error {
				if stage == "claimed" {
					claims++
				}
				if stage != "after_fact_duplicate_rollback" {
					return nil
				}
				retries++
				if tx != nil || !trace.RollbackConfirmed {
					t.Error("retry started without a confirmed full rollback")
				}
				if committed, err := c.Committed(ctx, ds[0]); err != nil || committed {
					t.Error("first attempt exposed a receipt", committed, err)
				}
				for _, query := range []string{
					"SELECT COUNT(*) FROM " + c.table("order_book_minute") + " WHERE id=?",
					"SELECT COUNT(*) FROM " + c.table("order_book_second_delta") + " WHERE minute_id=?",
					"SELECT COUNT(*) FROM cmi_storage.ingest_member WHERE database_name='" + c.database + "' AND kind='cex' AND member_id=?",
				} {
					var count int
					if err := c.db.QueryRowContext(ctx, query, fresh.Minute.ID).Scan(&count); err != nil || count != 0 {
						t.Error("first attempt exposed partial facts", count, err)
					}
				}
				probe, err := c.db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				var generation uint64
				err = probe.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
				probe.Rollback()
				if err != nil {
					return err
				}
				if scenario == "changed_after_rollback" {
					_, err = c.db.ExecContext(ctx, "UPDATE "+c.table("order_book_minute")+" SET valid_bitmap=? WHERE id=?", existing.Minute.ValidBitmap&^(uint64(1)<<14), existing.Minute.ID)
				}
				return err
			})
			err = c.CommitPrepared(fault, p)
			if claims != 2 || retries != 1 {
				t.Fatal("duplicate did not perform exactly one complete retry", claims, retries, err)
			}
			ok, proofErr := c.Committed(ctx, ds[0])
			if scenario != "identical" {
				if !errors.Is(err, contracts.ErrConflict) || proofErr != nil || ok {
					t.Fatal("fact collision was treated as batch success", err, ok, proofErr)
				}
				var count int
				if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("order_book_minute")+" WHERE id=?", fresh.Minute.ID).Scan(&count); e != nil || count != 0 {
					t.Fatal("failed retry exposed fresh facts", count, e)
				}
				return
			}
			if err != nil || proofErr != nil || !ok || !trace.Committed {
				t.Fatal("identical historical facts could not join the delivery", err, ok, proofErr)
			}
			for _, batch := range []model.MinuteBatch{existing, fresh} {
				for second := uint8(0); second < 60; second++ {
					want, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, second)
					if err != nil {
						t.Fatal(err)
					}
					got, gotValid, err := c.ReplayBook(ctx, batch.Minute.InstrumentID, at.Add(time.Duration(second)*time.Second))
					if err != nil || valid != gotValid || (valid && (!reflect.DeepEqual(want.Bids, got.Bids) || !reflect.DeepEqual(want.Asks, got.Asks))) {
						t.Fatal("retry changed exact replay", second, err)
					}
				}
			}
		})
	}
}

func TestCatalogFactCollisionUsesCompleteRetryAndReplay(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	run, envelope := catalogDBFixture(t, c)
	// Legacy/imported facts can exist without a delivery receipt. They cannot
	// authorize a batch success until every reference, member and domain commit
	// has been checked and committed together by the retry.
	if err := c.transaction(ctx, func(tx *sql.Tx) error {
		return c.insertDerivativeBooks(ctx, tx, envelope.ID(), envelope.Live.Books)
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := codec.EncodeOptionsCatalog(envelope)
	if err != nil {
		t.Fatal(err)
	}
	d := c.Delivery("options_catalog", run.ID.String(), envelope.Live.MinuteTime, uint32(len(envelope.Live.Books)), raw)
	p, err := c.PrepareDelivery(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	claims, retries := 0, 0
	fault := context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error {
		if stage == "claimed" {
			claims++
		}
		if stage != "after_fact_duplicate_rollback" {
			return nil
		}
		retries++
		if ok, err := c.Committed(ctx, d); err != nil || ok {
			t.Error("catalog first attempt exposed a receipt", ok, err)
		}
		var count int
		if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_catalog_live_minute_commit")).Scan(&count); err != nil || count != 0 {
			t.Error("catalog first attempt published a partial minute", count, err)
		}
		return nil
	})
	if err = c.CommitPrepared(fault, p); err != nil || claims != 2 || retries != 1 {
		t.Fatal("catalog legacy facts were not fully rechecked", claims, retries, err)
	}
	got, err := c.LoadOptionsCatalogMinute(ctx, run.ID, envelope.Live.MinuteTime)
	if err != nil || got.ID() != envelope.ID() {
		t.Fatal("catalog content changed", err)
	}
	for _, book := range got.Live.Books {
		for sec := uint8(0); sec < 60; sec++ {
			if _, err := replay.ReplayDerivative(book, sec); err != nil {
				t.Fatal(sec, err)
			}
		}
	}
}

func TestMinuteFactCollisionRechecksGenerationAfterRollback(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old, fresh := testBatch(t, 2, at, 5), testBatch(t, 1, at, 5)
	if err := c.WriteMinute(ctx, old); err != nil {
		t.Fatal(err)
	}
	ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{fresh, old}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.PrepareDelivery(ctx, ds[0])
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	retries := 0
	fault := context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error {
		if stage == "after_fact_duplicate_rollback" {
			retries++
			_, err := ActivateProducer(ctx, c.db, c.producer.Service, c.producer.Generation, uuid.New())
			return err
		}
		return nil
	})
	if err = c.CommitPrepared(fault, p); !errors.Is(err, contracts.ErrFenced) || retries != 1 {
		t.Fatal("retry reused the old generation", retries, err)
	}
	if committed, err := c.Committed(ctx, ds[0]); err != nil || committed {
		t.Fatal("fenced retry exposed a receipt", committed, err)
	}
	var count int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("order_book_minute")+" WHERE id=?", fresh.Minute.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("fenced retry left partial facts", count, err)
	}
}
