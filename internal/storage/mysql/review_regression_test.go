package mysql

import (
	"context"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestReviewUInt256MigrationKey(t *testing.T) {
	typ, err := codec.Parse("UInt256")
	if err != nil {
		t.Fatal(err)
	}
	mapping := Table{Name: "fixture", Key: []string{"deposit_id"}, Columns: []Column{{Name: "deposit_id", SourceType: "UInt256", Type: typ}}}
	a, err := MigrationKey(mapping, MigrationRow{"deposit_id": *big.NewInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := MigrationKey(mapping, MigrationRow{"deposit_id": *big.NewInt(2)})
	if err != nil {
		t.Fatal(err)
	}
	cmp, err := CompareMigrationKeys(mapping, a, b)
	if err != nil || cmp >= 0 {
		t.Fatalf("decoded UInt256 keys 1 < 2 must compare: cmp=%d err=%v", cmp, err)
	}
}

type reviewQueue struct{ accepted int }

func (q *reviewQueue) Accept(context.Context, contracts.Delivery) error { q.accepted++; return nil }

func TestReviewMySQLOutageStillEnqueuesMinute(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	q := &reviewQueue{}
	c.SetDeliveryQueue(q, nil)
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	batch := testBatch(t, 1, at, 5)
	// Only close this test client's connection pool. The server stays up.
	if err := c.db.Close(); err != nil {
		t.Fatal(err)
	}
	err := c.WriteCompletedMinute(ctx, model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{batch}})
	if err != nil || q.accepted != 1 {
		t.Fatalf("MySQL outage blocked durable queue: accepted=%d err=%v", q.accepted, err)
	}
}

func TestReviewDuplicatePublicationRemainsAmbiguous(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	row := MigrationRow{"source_kind": "fixture", "source_id": "id", "content_hash": strings.Repeat("a", 64), "codec": model.DiscoveryCodec, "source_ms": int64(1000), "observed_ms": int64(2000), "known_from_ms": int64(3000), "published_ms": int64(4000), "complete": uint8(1), "instrument_id": uint32(1), "effective_ms": int64(1000), "source_count": uint16(0), "provenance_hash": strings.Repeat("b", 64)}
	result, err := migrateRows(t, c, "source_batch_status", []MigrationRow{row, row}, uuid.New())
	if err != nil || result.Inserted != 2 {
		t.Fatalf("fixture import: %+v %v", result, err)
	}
	proof, err := c.latestDiscoveryProof(ctx, "fixture", 1, time.UnixMilli(5000))
	if err == nil {
		t.Fatalf("two physical publications accepted as valid: source=%s/%s", proof.Kind, proof.ID)
	}
}
