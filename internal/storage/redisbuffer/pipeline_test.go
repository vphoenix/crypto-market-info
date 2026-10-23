package redisbuffer

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func TestPipelinedDeliveryAcrossChunksAndPartialConflict(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	book, e := codec.DecodeBooks(d.Minute, d.Payload)
	if e != nil {
		t.Fatal(e)
	}
	base := book.Batches[0]
	book.Batches = nil
	for i := uint32(1); i <= 100; i++ {
		batch := base
		batch.Minute.InstrumentID = i
		batch.Minute.ID, e = model.MinuteID(i, d.Minute)
		if e != nil {
			t.Fatal(e)
		}
		batch.Deltas = append([]model.BookDelta(nil), base.Deltas...)
		for j := range batch.Deltas {
			batch.Deltas[j].MinuteID = batch.Minute.ID
		}
		book.Batches = append(book.Batches, batch)
	}
	d.Payload, e = codec.EncodeBooks(book)
	if e != nil {
		t.Fatal(e)
	}
	d.Digest = sha256.Sum256(d.Payload)
	d.Members = 100
	if e = b.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	got, e := b.Load(ctx, d.Database, d.ID)
	if e != nil || !reflect.DeepEqual(got, d) {
		t.Fatal("multi-chunk roundtrip", e)
	}
	d.ID = "partial-conflict"
	if e = b.client.HSet(ctx, b.bookKey(d, 51), "digest", "conflict").Err(); e != nil {
		t.Fatal(e)
	}
	if e = b.Accept(ctx, d); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("middle chunk conflict hidden", e)
	}
	if state, _ := b.State(ctx, d); state != "collecting" {
		t.Fatal("partial batch sealed", state)
	}
	if _, e = b.client.ZScore(ctx, b.readyKey(d.Database), d.ID).Result(); e == nil {
		t.Fatal("partial batch published")
	}
}

func TestPipelinedRealOptionsDelivery(t *testing.T) {
	path := os.Getenv("CMI_REDIS_OPTIONS_FIXTURE")
	if path == "" {
		t.Skip("optional captured options fixture")
	}
	b := openTestRedis(t)
	ctx := context.Background()
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	d, e := spool.Unmarshal(raw)
	if e != nil {
		t.Fatal(e)
	}
	if d.Kind != "options_catalog" {
		t.Fatal("expected real catalog fixture")
	}
	d.Database = "validation"
	if e = b.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	got, e := b.Load(ctx, d.Database, d.ID)
	if e != nil || !reflect.DeepEqual(d, got) {
		t.Fatal("options roundtrip", e)
	}
}
