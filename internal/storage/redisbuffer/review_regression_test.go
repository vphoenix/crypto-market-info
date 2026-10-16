package redisbuffer

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"testing"
	"time"
)

func TestReviewCommittedPreviewWithoutSecond59IsRetired(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	f := contracts.PreviewFrame{At: at, Scope: "fixture", Kind: "cex", Books: []contracts.PreviewBook{{InstrumentID: 1, StoredDepth: 5, Anchored: true, Valid: true, Snapshot: model.BookSnapshot{InstrumentID: 1, SourceTime: at, Bids: []model.Level{{PriceTick: 100, QtyLot: 3}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 4}}}}}}
	if err := b.WritePreview(ctx, "validation", "fixture", 1, f); err != nil {
		t.Fatal(err)
	}
	f.At = at.Add(58 * time.Second)
	if err := b.WritePreview(ctx, "validation", "fixture", 1, f); err != nil {
		t.Fatal(err)
	}
	verified := 0
	verify := func(context.Context, contracts.PreviewFrame) (bool, error) { verified++; return true, nil }
	if err := b.CleanupPreview(ctx, "validation", time.Now().UTC(), verify); err != nil {
		t.Fatal(err)
	}
	retained, err := b.client.Exists(ctx, b.previewKey("validation", "fixture", 1, "fixture", at)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if retained != 0 {
		t.Fatalf("committed old minute retained after missing s:59: verifier calls=%d", verified)
	}
}
