package redisbuffer

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"testing"
	"time"
)

func cleanupFrame(at time.Time, scope string, id uint32) contracts.PreviewFrame {
	return contracts.PreviewFrame{At: at, Scope: scope, Kind: "cex", Books: []contracts.PreviewBook{{InstrumentID: id, StoredDepth: 5, Anchored: true, Valid: true, Snapshot: model.BookSnapshot{InstrumentID: id, SourceTime: at, Bids: []model.Level{{PriceTick: 100, QtyLot: 3}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 4}}}}}}
}

func TestPreviewCleanupProgressBeyondUncommittedHundred(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	old := now.Add(-10 * time.Minute)
	for i := 0; i < 130; i++ {
		f := cleanupFrame(old, fmt.Sprintf("pending-%03d", i), uint32(i+1))
		if e := b.WritePreview(ctx, "validation", "fixture", 1, f); e != nil {
			t.Fatal(e)
		}
	}
	ready := cleanupFrame(old.Add(time.Minute), "ready", 999)
	if e := b.WritePreview(ctx, "validation", "fixture", 1, ready); e != nil {
		t.Fatal(e)
	}
	// Recent minutes must remain even when the verifier would approve them.
	recent := cleanupFrame(now.Add(-time.Minute), "ready", 999)
	if e := b.WritePreview(ctx, "validation", "fixture", 1, recent); e != nil {
		t.Fatal(e)
	}
	verify := func(_ context.Context, f contracts.PreviewFrame) (bool, error) { return f.Scope == "ready", nil }
	for range 10 {
		if e := b.CleanupPreview(ctx, "validation", now, verify); e != nil {
			t.Fatal(e)
		}
	}
	if n, e := b.client.Exists(ctx, b.previewKey("validation", "fixture", 1, "ready", ready.At)).Result(); e != nil || n != 0 {
		t.Fatal("later committed minute starved", n, e)
	}
	if n, e := b.client.ZCard(ctx, b.prefix+":preview-window:validation").Result(); e != nil || n != 131 {
		t.Fatal("uncommitted/recent minutes removed", n, e)
	}
	if ttl, e := b.client.TTL(ctx, b.previewKey("validation", "fixture", 1, "pending-000", old)).Result(); e != nil || ttl != -1 {
		t.Fatal("uncommitted preview expires", ttl, e)
	}
	// Abandoned cleanup uses its own cursor, independent of commit cleanup.
	fenced := cleanupFrame(old.Add(2*time.Minute), "fenced", 1000)
	if e := b.WritePreview(ctx, "validation", "old-service", 1, fenced); e != nil {
		t.Fatal(e)
	}
	current := func(_ context.Context, service string) (uint64, error) {
		if service == "old-service" {
			return 2, nil
		}
		return 1, nil
	}
	for range 10 {
		if e := b.CleanupAbandonedPreview(ctx, "validation", now, current); e != nil {
			t.Fatal(e)
		}
	}
	if n, e := b.client.Exists(ctx, b.previewKey("validation", "old-service", 1, "fenced", fenced.At)).Result(); e != nil || n != 0 {
		t.Fatal("fenced minute starved", n, e)
	}
}

func TestPreviewCleanupRequiresAllMembersAndUnchangedRevision(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	first := cleanupFrame(at, "members", 1)
	last := cleanupFrame(at.Add(58*time.Second), "members", 2)
	for _, f := range []contracts.PreviewFrame{first, last} {
		if e := b.WritePreview(ctx, "validation", "fixture", 1, f); e != nil {
			t.Fatal(e)
		}
	}
	verify := func(_ context.Context, f contracts.PreviewFrame) (bool, error) {
		if len(f.Books) != 2 || f.Books[0].InstrumentID != 1 || f.Books[1].InstrumentID != 2 {
			t.Fatal("earlier member omitted", f)
		}
		return false, nil
	}
	if e := b.CleanupPreview(ctx, "validation", time.Now().UTC(), verify); e != nil {
		t.Fatal(e)
	}
	// A delayed frame written during verification invalidates cleanup's snapshot.
	verify = func(_ context.Context, f contracts.PreviewFrame) (bool, error) {
		extra := cleanupFrame(at.Add(59*time.Second), "members", 3)
		return true, b.WritePreview(ctx, "validation", "fixture", 1, extra)
	}
	if e := b.CleanupPreview(ctx, "validation", time.Now().UTC(), verify); e != nil {
		t.Fatal(e)
	}
	key := b.previewKey("validation", "fixture", 1, "members", at)
	if n, e := b.client.Exists(ctx, key).Result(); e != nil || n != 1 {
		t.Fatal("concurrent frame deleted", n, e)
	}
	verify = func(_ context.Context, f contracts.PreviewFrame) (bool, error) {
		if len(f.Books) != 3 {
			t.Fatal("missing member", f)
		}
		return true, nil
	}
	if e := b.CleanupPreview(ctx, "validation", time.Now().UTC(), verify); e != nil {
		t.Fatal(e)
	}
	if n, e := b.client.Exists(ctx, key).Result(); e != nil || n != 0 {
		t.Fatal("complete preview retained", n, e)
	}
}
