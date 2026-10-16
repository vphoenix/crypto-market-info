package spool

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func delivery() contracts.Delivery {
	p := []byte{1, 0, 255, 7}
	return contracts.Delivery{Database: "crypto_market_info", Kind: "cex", ID: "test-1", Service: "collector", Generation: 1, Minute: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC), Codec: 1, Members: 1, Payload: p, Digest: sha256.Sum256(p)}
}
func TestDurableIdempotenceConflictAndACK(t *testing.T) {
	ctx := context.Background()
	q, e := Open(t.TempDir(), 4096)
	if e != nil {
		t.Fatal(e)
	}
	d := delivery()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := q.Accept(ctx, d); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	q2, e := Open(q.Directory, 4096)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := q2.List(ctx)
	if e != nil || len(rows) != 1 || !same(rows[0], d) {
		t.Fatalf("restart recovery: %v", e)
	}
	changed := d
	changed.Payload = []byte{2}
	changed.Digest = sha256.Sum256(changed.Payload)
	if e = q.Accept(ctx, changed); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal(e)
	}
	no := func(context.Context, contracts.Delivery) (bool, error) { return false, nil }
	if e = q.Ack(ctx, d, no); e == nil {
		t.Fatal("uncommitted ACK accepted")
	}
	if _, e = os.Stat(filepath.Join(q.Directory, Name(d))); e != nil {
		t.Fatal("lost uncommitted batch")
	}
	yes := func(context.Context, contracts.Delivery) (bool, error) { return true, nil }
	if e = q.Ack(ctx, d, yes); e != nil {
		t.Fatal(e)
	}
	rows, e = q.List(ctx)
	if e != nil || len(rows) != 0 {
		t.Fatal(e)
	}
}
func TestCapacityAndInterruptedTemporaryFile(t *testing.T) {
	ctx := context.Background()
	d := delivery()
	raw, _ := Marshal(d)
	q, _ := Open(t.TempDir(), int64(len(raw)+20))
	if e := q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	next := d
	next.ID = "next"
	if e := q.Accept(ctx, next); !errors.Is(e, contracts.ErrCapacity) {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(q.Directory, ".pending-interrupted"), []byte{0}, 0600); e != nil {
		t.Fatal(e)
	}
	rows, e := q.List(ctx)
	if e != nil || len(rows) != 1 {
		t.Fatalf("incomplete file published: %v", e)
	}
	// A retry of the accepted batch must still work even when quota is exhausted.
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
}
func TestCorruptionFailsClosed(t *testing.T) {
	d := delivery()
	raw, _ := Marshal(d)
	for i := range raw {
		if _, e := Unmarshal(raw[:i]); e == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	bad := append([]byte(nil), raw...)
	bad[len(bad)-33] ^= 1
	if _, e := Unmarshal(bad); e == nil {
		t.Fatal("checksum accepted")
	}
	q, _ := Open(t.TempDir(), 4096)
	os.WriteFile(filepath.Join(q.Directory, Name(d)), bad, 0600)
	if _, e := q.List(context.Background()); e == nil {
		t.Fatal("corruption hidden")
	}
}

func TestRecoveryPromotesCompleteTemporaryAndRetainsPartial(t *testing.T) {
	ctx := context.Background()
	q, _ := Open(t.TempDir(), 4096)
	d := delivery()
	raw, _ := Marshal(d)
	pending := filepath.Join(q.Directory, ".pending-crash")
	if e := os.WriteFile(pending, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e := q.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	got, ok, e := q.Next(ctx)
	if e != nil || !ok || !same(got, d) {
		t.Fatal("complete handoff lost", e)
	}
	partial := filepath.Join(q.Directory, ".pending-partial")
	os.WriteFile(partial, raw[:10], 0600)
	if e = q.Recover(ctx); e == nil {
		t.Fatal("partial silently discarded")
	}
	if _, e = os.Stat(partial); e != nil {
		t.Fatal("partial file removed", e)
	}
}

func TestEmptyHealthAndProducerDrain(t *testing.T) {
	ctx := context.Background()
	q, _ := Open(t.TempDir(), 4096)
	h, e := q.Inspect(ctx)
	if e != nil || h.Files != 0 || h.Bytes != 0 {
		t.Fatal("lock file counted as pending", h, e)
	}
	pending, e := q.ServicePending(ctx, "collector", 1)
	if e != nil || pending {
		t.Fatal("empty producer drain", e)
	}
	d := delivery()
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	pending, e = q.ServicePending(ctx, d.Service, d.Generation)
	if e != nil || !pending {
		t.Fatal("sealed spool missed", e)
	}
	h, e = q.Inspect(ctx)
	if e != nil || h.Files != 1 || h.Bytes <= 0 {
		t.Fatal(h, e)
	}
}
