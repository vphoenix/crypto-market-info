package redisbuffer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestCommittedCollectingCleanupProofAndLateWriters(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	key := b.bookKey(d, 1)
	if e := b.begin(ctx, b.client, d, []string{key}); e != nil {
		t.Fatal(e)
	}
	write := func() error {
		return b.client.Eval(ctx, memberScript, []string{key, b.batchKey(d.Database, d.ID)}, "member", hex.EncodeToString(d.Digest[:]), "payload", "partial").Err()
	}
	if e := write(); e != nil {
		t.Fatal(e)
	}
	cleanup := func(verify func(context.Context, contracts.Delivery) (bool, error)) (int, error) {
		return b.CleanupCommittedCollecting(ctx, d.Database, d.Minute.Add(10*time.Minute), verify)
	}
	if _, e := cleanup(nil); e == nil {
		t.Fatal("missing proof accepted")
	}
	for _, verify := range []func(context.Context, contracts.Delivery) (bool, error){
		func(context.Context, contracts.Delivery) (bool, error) { return false, nil },
		func(context.Context, contracts.Delivery) (bool, error) { return false, contracts.ErrConflict },
	} {
		cleanup(verify)
		if n, _ := b.client.Exists(ctx, key).Result(); n != 1 {
			t.Fatal("unproved member removed")
		}
	}
	n, e := cleanup(func(_ context.Context, got contracts.Delivery) (bool, error) {
		if !got.SameHeader(d) || len(got.Payload) != 0 {
			t.Fatal("incomplete proof identity")
		}
		return true, nil
	})
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if n, _ := b.client.Exists(ctx, key).Result(); n != 0 {
		t.Fatal("committed partial retained")
	}
	if state, _ := b.State(ctx, d); state != "retired" {
		t.Fatal(state)
	}
	if ttl, _ := b.client.TTL(ctx, b.batchKey(d.Database, d.ID)).Result(); ttl <= 0 {
		t.Fatal("tombstone not bounded")
	}
	if e = write(); e == nil {
		t.Fatal("late writer recreated member")
	}
	if e = b.begin(ctx, b.client, d, []string{key}); e == nil {
		t.Fatal("late begin resurrected retired parent")
	}
	b.client.Del(ctx, b.batchKey(d.Database, d.ID))
	if e = write(); e == nil {
		t.Fatal("late writer without parent recreated member")
	}
}

func TestCollectingCleanupProgressAndRecentRetention(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	for i := range 150 {
		d.ID = fmt.Sprintf("negative-%03d", i)
		if e := b.begin(ctx, b.client, d, []string{b.bookKey(d, 1)}); e != nil {
			t.Fatal(e)
		}
	}
	ready := d
	ready.ID = "committed"
	if e := b.begin(ctx, b.client, ready, []string{b.bookKey(ready, 1)}); e != nil {
		t.Fatal(e)
	}
	recent := d
	recent.ID = "recent"
	recent.Minute = ready.Minute.Add(10 * time.Minute)
	if e := b.begin(ctx, b.client, recent, []string{b.bookKey(recent, 1)}); e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for range 6 {
		_, e := b.CleanupCommittedCollecting(ctx, d.Database, recent.Minute, func(_ context.Context, got contracts.Delivery) (bool, error) {
			seen[got.ID] = true
			return got.ID == ready.ID, nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(seen) != 151 || seen[recent.ID] {
		t.Fatal("scan starvation/recent cleanup", len(seen))
	}
	if state, _ := b.State(ctx, ready); state != "retired" {
		t.Fatal("committed copy starved", state)
	}
	if state, _ := b.State(ctx, recent); state != "collecting" {
		t.Fatal("recent copy changed", state)
	}
}

func TestCollectingCleanupCASAndForeignMemberProtection(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	key := b.bookKey(d, 1)
	if e := b.begin(ctx, b.client, d, []string{key}); e != nil {
		t.Fatal(e)
	}
	b.client.HSet(ctx, key, "digest", "member")
	verify := func(context.Context, contracts.Delivery) (bool, error) {
		return true, b.client.HSet(ctx, b.batchKey(d.Database, d.ID), "state", "sealed").Err()
	}
	if n, e := b.CleanupCommittedCollecting(ctx, d.Database, d.Minute.Add(10*time.Minute), verify); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if n, _ := b.client.Exists(ctx, key).Result(); n != 1 {
		t.Fatal("concurrent seal removed")
	}
	bad := d
	bad.ID = "foreign"
	if e := b.begin(ctx, b.client, bad, []string{key}); e != nil {
		t.Fatal(e)
	}
	called := false
	_, e := b.CleanupCommittedCollecting(ctx, d.Database, d.Minute.Add(10*time.Minute), func(context.Context, contracts.Delivery) (bool, error) { called = true; return true, nil })
	if !errors.Is(e, contracts.ErrConflict) || called {
		t.Fatal("foreign member not rejected before proof", e, called)
	}
}
