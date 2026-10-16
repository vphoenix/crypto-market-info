package redisbuffer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func redisFixture(t *testing.T) contracts.Delivery {
	t.Helper()
	minute := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	buf, e := sampler.NewMinuteBufferWithDepth(1, minute, 5)
	if e != nil {
		t.Fatal(e)
	}
	s := model.BookSnapshot{InstrumentID: 1, SourceTime: minute, Bids: []model.Level{{PriceTick: 1000, QtyLot: 2}}, Asks: []model.Level{{PriceTick: 1001, QtyLot: 3}}}
	for second := range 60 {
		if second == 14 {
			s.Bids[0].QtyLot = 7
		}
		if second == 20 {
			s.Bids[0] = model.Level{PriceTick: 999, QtyLot: 4}
		}
		if e = buf.Sample(minute.Add(time.Duration(second)*time.Second), s, second != 12 && second != 13); e != nil {
			t.Fatal(e)
		}
	}
	batch, ok := buf.Batch()
	if !ok {
		t.Fatal("no batch")
	}
	raw, e := codec.EncodeBooks(model.CompletedMinute{MinuteTime: minute, Batches: []model.MinuteBatch{batch}})
	if e != nil {
		t.Fatal(e)
	}
	return contracts.Delivery{Database: "validation", Kind: "cex", ID: uuid.NewString(), Service: "test", Generation: 1, Minute: minute, Digest: sha256.Sum256(raw), Members: 1, Codec: 1, Payload: raw}
}
func openTestRedis(t *testing.T) *Buffer {
	t.Helper()
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("REDIS_INTEGRATION=1 enables isolated namespace tests")
	}
	b, e := Open(context.Background(), Config{Socket: os.Getenv("CMI_REDIS_TEST_SOCKET"), Prefix: "cmi:test:" + uuid.NewString(), WaitTimeout: 2 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			keys, next, e := b.client.Scan(ctx, cursor, b.prefix+":*", 100).Result()
			if e != nil {
				break
			}
			if len(keys) > 0 {
				b.client.Unlink(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		b.Close()
	})
	return b
}
func TestRedisDurableSealingRepairConflictAndCleanup(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	for range 2 {
		if e := b.Accept(ctx, d); e != nil {
			t.Fatal(e)
		}
	}
	pending, e := b.ServicePending(ctx, d.Database, d.Service, d.Generation)
	if e != nil || !pending {
		t.Fatal("sealed service not detected", e)
	}
	got, ok, e := b.Next(ctx, d.Database)
	if e != nil || !ok || !reflect.DeepEqual(d, got) {
		t.Fatalf("Redis round trip: %v", e)
	}
	if ttl, e := b.client.TTL(ctx, b.batchKey(d.Database, d.ID)).Result(); e != nil || ttl >= 0 {
		t.Fatalf("unconfirmed batch has TTL: %v", e)
	}
	changed := d
	completed, e := codec.DecodeBooks(d.Minute, d.Payload)
	if e != nil {
		t.Fatal(e)
	}
	completed.Batches[0].Minute.ValidBitmap ^= 1 << 59
	changed.Payload, e = codec.EncodeBooks(completed)
	if e != nil {
		t.Fatal(e)
	}
	changed.Digest = sha256.Sum256(changed.Payload)
	if e = b.Accept(ctx, changed); !errors.Is(e, contracts.ErrConflict) {
		t.Fatalf("conflict accepted: %v", e)
	}
	b.client.ZRem(ctx, b.readyKey(d.Database), d.ID)
	if e = b.RepairReady(ctx, d.Database); e != nil {
		t.Fatal(e)
	}
	if _, ok, e = b.Next(ctx, d.Database); e != nil || !ok {
		t.Fatal("ready index not repaired")
	}
	if yes, e := b.Lease(ctx, d, "one", time.Second); e != nil || !yes {
		t.Fatal(e)
	}
	if yes, e := b.Lease(ctx, d, "two", time.Second); e != nil || yes {
		t.Fatal("double lease")
	}
	if e = b.Cleanup(ctx, d.Database, d.Minute.Add(10*time.Minute), 10); e != nil {
		t.Fatal(e)
	}
	if _, ok, e = b.Next(ctx, d.Database); e != nil || !ok {
		t.Fatal("unconfirmed payload evicted")
	}
	no := func(context.Context, contracts.Delivery) (bool, error) { return false, nil }
	if e = b.Ack(ctx, d, no); e == nil {
		t.Fatal("unverified ACK")
	}
	yes := func(context.Context, contracts.Delivery) (bool, error) { return true, nil }
	if e = b.Ack(ctx, d, yes); e != nil {
		t.Fatal(e)
	}
	if _, ok, e = b.Next(ctx, d.Database); e != nil || ok {
		t.Fatal("ACK not removed")
	}
	if e = b.Cleanup(ctx, d.Database, d.Minute.Add(time.Minute), 10); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Load(ctx, d.Database, d.ID); e != nil {
		t.Fatal("previous minute deleted")
	}
	if e = b.Cleanup(ctx, d.Database, d.Minute.Add(2*time.Minute), 10); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Load(ctx, d.Database, d.ID); e == nil {
		t.Fatal("old committed cache not removed")
	}
}
func TestRedisSoftLimitSpoolFallbackAndRetention(t *testing.T) {
	b := openTestRedis(t)
	b.soft = 1
	disk, e := spool.Open(t.TempDir(), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	q := Queue{Redis: b, Spool: disk}
	d := redisFixture(t)
	ctx := context.Background()
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	got, ok, e := disk.Next(ctx)
	if e != nil || !ok || !reflect.DeepEqual(got, d) {
		t.Fatalf("spool fallback lost content: %v", e)
	}
}

func TestRedisPreviewIsProvisionalAndRetainedUntilProof(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	f := contracts.PreviewFrame{At: at, Scope: "fixture", Kind: "cex", Books: []contracts.PreviewBook{{InstrumentID: 1, StoredDepth: 5, Anchored: true, Valid: true, Snapshot: model.BookSnapshot{InstrumentID: 1, SourceTime: at, Bids: []model.Level{{PriceTick: 100, QtyLot: 3}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 4}}}}}}
	if e := b.WritePreview(ctx, "validation", "fixture", 1, f); e != nil {
		t.Fatal(e)
	}
	f.At = at.Add(59 * time.Second)
	f.Books[0].Valid = false
	if e := b.WritePreview(ctx, "validation", "fixture", 1, f); e != nil {
		t.Fatal(e)
	}
	out, e := b.PreviewMinute(ctx, "validation", "fixture", 1, "fixture", at)
	if e != nil || !out.Provisional || out.SampledBitmap != (uint64(1)|uint64(1)<<59) || out.ValidBitmap[1] != 1 || !out.LastSampled.Equal(f.At) {
		t.Fatal(out, e)
	}
	ttl, e := b.client.TTL(ctx, b.previewKey("validation", "fixture", 1, "fixture", at)).Result()
	if e != nil || ttl != -1 {
		t.Fatal("collecting preview has TTL", ttl, e)
	}
	no := func(context.Context, contracts.PreviewFrame) (bool, error) { return false, nil }
	if e = b.CleanupPreview(ctx, "validation", time.Now().UTC(), no); e != nil {
		t.Fatal(e)
	}
	if _, e = b.LoadPreview(ctx, "validation", "fixture", 1, "fixture", f.At); e != nil {
		t.Fatal("uncommitted preview deleted", e)
	}
	yes := func(context.Context, contracts.PreviewFrame) (bool, error) { return true, nil }
	if e = b.CleanupPreview(ctx, "validation", time.Now().UTC(), yes); e != nil {
		t.Fatal(e)
	}
	if _, e = b.LoadPreview(ctx, "validation", "fixture", 1, "fixture", f.At); e == nil {
		t.Fatal("committed old preview retained")
	}
	// An uncommitted partial minute can also retire after explicit producer fencing.
	f.At = at
	if e = b.WritePreview(ctx, "validation", "fixture", 1, f); e != nil {
		t.Fatal(e)
	}
	if e = b.CleanupAbandonedPreview(ctx, "validation", time.Now().UTC(), func(context.Context, string) (uint64, error) { return 2, nil }); e != nil {
		t.Fatal(e)
	}
	if _, e = b.LoadPreview(ctx, "validation", "fixture", 1, "fixture", f.At); e == nil {
		t.Fatal("fenced partial preview retained")
	}
}
func TestRedisCollectingAbandonmentRequiresNewGeneration(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	key := b.bookKey(d, 1)
	if e := b.begin(ctx, b.client, d, []string{key}); e != nil {
		t.Fatal(e)
	}
	current := func(context.Context, string) (uint64, error) { return 1, nil }
	if e := b.CleanupAbandoned(ctx, d.Database, d.Minute.Add(10*time.Minute), current); e != nil {
		t.Fatal(e)
	}
	state, _ := b.client.HGet(ctx, b.batchKey(d.Database, d.ID), "state").Result()
	if state != "collecting" {
		t.Fatal("current producer abandoned")
	}
	current = func(context.Context, string) (uint64, error) { return 2, nil }
	if e := b.CleanupAbandoned(ctx, d.Database, d.Minute.Add(10*time.Minute), current); e != nil {
		t.Fatal(e)
	}
	state, _ = b.client.HGet(ctx, b.batchKey(d.Database, d.ID), "state").Result()
	if state != "abandoned" {
		t.Fatal(state)
	}
}
func TestRedisAOFSealedDeliverySurvivesRestart(t *testing.T) {
	if os.Getenv("REDIS_RESTART_TEST") != "1" {
		t.Skip("explicit restart test for the isolated validation Redis")
	}
	const directory = "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/bench-redis"
	name := os.Getenv("CMI_REDIS_TEST_RESTART_CONTAINER")
	if name != "cmi-backlog-validation-redis" || os.Getenv("CMI_REDIS_TEST_SOCKET") != directory+"/run/redis.sock" {
		t.Fatal("restart requires the pinned isolated Redis container and socket")
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(probeCtx, "docker", "inspect", "--format", `{"Network":{{json .HostConfig.NetworkMode}},"Mounts":{{json .Mounts}}}`, name).Output()
	if err != nil {
		t.Fatal(err)
	}
	var target struct {
		Network string
		Mounts  []struct{ Source, Destination string }
	}
	if err = json.Unmarshal(raw, &target); err != nil {
		t.Fatal(err)
	}
	dataOK := false
	for _, mount := range target.Mounts {
		dataOK = dataOK || (mount.Source == directory+"/data" && mount.Destination == "/data")
	}
	if target.Network != "none" || !dataOK {
		t.Fatal("Redis isolation or data directory does not match")
	}
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	if e := b.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	if out, e := exec.CommandContext(probeCtx, "docker", "restart", "--signal", "SIGKILL", name).CombinedOutput(); e != nil {
		t.Fatalf("restart isolated Redis: %v: %s", e, out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, e := b.Load(ctx, d.Database, d.ID)
		if e == nil {
			if !reflect.DeepEqual(got, d) {
				t.Fatal("durable delivery changed")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sealed handoff missing after AOF recovery", e)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
