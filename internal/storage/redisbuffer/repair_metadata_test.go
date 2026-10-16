package redisbuffer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type rejectPayloadReads struct {
	enabled atomic.Bool
	calls   atomic.Int64
}

func (h *rejectPayloadReads) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *rejectPayloadReads) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.enabled.Load() && cmd.Name() == "hgetall" {
			h.calls.Add(1)
			return errors.New("unexpected payload read during index repair")
		}
		return next(ctx, cmd)
	}
}
func (h *rejectPayloadReads) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if h.enabled.Load() && cmd.Name() == "hgetall" {
				h.calls.Add(1)
				return errors.New("unexpected pipeline payload read")
			}
		}
		return next(ctx, cmds)
	}
}

func TestRedisRepairUsesMetadataAndDefersCorruptPayloadValidation(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	if err := b.Accept(ctx, d); err != nil {
		t.Fatal(err)
	}
	raw, err := b.client.HGet(ctx, b.batchKey(d.Database, d.ID), "header").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(headerType, raw)
	if err != nil {
		t.Fatal(err)
	}
	member := decoded.(map[string]any)["keys"].([]any)[0].(string)
	if err = b.client.HSet(ctx, member, "anchor", "corrupt fixture payload").Err(); err != nil {
		t.Fatal(err)
	}
	b.client.ZRem(ctx, b.readyKey(d.Database), d.ID)
	hook := &rejectPayloadReads{}
	hook.enabled.Store(true)
	b.client.AddHook(hook)
	if err = b.RepairReady(ctx, d.Database); err != nil {
		t.Fatal(err)
	}
	if hook.calls.Load() != 0 {
		t.Fatal("repair fetched unclaimed payload")
	}
	hook.enabled.Store(false)
	if score, err := b.client.ZScore(ctx, b.readyKey(d.Database), d.ID).Result(); err != nil || score != float64(d.Minute.Unix()/60) {
		t.Fatal("sealed metadata not restored", score, err)
	}
	if _, err = b.Load(ctx, d.Database, d.ID); err == nil {
		t.Fatal("corrupt payload accepted after repair")
	}
	if state, err := b.State(ctx, d); err != nil || state != "sealed" {
		t.Fatal("repair removed or acknowledged bad payload", state, err)
	}
	// Repair does not reset ownership or an independent retry deadline.
	b.client.Set(ctx, b.leaseKey(d.Database, d.ID), "original-owner", time.Minute)
	b.client.HSet(ctx, b.batchKey(d.Database, d.ID), "retry_at", time.Now().Add(time.Minute).UnixMilli())
	b.client.ZRem(ctx, b.readyKey(d.Database), d.ID)
	if err = b.RepairReady(ctx, d.Database); err != nil {
		t.Fatal(err)
	}
	if owner, _ := b.client.Get(ctx, b.leaseKey(d.Database, d.ID)).Result(); owner != "original-owner" {
		t.Fatal("lease reset")
	}
	candidates, err := b.Candidates(ctx, d.Database, 0, 10)
	if err != nil || len(candidates) != 1 {
		t.Fatal("candidate", err)
	}
	if owned, err := b.Claim(ctx, candidates[0], "another-owner", time.Minute); err != nil || owned {
		t.Fatal("repair bypassed lease/retry", owned, err)
	}
}
func TestRedisRepairCannotResurrectCommittedSnapshot(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	if err := b.Accept(ctx, d); err != nil {
		t.Fatal(err)
	}
	raw, err := b.client.HGet(ctx, b.batchKey(d.Database, d.ID), "header").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	n, err := b.client.Eval(ctx, repairReadyScript, []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database)}, raw, hex.EncodeToString(d.Digest[:]), d.Minute.Unix()/60, d.ID).Int()
	if err != nil || n != 0 {
		t.Fatal("committed snapshot requeued", n, err)
	}
	if _, err = b.client.ZScore(ctx, b.readyKey(d.Database), d.ID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("committed ready entry resurrected", err)
	}
}

func TestRedisCandidatePageDoesNotTreatVanishedHeadersAsEnd(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	if err := b.Accept(ctx, d); err != nil {
		t.Fatal(err)
	}
	for n := range 19 {
		id := fmt.Sprintf("stale-ack-%03d", n)
		b.client.HSet(ctx, b.batchKey(d.Database, id), "state", "committed")
		b.client.ZAdd(ctx, b.readyKey(d.Database), redis.Z{Score: float64(d.Minute.Unix()/60 - 1), Member: id})
	}
	first, next, err := b.CandidatePage(ctx, d.Database, 0, 16)
	if err != nil || len(first) != 0 || next != 16 {
		t.Fatal("filtered page ended scan", len(first), next, err)
	}
	second, next, err := b.CandidatePage(ctx, d.Database, next, 16)
	if err != nil || len(second) != 1 || next != 0 || !second[0].Header.SameHeader(d) {
		t.Fatal("valid later candidate lost", len(second), next, err)
	}
}
