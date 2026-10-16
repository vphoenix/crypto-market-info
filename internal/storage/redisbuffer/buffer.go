// Package redisbuffer owns delivery keys and durable sealing. Redis is a
// transient typed cache; a matching MySQL commit is required before cleanup.
package redisbuffer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type Config struct {
	Socket, Prefix string
	WaitTimeout    time.Duration
	SoftBytes      int64
}
type Buffer struct {
	client *redis.Client
	prefix string
	wait   time.Duration
	soft   int64
}

var identity = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,160}$`)
var headerType = parsed("Tuple(database String, kind String, id String, service String, generation UInt64, minute DateTime('UTC'), digest FixedString(32), members UInt32, codec UInt16, keys Array(String))")

func parsed(s string) *codec.Type {
	t, e := codec.Parse(s)
	if e != nil {
		panic(e)
	}
	return t
}

var uintType = parsed("UInt64")

func Open(ctx context.Context, cfg Config) (*Buffer, error) {
	if !strings.HasPrefix(cfg.Socket, "/") {
		return nil, fmt.Errorf("Redis requires explicit private Unix socket")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "cmi:v1"
	}
	if !identity.MatchString(cfg.Prefix) {
		return nil, fmt.Errorf("invalid Redis prefix")
	}
	if cfg.WaitTimeout <= 0 || cfg.WaitTimeout > 10*time.Second {
		return nil, fmt.Errorf("WAITAOF requires finite positive timeout")
	}
	if cfg.SoftBytes <= 0 {
		cfg.SoftBytes = 320 << 20
	}
	c := redis.NewClient(&redis.Options{Network: "unix", Addr: cfg.Socket, Protocol: 2, PoolSize: 2, MaxRetries: -1, ReadTimeout: cfg.WaitTimeout + 2*time.Second, WriteTimeout: 5 * time.Second, ContextTimeoutEnabled: true, DisableIdentity: true})
	if e := c.Ping(ctx).Err(); e != nil {
		c.Close()
		return nil, e
	}
	config, e := c.ConfigGet(ctx, "append*").Result()
	if e != nil {
		c.Close()
		return nil, e
	}
	policy, e := c.ConfigGet(ctx, "maxmemory-policy").Result()
	if e != nil {
		c.Close()
		return nil, e
	}
	if config["appendonly"] != "yes" || config["appendfsync"] != "everysec" || policy["maxmemory-policy"] != "noeviction" {
		c.Close()
		return nil, fmt.Errorf("Redis durability or eviction configuration mismatch")
	}
	return &Buffer{client: c, prefix: cfg.Prefix, wait: cfg.WaitTimeout, soft: cfg.SoftBytes}, nil
}
func (b *Buffer) Close() error                  { return b.client.Close() }
func (b *Buffer) batchKey(db, id string) string { return b.prefix + ":batch:" + db + ":" + id }
func (b *Buffer) readyKey(db string) string     { return b.prefix + ":ready:" + db }
func (b *Buffer) leaseKey(db, id string) string { return b.prefix + ":lease:" + db + ":" + id }
func (b *Buffer) windowKey(db string) string    { return b.prefix + ":window:" + db }
func (b *Buffer) bookKey(d contracts.Delivery, id uint32) string {
	return fmt.Sprintf("%s:book:%s:%s:%d:%s:%d:%d", b.prefix, d.Database, d.Kind, d.Generation, d.ID, d.Minute.Unix()/60, id)
}
func (b *Buffer) memory(ctx context.Context) (int64, error) {
	raw, e := b.client.Info(ctx, "memory").Result()
	if e != nil {
		return 0, e
	}
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			return strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "used_memory:")), 10, 64)
		}
	}
	return 0, fmt.Errorf("Redis used_memory missing")
}

const memberScript = `
local old=redis.call('HGET',KEYS[1],'digest')
if old and old~=ARGV[1] then return redis.error_reply('CMI_CONFLICT') end
if redis.call('TYPE',KEYS[1]).ok~='none' and redis.call('TYPE',KEYS[1]).ok~='hash' then return redis.error_reply('CMI_WRONGTYPE') end
redis.call('HSET',KEYS[1],'digest',ARGV[1])
for i=2,#ARGV,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
return 1`
const sealScript = `
local kind=redis.call('TYPE',KEYS[1]).ok
if kind~='none' and kind~='hash' then return redis.error_reply('CMI_WRONGTYPE') end
local readyType=redis.call('TYPE',KEYS[2]).ok
if readyType~='none' and readyType~='zset' then return redis.error_reply('CMI_WRONGTYPE') end
local old=redis.call('HGET',KEYS[1],'digest')
if old and old~=ARGV[1] then return redis.error_reply('CMI_CONFLICT') end
for i=3,#KEYS do
 if redis.call('HGET',KEYS[i],'digest')~=ARGV[i+3] then return redis.error_reply('CMI_INCOMPLETE') end
end
local state=redis.call('HGET',KEYS[1],'state')
if state and state~='collecting' and state~='sealed' and state~='committed' then return redis.error_reply('CMI_STATE') end
redis.call('HSET',KEYS[1],'digest',ARGV[1],'header',ARGV[2],'token',ARGV[5],'payload_bytes',ARGV[#ARGV])
if state~='committed' then
 redis.call('HSET',KEYS[1],'state','sealed')
 redis.call('ZADD',KEYS[2],ARGV[3],ARGV[4])
end
return 1`

func conflict(e error) error {
	if e != nil && strings.Contains(e.Error(), "CMI_CONFLICT") {
		return contracts.ErrConflict
	}
	return e
}
func (b *Buffer) Accept(ctx context.Context, d contracts.Delivery) error {
	if e := spool.Validate(d); e != nil {
		return e
	}
	if d.Kind == "options_live" || d.Kind == "options_catalog" {
		return b.acceptOptions(ctx, d)
	}
	if d.Kind != "cex" || (d.Codec != 1 && d.Codec != 2) {
		return fmt.Errorf("unsupported Redis delivery kind or codec")
	}
	completed, e := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
	if e != nil {
		return e
	}
	if len(completed.Batches) > 100 || uint32(len(completed.Batches)) != d.Members {
		return fmt.Errorf("Redis member quota mismatch")
	}
	used, e := b.memory(ctx)
	if e != nil {
		return e
	}
	if used+int64(len(d.Payload))*2 > b.soft {
		return contracts.ErrCapacity
	}
	conn := b.client.Conn()
	defer conn.Close()
	connectionID, e := conn.ClientID(ctx).Result()
	if e != nil {
		return e
	}
	planned := []string{}
	for _, batch := range completed.Batches {
		planned = append(planned, b.bookKey(d, batch.Minute.InstrumentID))
	}
	if e = b.begin(ctx, conn, d, planned); e != nil {
		return e
	}
	keys := []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database)}
	digests := []any{}
	memberKeys := []string{}
	for _, batch := range completed.Batches {
		key := b.bookKey(d, batch.Minute.InstrumentID)
		payload, e := codec.EncodeBookDelivery(model.CompletedMinute{MinuteTime: d.Minute, KnownFromMS: completed.KnownFromMS, Batches: []model.MinuteBatch{batch}}, d.Codec)
		if e != nil {
			return e
		}
		hash := sha256.Sum256(payload)
		anchor := batch
		anchor.Deltas = nil
		anchorRaw, e := codec.EncodeBookDelivery(model.CompletedMinute{MinuteTime: d.Minute, KnownFromMS: completed.KnownFromMS, Batches: []model.MinuteBatch{anchor}}, d.Codec)
		if e != nil {
			return e
		}
		valid, e := codec.Encode(uintType, batch.Minute.ValidBitmap)
		if e != nil {
			return e
		}
		// Every changed second is individually typed, while unchanged/invalid
		// seconds are represented by the final validity bitmap.
		args := []any{hex.EncodeToString(hash[:]), "anchor", anchorRaw, "valid", valid}
		for _, delta := range batch.Deltas {
			raw, e := codec.EncodeDelta(delta)
			if e != nil {
				return e
			}
			args = append(args, fmt.Sprintf("d:%02d", delta.SecondOffset), raw)
		}
		if e = conn.Eval(ctx, memberScript, []string{key}, args...).Err(); e != nil {
			return conflict(e)
		}
		keys = append(keys, key)
		memberKeys = append(memberKeys, key)
		digests = append(digests, hex.EncodeToString(hash[:]))
	}
	header, e := codec.Encode(headerType, map[string]any{"database": d.Database, "kind": d.Kind, "id": d.ID, "service": d.Service, "generation": d.Generation, "minute": d.Minute, "digest": d.Digest[:], "members": d.Members, "codec": d.Codec, "keys": memberKeys})
	if e != nil {
		return e
	}
	args := []any{hex.EncodeToString(d.Digest[:]), header, d.Minute.Unix() / 60, d.ID, uuid.NewString()}
	args = append(args, digests...)
	args = append(args, len(d.Payload))
	if e = conn.Eval(ctx, sealScript, keys, args...).Err(); e != nil {
		return conflict(e)
	}
	confirmed, e := conn.WaitAOF(ctx, 1, 0, b.wait).Result()
	if e != nil {
		return e
	}
	if len(confirmed) != 2 || confirmed[0] < 1 {
		return fmt.Errorf("Redis local AOF confirmation timed out")
	}
	after, e := conn.ClientID(ctx).Result()
	if e != nil {
		return e
	}
	if after != connectionID {
		return fmt.Errorf("Redis connection changed across durability barrier")
	}
	return nil
}

func (b *Buffer) Load(ctx context.Context, database, id string) (contracts.Delivery, error) {
	var d contracts.Delivery
	if !identity.MatchString(database) || !identity.MatchString(id) {
		return d, fmt.Errorf("invalid Redis batch identity")
	}
	fields, e := b.client.HGetAll(ctx, b.batchKey(database, id)).Result()
	if e != nil {
		return d, e
	}
	if fields["state"] != "sealed" && fields["state"] != "committed" {
		return d, fmt.Errorf("Redis batch is not sealed")
	}
	v, e := codec.Decode(headerType, []byte(fields["header"]))
	if e != nil {
		return d, e
	}
	h := v.(map[string]any)
	d = contracts.Delivery{Database: h["database"].(string), Kind: h["kind"].(string), ID: h["id"].(string), Service: h["service"].(string), Generation: h["generation"].(uint64), Minute: h["minute"].(time.Time), Members: h["members"].(uint32), Codec: h["codec"].(uint16)}
	copy(d.Digest[:], h["digest"].(string))
	if d.Database != database || d.ID != id || hex.EncodeToString(d.Digest[:]) != fields["digest"] {
		return d, contracts.ErrConflict
	}
	if d.Kind == "options_live" || d.Kind == "options_catalog" {
		return b.loadOptions(ctx, d, h)
	}
	keyValues := h["keys"].([]any)
	if len(keyValues) > 100 || uint32(len(keyValues)) != d.Members {
		return d, fmt.Errorf("Redis manifest count mismatch")
	}
	completed := model.CompletedMinute{MinuteTime: d.Minute}
	for _, key := range keyValues {
		key := key.(string)
		if !strings.HasPrefix(key, b.prefix+":book:"+database+":") {
			return d, fmt.Errorf("Redis manifest key outside namespace")
		}
		member, e := b.client.HGetAll(ctx, key).Result()
		if e != nil {
			return d, e
		}
		anchor, e := codec.DecodeBookDelivery(d.Minute, []byte(member["anchor"]), d.Codec)
		if e != nil {
			return d, e
		}
		if len(anchor.Batches) != 1 {
			return d, fmt.Errorf("Redis anchor count")
		}
		if len(completed.Batches) != 0 && completed.KnownFromMS != anchor.KnownFromMS {
			return d, contracts.ErrConflict
		}
		completed.KnownFromMS = anchor.KnownFromMS
		batch := anchor.Batches[0]
		for second := 1; second < 60; second++ {
			raw, ok := member[fmt.Sprintf("d:%02d", second)]
			if !ok {
				continue
			}
			delta, e := codec.DecodeDelta([]byte(raw))
			if e != nil {
				return d, e
			}
			if delta.MinuteID != batch.Minute.ID || int(delta.SecondOffset) != second {
				return d, fmt.Errorf("Redis second identity mismatch")
			}
			batch.Deltas = append(batch.Deltas, delta)
		}
		payload, e := codec.EncodeBookDelivery(model.CompletedMinute{MinuteTime: d.Minute, KnownFromMS: completed.KnownFromMS, Batches: []model.MinuteBatch{batch}}, d.Codec)
		if e != nil {
			return d, e
		}
		hash := sha256.Sum256(payload)
		if hex.EncodeToString(hash[:]) != member["digest"] {
			return d, contracts.ErrConflict
		}
		if b.bookKey(d, batch.Minute.InstrumentID) != key {
			return d, contracts.ErrConflict
		}
		completed.Batches = append(completed.Batches, batch)
	}
	d.Payload, e = codec.EncodeBookDelivery(completed, d.Codec)
	if e != nil {
		return d, e
	}
	return d, spool.Validate(d)
}
func (b *Buffer) Next(ctx context.Context, database string) (contracts.Delivery, bool, error) {
	var d contracts.Delivery
	if !identity.MatchString(database) {
		return d, false, fmt.Errorf("invalid database")
	}
	ids, e := b.client.ZRange(ctx, b.readyKey(database), 0, 0).Result()
	if e != nil {
		return d, false, e
	}
	if len(ids) == 0 {
		return d, false, nil
	}
	d, e = b.Load(ctx, database, ids[0])
	return d, e == nil, e
}
func (b *Buffer) Lease(ctx context.Context, d contracts.Delivery, owner string, ttl time.Duration) (bool, error) {
	if !identity.MatchString(owner) || ttl <= 0 || ttl > time.Minute {
		return false, fmt.Errorf("invalid lease")
	}
	return b.client.SetNX(ctx, b.leaseKey(d.Database, d.ID), owner, ttl).Result()
}

const ackScript = `
for i=1,4 do
 local t=redis.call('TYPE',KEYS[i]).ok
 local expected='string'
 if i==1 then expected='hash' elseif i==2 or i==3 then expected='zset' end
 if t~='none' and t~=expected then return redis.error_reply('CMI_WRONGTYPE') end
end
if redis.call('HGET',KEYS[1],'digest')~=ARGV[1] then return redis.error_reply('CMI_CONFLICT') end
if redis.call('HGET',KEYS[1],'state')~='sealed' and redis.call('HGET',KEYS[1],'state')~='committed' then return redis.error_reply('CMI_STATE') end
redis.call('HSET',KEYS[1],'state','committed')
redis.call('ZREM',KEYS[2],ARGV[2])
redis.call('ZADD',KEYS[3],ARGV[3],ARGV[2])
redis.call('DEL',KEYS[4])
return 1`

func (b *Buffer) Ack(ctx context.Context, d contracts.Delivery, verify func(context.Context, contracts.Delivery) (bool, error)) error {
	if verify == nil {
		return fmt.Errorf("Redis ACK requires database verifier")
	}
	ok, e := verify(ctx, d)
	if e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("MySQL batch not committed")
	}
	return conflict(b.client.Eval(ctx, ackScript, []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database), b.windowKey(d.Database), b.leaseKey(d.Database, d.ID)}, hex.EncodeToString(d.Digest[:]), d.ID, d.Minute.Unix()/60).Err())
}

const cleanupScript = `
if redis.call('HGET',KEYS[1],'state')~='committed' then return 0 end
redis.call('ZREM',KEYS[2],ARGV[1])
redis.call('UNLINK',KEYS[1])
for i=3,#KEYS do redis.call('UNLINK',KEYS[i]) end
return 1`

// Cleanup keeps the current and previous UTC minutes and never touches sealed
// batches. Limits bound both cache cleanup work and each UNLINK invocation.
func (b *Buffer) Cleanup(ctx context.Context, database string, now time.Time, limit int) error {
	if !identity.MatchString(database) || now.IsZero() || limit < 1 || limit > 100 {
		return fmt.Errorf("invalid Redis cleanup request")
	}
	cutoff := now.UTC().Truncate(time.Minute).Unix()/60 - 1
	ids, e := b.client.ZRangeByScore(ctx, b.windowKey(database), &redis.ZRangeBy{Min: "-inf", Max: "(" + strconv.FormatInt(cutoff, 10), Count: int64(limit)}).Result()
	if e != nil {
		return e
	}
	for _, id := range ids {
		key := b.batchKey(database, id)
		header, e := b.client.HGet(ctx, key, "header").Bytes()
		if e != nil {
			return e
		}
		v, e := codec.Decode(headerType, header)
		if e != nil {
			return e
		}
		h := v.(map[string]any)
		if h["database"].(string) != database || h["id"].(string) != id || h["minute"].(time.Time).Unix()/60 >= cutoff {
			return contracts.ErrConflict
		}
		keys := []string{key, b.windowKey(database)}
		for _, member := range h["keys"].([]any) {
			s := member.(string)
			if !strings.HasPrefix(s, b.prefix+":book:"+database+":") {
				return contracts.ErrConflict
			}
			keys = append(keys, s)
		}
		if e = b.client.Eval(ctx, cleanupScript, keys, id).Err(); e != nil {
			return e
		}
	}
	return nil
}

// Rebuilding the auxiliary ready index must not load every member payload.
// Final payload checks still run after claim, before any database commit/ACK.
const repairReadyScript = `
if redis.call('HGET',KEYS[1],'state')~='sealed' then return 0 end
if redis.call('HGET',KEYS[1],'header')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] then return redis.error_reply('CMI_CONFLICT') end
redis.call('ZADD',KEYS[2],ARGV[3],ARGV[4])
return 1`

// RepairReady scans bounded metadata pages and atomically repairs only sealed
// entries. A concurrent ACK/cleanup cannot resurrect an already committed item.
func (b *Buffer) RepairReady(ctx context.Context, database string) error {
	if !identity.MatchString(database) {
		return fmt.Errorf("invalid database")
	}
	var cursor uint64
	for {
		keys, next, err := b.client.Scan(ctx, cursor, b.prefix+":batch:"+database+":*", 100).Result()
		if err != nil {
			return err
		}
		pipe := b.client.Pipeline()
		headers := make([]*redis.SliceCmd, len(keys))
		scores := make([]*redis.FloatCmd, len(keys))
		for n, key := range keys {
			headers[n] = pipe.HMGet(ctx, key, "header", "state", "digest")
			id := strings.TrimPrefix(key, b.prefix+":batch:"+database+":")
			scores[n] = pipe.ZScore(ctx, b.readyKey(database), id)
		}
		if _, err = pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		repairs := b.client.Pipeline()
		for n, key := range keys {
			values, err := headers[n].Result()
			if err != nil {
				return err
			}
			if values[1] != "sealed" {
				continue
			}
			raw, ok := values[0].(string)
			if !ok || len(raw) > 2<<20 {
				return contracts.ErrConflict
			}
			value, err := codec.Decode(headerType, []byte(raw))
			if err != nil {
				return err
			}
			h := value.(map[string]any)
			id := strings.TrimPrefix(key, b.prefix+":batch:"+database+":")
			minute := h["minute"].(time.Time)
			digest := hex.EncodeToString([]byte(h["digest"].(string)))
			if !identity.MatchString(id) || h["database"] != database || h["id"] != id || values[2] != digest || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
				return contracts.ErrConflict
			}
			score, err := scores[n].Result()
			if err == nil && score == float64(minute.Unix()/60) {
				continue
			}
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			repairs.Eval(ctx, repairReadyScript, []string{key, b.readyKey(database)}, raw, digest, minute.Unix()/60, id)
		}
		if _, err = repairs.Exec(ctx); err != nil {
			return conflict(err)
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// Queue transfers ownership to disk if Redis rejects or cannot prove durability.
// Immutable-content conflicts never fall through to another storage backend.
type Queue struct {
	Redis *Buffer
	Spool *spool.Queue
}

func (q Queue) Accept(ctx context.Context, d contracts.Delivery) error {
	var e error
	if q.Redis != nil {
		redisCtx := ctx
		cancel := func() {}
		if q.Spool != nil {
			// Redis is the preferred durable destination, but must not consume
			// the caller's entire deadline before disk fallback can take over.
			budget := max(5*time.Second, q.Redis.wait+2*time.Second)
			if deadline, ok := ctx.Deadline(); ok {
				budget = min(budget, max(time.Nanosecond, time.Until(deadline)/2))
			}
			redisCtx, cancel = context.WithTimeout(ctx, budget)
		}
		e = q.Redis.Accept(redisCtx, d)
		cancel()
	} else {
		e = fmt.Errorf("Redis unavailable")
	}
	if e == nil {
		return nil
	}
	if errors.Is(e, contracts.ErrConflict) {
		return e
	}
	if q.Spool == nil {
		return e
	}
	if se := q.Spool.Accept(ctx, d); se != nil {
		return fmt.Errorf("Redis delivery failed (%v), spool: %w", e, se)
	}
	return nil
}

var _ contracts.DurableQueue = (*Buffer)(nil)
var _ contracts.DurableQueue = Queue{}

func (b *Buffer) OldestReadyLag(ctx context.Context, database string, now time.Time) (time.Duration, error) {
	if !identity.MatchString(database) {
		return 0, fmt.Errorf("invalid database")
	}
	rows, e := b.client.ZRangeWithScores(ctx, b.readyKey(database), 0, 0).Result()
	if e != nil || len(rows) == 0 {
		return 0, e
	}
	sealed := time.Unix(int64(rows[0].Score)*60+60, 0)
	if now.Before(sealed) {
		return 0, nil
	}
	return now.Sub(sealed), nil
}
