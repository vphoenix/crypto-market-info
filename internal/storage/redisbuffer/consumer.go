package redisbuffer

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (b *Buffer) State(ctx context.Context, d contracts.Delivery) (string, error) {
	state, e := b.client.HGet(ctx, b.batchKey(d.Database, d.ID), "state").Result()
	if e == redis.Nil {
		return "", nil
	}
	return state, e
}

// Candidates reads small headers in bounded pages; member payloads are fetched
// only after an atomic claim. The ready zset remains format compatible.
func (b *Buffer) Candidates(ctx context.Context, database string, offset, limit int64) ([]contracts.Candidate, error) {
	rows, _, err := b.CandidatePage(ctx, database, offset, limit)
	return rows, err
}

// CandidatePage advances by raw sorted-set entries. Concurrent ACKs can remove
// entries between ZRANGE and HMGET; fewer valid headers do not mean end-of-index.
func (b *Buffer) CandidatePage(ctx context.Context, database string, offset, limit int64) ([]contracts.Candidate, int64, error) {
	if !identity.MatchString(database) || offset < 0 || limit < 1 || limit > 256 {
		return nil, 0, fmt.Errorf("invalid candidate page")
	}
	ids, e := b.client.ZRange(ctx, b.readyKey(database), offset, offset+limit-1).Result()
	if e != nil {
		return nil, 0, e
	}
	pipe := b.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for n, id := range ids {
		cmds[n] = pipe.HMGet(ctx, b.batchKey(database, id), "header", "state", "digest", "payload_bytes")
	}
	if _, e = pipe.Exec(ctx); e != nil {
		return nil, 0, e
	}
	out := make([]contracts.Candidate, 0, len(ids))
	for n, cmd := range cmds {
		values, e := cmd.Result()
		if e != nil {
			return nil, 0, e
		}
		if values[0] == nil || values[1] != "sealed" {
			continue
		}
		v, e := codec.Decode(headerType, []byte(values[0].(string)))
		if e != nil {
			return nil, 0, e
		}
		h := v.(map[string]any)
		d := contracts.Delivery{Database: h["database"].(string), Kind: h["kind"].(string), ID: h["id"].(string), Service: h["service"].(string), Generation: h["generation"].(uint64), Minute: h["minute"].(time.Time), Members: h["members"].(uint32), Codec: h["codec"].(uint16)}
		copy(d.Digest[:], h["digest"].(string))
		if d.Database != database || d.ID != ids[n] || values[2] != hex.EncodeToString(d.Digest[:]) {
			return nil, 0, contracts.ErrConflict
		}
		var size int64
		if values[3] != nil {
			if _, e = fmt.Sscan(values[3].(string), &size); e != nil {
				return nil, 0, e
			}
		}
		out = append(out, contracts.Candidate{Header: d, Bytes: size})
	}
	next := offset + limit
	if int64(len(ids)) < limit {
		next = 0
	}
	return out, next, nil
}

const claimScript = `
if redis.call('ZSCORE',KEYS[2],ARGV[1])==false then return 0 end
if redis.call('HGET',KEYS[1],'state')~='sealed' then return 0 end
if redis.call('HGET',KEYS[1],'digest')~=ARGV[2] then return redis.error_reply('CMI_CONFLICT') end
local retry=redis.call('HGET',KEYS[1],'retry_at')
if retry and tonumber(retry)>tonumber(ARGV[4]) then return 0 end
if redis.call('SET',KEYS[3],ARGV[3],'NX','PX',ARGV[5]) then return 1 end
return 0`

func (b *Buffer) Claim(ctx context.Context, c contracts.Candidate, owner string, ttl time.Duration) (bool, error) {
	d := c.Header
	if !identity.MatchString(owner) || ttl <= 0 || ttl > time.Minute {
		return false, fmt.Errorf("invalid claim")
	}
	n, e := b.client.Eval(ctx, claimScript, []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database), b.leaseKey(d.Database, d.ID)}, d.ID, hex.EncodeToString(d.Digest[:]), owner, time.Now().UnixMilli(), ttl.Milliseconds()).Int()
	return n == 1, conflict(e)
}

const renewScript = `if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end return redis.call('PEXPIRE',KEYS[1],ARGV[2])`

func (b *Buffer) Renew(ctx context.Context, d contracts.Delivery, owner string, ttl time.Duration) (bool, error) {
	if !identity.MatchString(owner) || ttl <= 0 || ttl > time.Minute {
		return false, fmt.Errorf("invalid lease renewal")
	}
	n, e := b.client.Eval(ctx, renewScript, []string{b.leaseKey(d.Database, d.ID)}, owner, ttl.Milliseconds()).Int()
	return n == 1, e
}

const releaseScript = `
if redis.call('GET',KEYS[2])~=ARGV[1] then return 0 end
redis.call('HSET',KEYS[1],'retry_at',ARGV[2])
redis.call('DEL',KEYS[2])
return 1`

func (b *Buffer) Release(ctx context.Context, d contracts.Delivery, owner string, retryAt time.Time) (bool, error) {
	if !identity.MatchString(owner) {
		return false, fmt.Errorf("invalid lease owner")
	}
	n, e := b.client.Eval(ctx, releaseScript, []string{b.batchKey(d.Database, d.ID), b.leaseKey(d.Database, d.ID)}, owner, retryAt.UnixMilli()).Int()
	return n == 1, e
}

const ownedAckScript = `
local owner=redis.call('GET',KEYS[4])
if owner~=ARGV[4] then return 0 end
if redis.call('HGET',KEYS[1],'digest')~=ARGV[1] then return redis.error_reply('CMI_CONFLICT') end
if redis.call('HGET',KEYS[1],'state')~='sealed' and redis.call('HGET',KEYS[1],'state')~='committed' then return redis.error_reply('CMI_STATE') end
redis.call('HSET',KEYS[1],'state','committed')
redis.call('ZREM',KEYS[2],ARGV[2])
redis.call('ZADD',KEYS[3],ARGV[3],ARGV[2])
redis.call('DEL',KEYS[4])
return 1`

func (b *Buffer) AckOwned(ctx context.Context, d contracts.Delivery, owner string, verify func(context.Context, contracts.Delivery) (bool, error)) error {
	if verify == nil {
		return fmt.Errorf("Redis ACK requires commit proof")
	}
	ok, e := verify(ctx, d)
	if e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("batch is not committed")
	}
	n, e := b.client.Eval(ctx, ownedAckScript, []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database), b.windowKey(d.Database), b.leaseKey(d.Database, d.ID)}, hex.EncodeToString(d.Digest[:]), d.ID, d.Minute.Unix()/60, owner).Int()
	if e != nil {
		return conflict(e)
	}
	if n != 1 {
		return fmt.Errorf("Redis lease lost before ACK")
	}
	return nil
}
