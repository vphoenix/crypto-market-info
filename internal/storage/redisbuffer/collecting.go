package redisbuffer

import (
	"context"
	"encoding/hex"
	"fmt"
	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"strings"
	"time"
)

const beginScript = `
local t=redis.call('TYPE',KEYS[1]).ok
if t~='none' and t~='hash' then return redis.error_reply('CMI_WRONGTYPE') end
local digest=redis.call('HGET',KEYS[1],'digest')
if digest and digest~=ARGV[1] then return redis.error_reply('CMI_CONFLICT') end
local header=redis.call('HGET',KEYS[1],'header')
if header and header~=ARGV[2] then return redis.error_reply('CMI_CONFLICT') end
if not digest then redis.call('HSET',KEYS[1],'digest',ARGV[1],'header',ARGV[2],'state','collecting') end
return 1`

func (b *Buffer) begin(ctx context.Context, conn interface {
	Eval(context.Context, string, []string, ...any) *redis.Cmd
}, d contracts.Delivery, memberKeys []string) error {
	header, e := codec.Encode(headerType, map[string]any{"database": d.Database, "kind": d.Kind, "id": d.ID, "service": d.Service, "generation": d.Generation, "minute": d.Minute, "digest": d.Digest[:], "members": d.Members, "codec": d.Codec, "keys": memberKeys})
	if e != nil {
		return e
	}
	return conflict(conn.Eval(ctx, beginScript, []string{b.batchKey(d.Database, d.ID)}, hex.EncodeToString(d.Digest[:]), header).Err())
}

const abandonScript = `
if redis.call('HGET',KEYS[1],'state')~='collecting' then return 0 end
redis.call('HSET',KEYS[1],'state','abandoned')
for i=2,#KEYS do redis.call('UNLINK',KEYS[i]) end
redis.call('EXPIRE',KEYS[1],86400)
return 1`

// Collecting headers belong to an old, explicitly fenced generation. They were
// never durable handoffs, and are recorded as abandoned before member cleanup.
func (b *Buffer) CleanupAbandoned(ctx context.Context, database string, now time.Time, current func(context.Context, string) (uint64, error)) error {
	if !identity.MatchString(database) || current == nil {
		return fmt.Errorf("abandoned cleanup requires producer verifier")
	}
	var cursor uint64
	for {
		keys, next, e := b.client.Scan(ctx, cursor, b.prefix+":batch:"+database+":*", 100).Result()
		if e != nil {
			return e
		}
		for _, key := range keys {
			fields, e := b.client.HGetAll(ctx, key).Result()
			if e != nil {
				return e
			}
			if fields["state"] != "collecting" {
				continue
			}
			v, e := codec.Decode(headerType, []byte(fields["header"]))
			if e != nil {
				return e
			}
			h := v.(map[string]any)
			if h["database"] != database {
				return contracts.ErrConflict
			}
			if !h["minute"].(time.Time).Add(2 * time.Minute).Before(now) {
				continue
			}
			generation, e := current(ctx, h["service"].(string))
			if e != nil {
				return e
			}
			if generation <= h["generation"].(uint64) {
				continue
			}
			all := []string{key}
			for _, v := range h["keys"].([]any) {
				s := v.(string)
				if !strings.HasPrefix(s, b.prefix+":book:"+database+":") {
					return contracts.ErrConflict
				}
				all = append(all, s)
			}
			if e = b.client.Eval(ctx, abandonScript, all).Err(); e != nil {
				return e
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
