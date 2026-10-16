package redisbuffer

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

func (b *Buffer) ServicePending(ctx context.Context, database, service string, generation uint64) (bool, error) {
	if !identity.MatchString(database) || !identity.MatchString(service) || generation == 0 {
		return false, fmt.Errorf("invalid drain identity")
	}
	var cursor uint64
	for {
		keys, next, e := b.client.Scan(ctx, cursor, b.prefix+":batch:"+database+":*", 100).Result()
		if e != nil {
			return false, e
		}
		for _, key := range keys {
			fields, e := b.client.HMGet(ctx, key, "state", "header").Result()
			if e != nil {
				return false, e
			}
			if fields[0] == nil || fields[0] != "sealed" {
				continue
			}
			if fields[1] == nil {
				return false, fmt.Errorf("sealed delivery header missing")
			}
			v, e := codec.Decode(headerType, []byte(fields[1].(string)))
			if e != nil {
				return false, e
			}
			h := v.(map[string]any)
			if h["service"] == service && h["generation"] == generation {
				return true, nil
			}
		}
		cursor = next
		if cursor == 0 {
			return false, nil
		}
		if e = ctx.Err(); e != nil {
			return false, e
		}
	}
}
