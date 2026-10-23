package redisbuffer

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type collectingPage struct {
	cursor uint64
	keys   []string
}

// The immutable MySQL transaction is authoritative. CAS the complete header
// after verification so a concurrent seal is never removed. A tombstone plus
// parent checks in memberScript prevents in-flight writes recreating orphans.
const retireCollectingScript = `
if redis.call('HGET',KEYS[1],'state')~='collecting' then return 0 end
if redis.call('HGET',KEYS[1],'header')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] then return 0 end
redis.call('HSET',KEYS[1],'state','retired')
for i=2,#KEYS do redis.call('UNLINK',KEYS[i]) end
redis.call('EXPIRE',KEYS[1],86400)
return 1`

// CleanupCommittedCollecting reclaims failed Redis attempts whose complete
// delivery committed via spool. Absence, age or producer generation alone is
// never proof. Scan progress survives bounded calls, including negative proofs.
func (b *Buffer) CleanupCommittedCollecting(ctx context.Context, database string, now time.Time, verify func(context.Context, contracts.Delivery) (bool, error)) (int, error) {
	if !identity.MatchString(database) || verify == nil {
		return 0, fmt.Errorf("collecting cleanup requires commit verifier")
	}
	b.collectingMu.Lock()
	defer b.collectingMu.Unlock()
	if b.collectingPages == nil {
		b.collectingPages = map[string]*collectingPage{}
	}
	p := b.collectingPages[database]
	if p == nil {
		p = &collectingPage{}
		b.collectingPages[database] = p
	}
	retired, scans := 0, 0
	for examined := 0; examined < 100; {
		if err := ctx.Err(); err != nil {
			return retired, err
		}
		if len(p.keys) == 0 {
			if scans >= 16 {
				break
			}
			keys, next, err := b.client.Scan(ctx, p.cursor, b.prefix+":batch:"+database+":*", 200).Result()
			if err != nil {
				return retired, err
			}
			p.cursor, p.keys = next, keys
			scans++
			if len(keys) == 0 {
				if next == 0 {
					break
				}
				continue
			}
		}
		key := p.keys[0]
		p.keys = p.keys[1:]
		examined++
		fields, err := b.client.HGetAll(ctx, key).Result()
		if err != nil {
			return retired, err
		}
		if fields["state"] != "collecting" {
			continue
		}
		if len(fields["header"]) > 2<<20 {
			return retired, contracts.ErrConflict
		}
		value, err := codec.Decode(headerType, []byte(fields["header"]))
		if err != nil {
			return retired, err
		}
		h := value.(map[string]any)
		d := contracts.Delivery{Database: h["database"].(string), Kind: h["kind"].(string), ID: h["id"].(string), Service: h["service"].(string), Generation: h["generation"].(uint64), Minute: h["minute"].(time.Time), Members: h["members"].(uint32), Codec: h["codec"].(uint16)}
		copy(d.Digest[:], h["digest"].(string))
		if d.Database != database || key != b.batchKey(database, d.ID) || fields["digest"] != hex.EncodeToString(d.Digest[:]) || !identity.MatchString(d.ID) || !identity.MatchString(d.Service) || d.Generation == 0 || d.Members == 0 || !d.Minute.Equal(d.Minute.Truncate(time.Minute)) {
			return retired, contracts.ErrConflict
		}
		if !d.Minute.Add(2 * time.Minute).Before(now) {
			continue
		}
		members := h["keys"].([]any)
		limit := 100
		if d.Kind == "options_live" || d.Kind == "options_catalog" {
			limit = 1346
		} else if d.Kind != "cex" {
			return retired, contracts.ErrConflict
		}
		if len(members) == 0 || len(members) > limit {
			return retired, contracts.ErrConflict
		}
		prefix := fmt.Sprintf("%s:book:%s:%s:%d:%s:%d:", b.prefix, database, d.Kind, d.Generation, d.ID, d.Minute.Unix()/60)
		all, seen := []string{key}, map[string]bool{}
		for i, member := range members {
			s := member.(string)
			part := strings.TrimPrefix(s, prefix)
			n, e := strconv.ParseUint(part, 10, 32)
			if !strings.HasPrefix(s, prefix) || e != nil || strconv.FormatUint(n, 10) != part || seen[s] || (d.Kind != "cex" && n != uint64(i)) {
				return retired, contracts.ErrConflict
			}
			seen[s] = true
			all = append(all, s)
		}
		ok, err := verify(ctx, d)
		if err != nil {
			return retired, err
		}
		if ok {
			n, err := b.client.Eval(ctx, retireCollectingScript, all, fields["header"], fields["digest"]).Int()
			if err != nil {
				return retired, err
			}
			retired += n
		}
		if len(p.keys) == 0 && p.cursor == 0 {
			break
		}
	}
	return retired, nil
}
