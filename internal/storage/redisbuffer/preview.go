package redisbuffer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const previewScript = `redis.call('HINCRBY',KEYS[1],'revision',1);redis.call('HSET',KEYS[1],'state','collecting','service',ARGV[4],'generation',ARGV[5],ARGV[1],ARGV[2]);redis.call('ZADD',KEYS[2],ARGV[3],KEYS[1]);return 1`

func (b *Buffer) previewKey(database, service string, generation uint64, scope string, at time.Time) string {
	hash := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("%s:preview:%s:%s:%d:%s:%d", b.prefix, database, service, generation, hex.EncodeToString(hash[:16]), at.UTC().Unix()/60)
}
func (b *Buffer) WritePreview(ctx context.Context, database, service string, generation uint64, frame contracts.PreviewFrame) error {
	if !identity.MatchString(database) || !identity.MatchString(service) || generation == 0 {
		return fmt.Errorf("invalid preview owner")
	}
	raw, e := codec.EncodePreview(frame)
	if e != nil {
		return e
	}
	used, e := b.memory(ctx)
	if e != nil {
		return e
	}
	if used+int64(len(raw))*2 > 192<<20 {
		return contracts.ErrCapacity
	}
	return b.client.Eval(ctx, previewScript, []string{b.previewKey(database, service, generation, frame.Scope, frame.At), b.prefix + ":preview-window:" + database}, fmt.Sprintf("s:%02d", frame.At.Second()), raw, frame.At.Unix()/60, service, generation).Err()
}
func (b *Buffer) LoadPreview(ctx context.Context, database, service string, generation uint64, scope string, at time.Time) (contracts.PreviewFrame, error) {
	raw, e := b.client.HGet(ctx, b.previewKey(database, service, generation, scope, at), fmt.Sprintf("s:%02d", at.Second())).Bytes()
	if e != nil {
		return contracts.PreviewFrame{}, e
	}
	frame, e := codec.DecodePreview(raw)
	if e == nil && (!frame.At.Equal(at) || frame.Scope != scope) {
		return frame, contracts.ErrConflict
	}
	return frame, e
}

type PreviewPump struct {
	frames      chan contracts.PreviewFrame
	cancel      context.CancelFunc
	done        chan struct{}
	cleanupDone chan struct{}
	Drops       atomic.Uint64
}

func (b *Buffer) StartPreview(ctx context.Context, database, service string, generation uint64, verify ...func(context.Context, contracts.PreviewFrame) (bool, error)) *PreviewPump {
	ctx, cancel := context.WithCancel(ctx)
	p := &PreviewPump{cleanupDone: make(chan struct{}), frames: make(chan contracts.PreviewFrame, 16), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.cleanupDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if len(verify) > 0 {
					attempt, release := context.WithTimeout(ctx, 3*time.Second)
					b.CleanupPreview(attempt, database, time.Now().UTC(), verify[0])
					release()
				}
			}
		}
	}()
	go func() {
		defer close(p.done)
		for {
			select {
			case <-ctx.Done():
				return
			case f := <-p.frames:
				attempt, release := context.WithTimeout(ctx, time.Second)
				if e := b.WritePreview(attempt, database, service, generation, f); e != nil {
					p.Drops.Add(1)
				}
				release()
			}
		}
	}()
	return p
}
func (p *PreviewPump) OfferPreview(frame contracts.PreviewFrame) {
	select {
	case p.frames <- frame:
	default:
		p.Drops.Add(1)
	}
}
func (p *PreviewPump) Close() { p.cancel(); <-p.done; <-p.cleanupDone }

const retirePreviewScript = `
if redis.call('HGET',KEYS[1],'state')~='collecting' then return 0 end
if ARGV[2] and (redis.call('HGET',KEYS[1],'revision') or '0')~=ARGV[2] then return 0 end
redis.call('HSET',KEYS[1],'state',ARGV[1])
redis.call('ZREM',KEYS[2],KEYS[1])
redis.call('UNLINK',KEYS[1])
return 1`

func (b *Buffer) CleanupPreview(ctx context.Context, database string, now time.Time, verify func(context.Context, contracts.PreviewFrame) (bool, error)) error {
	if !identity.MatchString(database) || verify == nil {
		return fmt.Errorf("preview cleanup needs verifier")
	}
	keys, e := b.previewCleanupKeys(ctx, database, now, "committed")
	if e != nil {
		return e
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, b.prefix+":preview:"+database+":") {
			return contracts.ErrConflict
		}
		fields, e := b.client.HGetAll(ctx, key).Result()
		if e != nil {
			return e
		}
		frame, found, e := b.previewClosure(database, key, fields)
		if e != nil {
			return e
		}
		if !found {
			continue
		}
		committed, e := verify(ctx, frame)
		if e != nil {
			return e
		}
		if !committed {
			continue
		}
		if e = b.client.Eval(ctx, retirePreviewScript, []string{key, b.prefix + ":preview-window:" + database}, "committed", previewRevision(fields)).Err(); e != nil {
			return e
		}
	}
	return nil
}

func (b *Buffer) CleanupAbandonedPreview(ctx context.Context, database string, now time.Time, current func(context.Context, string) (uint64, error)) error {
	if !identity.MatchString(database) || current == nil {
		return fmt.Errorf("preview abandonment needs producer verifier")
	}
	keys, e := b.previewCleanupKeys(ctx, database, now, "abandoned")
	if e != nil {
		return e
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, b.prefix+":preview:"+database+":") {
			return contracts.ErrConflict
		}
		fields, e := b.client.HMGet(ctx, key, "service", "generation").Result()
		if e != nil {
			return e
		}
		if len(fields) != 2 || fields[0] == nil || fields[1] == nil {
			continue
		}
		generation, e := strconv.ParseUint(fields[1].(string), 10, 64)
		if e != nil {
			return e
		}
		active, e := current(ctx, fields[0].(string))
		if e != nil {
			return e
		}
		if active <= generation {
			continue
		}
		if e = b.client.Eval(ctx, retirePreviewScript, []string{key, b.prefix + ":preview-window:" + database}, "abandoned").Err(); e != nil {
			return e
		}
	}
	return nil
}

// ZSCAN advances independently of whether the oldest 100 minutes can retire.
// Persist the cursor so short-lived cleanup clients make progress as well.
func (b *Buffer) previewCleanupKeys(ctx context.Context, database string, now time.Time, mode string) ([]string, error) {
	cursorKey := b.prefix + ":preview-cleanup:" + database + ":" + mode
	cursor, e := b.client.Get(ctx, cursorKey).Uint64()
	if e != nil && e != redis.Nil {
		return nil, e
	}
	values, next, e := b.client.ZScan(ctx, b.prefix+":preview-window:"+database, cursor, "*", 100).Result()
	if e != nil {
		return nil, e
	}
	if e = b.client.Set(ctx, cursorKey, next, 0).Err(); e != nil {
		return nil, e
	}
	cutoff := now.UTC().Truncate(time.Minute).Unix()/60 - 1
	keys := []string{}
	for i := 0; i < len(values); i += 2 {
		minute, e := strconv.ParseInt(values[i+1], 10, 64)
		if e != nil {
			return nil, e
		}
		if minute < cutoff {
			keys = append(keys, values[i])
		}
	}
	return keys, nil
}

func previewRevision(fields map[string]string) string {
	if fields["revision"] == "" {
		return "0"
	}
	return fields["revision"]
}

// All retained frames contribute to the closure. A later frame may omit a
// member present earlier; it cannot authorize deleting that member's preview.
func (b *Buffer) previewClosure(database, key string, fields map[string]string) (contracts.PreviewFrame, bool, error) {
	var out contracts.PreviewFrame
	if len(fields) == 0 {
		return out, false, nil
	}
	generation, e := strconv.ParseUint(fields["generation"], 10, 64)
	if e != nil {
		return out, false, e
	}
	names := []string{}
	for name := range fields {
		if strings.HasPrefix(name, "s:") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	books := map[uint32]contracts.PreviewBook{}
	for _, name := range names {
		f, e := codec.DecodePreview([]byte(fields[name]))
		if e != nil {
			return out, false, e
		}
		if name != fmt.Sprintf("s:%02d", f.At.Second()) || key != b.previewKey(database, fields["service"], generation, f.Scope, f.At) {
			return out, false, contracts.ErrConflict
		}
		if !out.At.IsZero() && (out.Kind != f.Kind || out.Scope != f.Scope) {
			return out, false, contracts.ErrConflict
		}
		out = f
		for _, book := range f.Books {
			book.Anchored = book.Anchored || books[book.InstrumentID].Anchored
			books[book.InstrumentID] = book
		}
	}
	out.Books = nil
	for _, book := range books {
		out.Books = append(out.Books, book)
	}
	sort.Slice(out.Books, func(i, j int) bool { return out.Books[i].InstrumentID < out.Books[j].InstrumentID })
	return out, len(names) > 0, nil
}
