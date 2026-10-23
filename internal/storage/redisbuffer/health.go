package redisbuffer

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Health struct {
	UsedBytes                                         int64  `json:"used_bytes"`
	Ready                                             uint64 `json:"ready_batches"`
	OldestLagSeconds                                  int64  `json:"oldest_ready_lag_seconds"`
	Collecting, Sealed, Committed, Abandoned, Retired uint64
}

func (b *Buffer) Inspect(ctx context.Context, database string) (Health, error) {
	var out Health
	if !identity.MatchString(database) {
		return out, fmt.Errorf("invalid database")
	}
	used, e := b.memory(ctx)
	if e != nil {
		return out, e
	}
	out.UsedBytes = used
	n, e := b.client.ZCard(ctx, b.readyKey(database)).Result()
	if e != nil {
		return out, e
	}
	out.Ready = uint64(n)
	lag, e := b.OldestReadyLag(ctx, database, time.Now().UTC())
	if e != nil {
		return out, e
	}
	out.OldestLagSeconds = int64(lag / time.Second)
	var cursor uint64
	for {
		keys, next, e := b.client.Scan(ctx, cursor, b.prefix+":batch:"+database+":*", 100).Result()
		if e != nil {
			return out, e
		}
		for _, key := range keys {
			state, e := b.client.HGet(ctx, key, "state").Result()
			if e != nil {
				return out, e
			}
			switch state {
			case "collecting":
				out.Collecting++
			case "sealed":
				out.Sealed++
			case "committed":
				out.Committed++
			case "abandoned":
				out.Abandoned++
			case "retired":
				out.Retired++
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

type Preview struct {
	Provisional   bool                   `json:"provisional"`
	LastSampled   time.Time              `json:"last_sampled_utc"`
	SampledBitmap uint64                 `json:"sampled_bitmap"`
	ValidBitmap   map[uint32]uint64      `json:"valid_bitmap"`
	Latest        contracts.PreviewFrame `json:"latest"`
}

func (b *Buffer) PreviewMinute(ctx context.Context, database, service string, generation uint64, scope string, minute time.Time) (Preview, error) {
	out := Preview{Provisional: true, ValidBitmap: map[uint32]uint64{}}
	if !identity.MatchString(database) || !identity.MatchString(service) || generation == 0 || scope == "" || !minute.Equal(minute.Truncate(time.Minute)) {
		return out, fmt.Errorf("invalid explicit preview identity")
	}
	key := b.previewKey(database, service, generation, scope, minute)
	fields, e := b.client.HKeys(ctx, key).Result()
	if e != nil {
		return out, e
	}
	sort.Strings(fields)
	for _, field := range fields {
		if !strings.HasPrefix(field, "s:") {
			continue
		}
		second, e := strconv.Atoi(strings.TrimPrefix(field, "s:"))
		if e != nil || second < 0 || second > 59 {
			return out, contracts.ErrConflict
		}
		raw, e := b.client.HGet(ctx, key, field).Bytes()
		if e != nil {
			return out, e
		}
		f, e := codec.DecodePreview(raw)
		if e != nil {
			return out, e
		}
		if f.Scope != scope || !f.At.Equal(minute.Add(time.Duration(second)*time.Second)) {
			return out, contracts.ErrConflict
		}
		out.SampledBitmap |= uint64(1) << second
		out.LastSampled = f.At
		out.Latest = f
		for _, book := range f.Books {
			if book.Valid {
				out.ValidBitmap[book.InstrumentID] |= uint64(1) << second
			}
		}
		for _, book := range f.Derivatives {
			if book.Quality.ReplayValid {
				out.ValidBitmap[book.InstrumentID] |= uint64(1) << second
			}
		}
	}
	if out.LastSampled.IsZero() {
		return out, contracts.ErrNotFound
	}
	return out, nil
}
