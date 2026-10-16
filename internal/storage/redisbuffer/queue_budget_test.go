package redisbuffer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type stalledRedis struct{ err error }

func (h stalledRedis) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h stalledRedis) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h stalledRedis) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.err != nil {
			return h.err
		}
		<-ctx.Done()
		return ctx.Err()
	}
}

func TestRedisStallReservesCallerDeadlineForDiskOwnership(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "conflict"}[conflict], func(t *testing.T) {
			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
			defer client.Close()
			hook := stalledRedis{}
			if conflict {
				hook.err = contracts.ErrConflict
			}
			client.AddHook(hook)
			disk, err := spool.Open(t.TempDir(), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			queue := Queue{Redis: &Buffer{client: client, wait: 2 * time.Second}, Spool: disk}
			delivery := redisFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = queue.Accept(ctx, delivery)
			if conflict {
				if !errors.Is(err, contracts.ErrConflict) {
					t.Fatal(err)
				}
				if _, found, err := disk.Next(context.Background()); err != nil || found {
					t.Fatal("conflict fell through to disk", found, err)
				}
				return
			}
			if err != nil || ctx.Err() != nil {
				t.Fatal("Redis consumed fallback deadline", err, ctx.Err())
			}
			got, found, err := disk.Next(context.Background())
			if err != nil || !found || !reflect.DeepEqual(got, delivery) {
				t.Fatal("fallback changed immutable delivery", found, err)
			}
		})
	}
}
