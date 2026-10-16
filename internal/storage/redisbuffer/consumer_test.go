package redisbuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestAtomicClaimOwnerRenewalRetryAndBothCopiesAck(t *testing.T) {
	b := openTestRedis(t)
	ctx := context.Background()
	d := redisFixture(t)
	if e := b.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	cs, e := b.Candidates(ctx, d.Database, 0, 256)
	if e != nil || len(cs) != 1 || cs[0].Bytes != int64(len(d.Payload)) || len(cs[0].Header.Payload) != 0 {
		t.Fatal("header candidates", cs, e)
	}
	var wg sync.WaitGroup
	wins := make(chan string, 2)
	for _, owner := range []string{"one", "two"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			ok, e := b.Claim(ctx, cs[0], owner, 300*time.Millisecond)
			if e != nil {
				t.Error(e)
			}
			if ok {
				wins <- owner
			}
		}(owner)
	}
	wg.Wait()
	close(wins)
	if len(wins) != 1 {
		t.Fatal("claim duplicated", len(wins))
	}
	owner := <-wins
	if ok, e := b.Renew(ctx, d, "wrong", time.Minute); e != nil || ok {
		t.Fatal("wrong owner renewed", e)
	}
	if ok, e := b.Release(ctx, d, "wrong", time.Time{}); e != nil || ok {
		t.Fatal("wrong owner released", e)
	}
	if e := b.AckOwned(ctx, d, "wrong", func(context.Context, contracts.Delivery) (bool, error) { return true, nil }); e == nil {
		t.Fatal("wrong owner acknowledged")
	}
	time.Sleep(350 * time.Millisecond)
	if ok, e := b.Claim(ctx, cs[0], "new", time.Minute); e != nil || !ok {
		t.Fatal("expired lease not recovered", owner, e)
	}
	if ok, e := b.Renew(ctx, d, "new", time.Minute); e != nil || !ok {
		t.Fatal(e)
	}
	if ok, e := b.Release(ctx, d, "new", time.Now().Add(200*time.Millisecond)); e != nil || !ok {
		t.Fatal(e)
	}
	if ok, e := b.Claim(ctx, cs[0], "later", time.Minute); e != nil || ok {
		t.Fatal("retry eligibility ignored", e)
	}
	time.Sleep(250 * time.Millisecond)
	if ok, e := b.Claim(ctx, cs[0], "later", time.Minute); e != nil || !ok {
		t.Fatal(e)
	}
	if e := b.AckOwned(ctx, d, "later", func(context.Context, contracts.Delivery) (bool, error) { return false, nil }); e == nil {
		t.Fatal("uncommitted delivery acknowledged")
	}
	if e := b.AckOwned(ctx, d, "later", func(context.Context, contracts.Delivery) (bool, error) { return true, nil }); e != nil {
		t.Fatal(e)
	}
	cs, e = b.Candidates(ctx, d.Database, 0, 256)
	if e != nil || len(cs) != 0 {
		t.Fatal("ack retained ready entry", e)
	}
}
