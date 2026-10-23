package optionslive

import (
	"fmt"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func TestFailedScopeInvalidatesEachOwnedMemberOnceWithoutFloodingIngress(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("all=%t", all), func(t *testing.T) {
			budget := &exchange.BufferBudget{Limit: 64 << 20}
			s := &catalogSupervisor{byID: map[uint32]*catalogWorker{}}
			scope := deribit.Scope{Currency: "BTC", Kind: "option"}
			if all {
				scope = deribit.Scope{}
			}
			var workers []*catalogWorker
			want := map[uint32]bool{}
			// A full BTC shard, a full ETH shard, and a mixed shard. Existing
			// book traffic must survive invalidation of only the failed scope.
			for group := 0; group < 3; group++ {
				w := &catalogWorker{q: newIngress(time.Now())}
				w.q.budget = budget
				for n := 0; n < 32; n++ {
					id := uint32(group*32 + n + 1)
					currency := "BTC"
					if group == 1 || group == 2 && n >= 16 {
						currency = "ETH"
					}
					var spec options.ContractSpec
					spec.Instrument.ID = id
					spec.Instrument.ExchangeSymbol = fmt.Sprintf("%s-30OCT26-%d-C", currency, 90000+n)
					w.specs = append(w.specs, spec)
					s.byID[id] = w
					if all || currency == "BTC" {
						want[id] = true
					}
				}
				for n := 0; n < 128; n++ {
					if err := w.q.offer(event{Group: "existing book traffic"}); err != nil {
						t.Fatal(err)
					}
				}
				workers = append(workers, w)
			}
			s.markScopeUnknown(scope)
			if !s.uncertainScopes[scope.String()] || s.scopeGenerations[scope.String()] != 1 {
				t.Fatal("failed scope did not retain its confirmation barrier")
			}
			seen := map[uint32]int{}
			for _, w := range workers {
				if w.q.failed {
					t.Fatal("scope invalidation flooded a live shard's ingress")
				}
				traffic := 0
				for len(w.q.queue) > 0 {
					v := <-w.q.queue
					w.q.consumed(v)
					if v.Group == "existing book traffic" {
						traffic++
					}
					if v.State != nil {
						if v.State.Known || v.State.Open || !want[v.State.InstrumentID] || s.byID[v.State.InstrumentID] != w {
							t.Fatalf("wrong invalidation: %+v", v.State)
						}
						seen[v.State.InstrumentID]++
					}
				}
				if traffic != 128 {
					t.Fatalf("existing traffic lost: %d", traffic)
				}
			}
			for id := range want {
				if seen[id] != 1 {
					t.Fatalf("member %d invalidated %d times", id, seen[id])
				}
			}
			if budget.Used() != 0 {
				t.Fatal("invalidation leaked ingress reservations")
			}
		})
	}
}
