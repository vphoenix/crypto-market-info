package reserve

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"sort"
)

func (c *Collector) SimulateBatch(ctx context.Context, head dex.Block, b Batch, limit int, progress func(string)) error {
	store, ok := c.Store.(SimulationStore)
	if !ok {
		return errors.New("simulation_store_unavailable")
	}
	candidates := []Quote{}
	for _, q := range b.Quotes {
		if q.Status == "ok" && (q.RouteKind == "mint" || q.RouteKind == "redeem" || q.RouteKind == "auction") {
			candidates = append(candidates, q)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if v := candidates[i].RequestedBudgetRaw.Cmp(candidates[j].RequestedBudgetRaw); v != 0 {
			return v < 0
		}
		return candidates[i].RouteKind < candidates[j].RouteKind
	})
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, q := range candidates {
		s, e := c.SimulateRoute(ctx, head, q)
		if e != nil {
			return e
		}
		if e = store.WriteReserveSimulation(ctx, s); e != nil {
			return e
		}
		progress(fmt.Sprintf("simulation block=%d kind=%s status=%s reason=%s evidence=%s", s.BlockNumber, s.RouteKind, s.Status, s.Reason, Hex(s.PayloadHash)))
		if e = c.RPC.WaitReady(ctx); e != nil {
			return e
		}
	}
	return nil
}
