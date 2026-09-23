package dexarb

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/dex"
)

// CostCache lives only for one report. Hash + full request are part of the key;
// neither latest state nor a failed quote can replace a historical observation.
type CostCache struct {
	RPC    CostRPC
	quotes map[dex.Hash]dex.SwapResult
}

// Canonical membership is mutable for head blocks, unlike their quoted state.
// Always recheck it even when the amount quote is already cached.
func (c *CostCache) Canonical(ctx context.Context, b dex.Block) error { return c.RPC.Canonical(ctx, b) }
func (c *CostCache) Quotes(ctx context.Context, h dex.Hash, rr []dex.SwapRequest) []dex.SwapResult {
	if c.quotes == nil {
		c.quotes = map[dex.Hash]dex.SwapResult{}
	}
	out := make([]dex.SwapResult, len(rr))
	var pending []dex.SwapRequest
	var indexes []int
	keys := make([]dex.Hash, len(rr))
	for i, r := range rr {
		keys[i] = dex.ObjectHash(struct {
			Block   dex.Hash
			Request dex.SwapRequest
		}{h, r})
		if v, ok := c.quotes[keys[i]]; ok {
			out[i] = v
		} else {
			pending = append(pending, r)
			indexes = append(indexes, i)
		}
	}
	if len(pending) > 0 {
		vv := c.RPC.Quotes(ctx, h, pending)
		for j, i := range indexes {
			if j < len(vv) {
				out[i] = vv[j]
				c.quotes[keys[i]] = vv[j]
			}
		}
	}
	return out
}
