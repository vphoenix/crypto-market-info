package dexarb

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
)

type Route struct {
	ID                                        string
	Fee                                       uint32
	In, Out                                   dex.Address
	PreSell, PreWrapper, PostBuy, PostWrapper bool
}
type Quoter interface {
	Quotes(context.Context, dex.Hash, []dex.SwapRequest) []dex.SwapResult
}

func Scan(ctx context.Context, provider Quoter, anchor dex.Anchor, sky dex.Sky, routes []Route, sizes []*big.Int, usdc dex.Address) []dex.Quote {
	type pending struct {
		index int
		state *PSM
		route Route
	}
	var out []dex.Quote
	var work []pending
	var requests []dex.SwapRequest
	for _, r := range routes {
		for _, size := range sizes {
			q := dex.Quote{Anchor: anchor, Role: "strategy", Route: r.ID, Mode: "exact_in", TokenIn: usdc, TokenOut: usdc, Requested: dex.Copy(size), V3InToken: r.In, V3OutToken: r.Out, Status: "unknown", Reason: "sky_state_incomplete", AvailableAt: dex.Now()}
			q.Payload = sky.Payload
			q.SetID()
			state, e := NewPSM(sky)
			in := dex.Copy(size)
			if e == nil && (r.PreWrapper || r.PostWrapper) && (!sky.VatLive || !sky.DAIJoinLive || !sky.DAIJoinWard || !sky.USDSJoinWard) {
				e = errors.New("sky_conversion_dependency_disabled")
			}
			if e == nil && r.PreSell {
				in, e = state.Sell(in)
			}
			if e != nil {
				q.Reason = e.Error()
			} else {
				q.V3In = dex.Copy(in)
				work = append(work, pending{len(out), state, r})
				requests = append(requests, dex.SwapRequest{TokenIn: r.In, TokenOut: r.Out, Fee: r.Fee, Amount: in})
			}
			out = append(out, q)
		}
	}
	results := provider.Quotes(ctx, anchor.Hash, requests)
	for i, w := range work {
		q := &out[w.index]
		if i >= len(results) {
			q.Reason = "quoter_batch_incomplete"
			continue
		}
		v := results[i]
		q.Payload = v.Payload
		q.AvailableAt = v.At
		if v.Err != nil {
			q.Reason = v.Err.Error()
			continue
		}
		if !dex.ValidUint(v.Amount, 256) {
			q.Reason = "invalid_quoter_amount"
			continue
		}
		q.V3Out = dex.Copy(v.Amount)
		q.SqrtAfter = dex.Copy(v.SqrtAfter)
		q.GasEstimate = dex.Copy(v.Gas)
		q.TicksCrossed = v.Ticks
		amount := dex.Copy(v.Amount)
		dust := new(big.Int)
		var e error
		if w.route.PostBuy {
			amount, dust, e = w.state.Buy(amount)
		}
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		q.DustDAI = new(big.Int)
		q.DustUSDS = new(big.Int)
		if w.route.PostWrapper {
			q.DustUSDS = dust
		} else {
			q.DustDAI = dust
		}
		q.AmountIn = dex.Copy(q.Requested)
		q.AmountOut = amount
		q.Status = "ok"
		q.Reason = ""
	}
	return out
}
