package reserve

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
)

func (r *Reader) AuctionQuotes(ctx context.Context, s State, budgets []*big.Int, limit ...int) ([]Quote, uint32) {
	if !AuctionActive(s) {
		return []Quote{}, 0
	}
	maxPairs := 6
	if len(limit) > 0 {
		maxPairs = max(0, min(6, limit[0]))
	}
	if maxPairs == 0 {
		return []Quote{}, uint32(len(s.Basket) * (len(s.Basket) - 1) * len(budgets))
	}
	type pair struct {
		sell, buy     string
		capacity, bid *big.Int
		reason        string
	}
	all := []pair{}
	calls := []ethereum.Call{}
	maxSell := new(big.Int).Sub(new(big.Int).Lsh(Uint(1), 256), Uint(1))
	for _, a := range s.Basket {
		for _, b := range s.Basket {
			if a.Token == b.Token {
				continue
			}
			all = append(all, pair{sell: a.Token, buy: b.Token})
			calls = append(calls, Call(folioAddress(s), stateHash(s), "getBid", s.AuctionId, ethRaw(a.Token), ethRaw(b.Token), maxSell))
		}
	}
	rr := r.Batch(ctx, calls)
	positive := []pair{}
	unknown := []pair{}
	for i, p := range all {
		v, e := Decode(rr[i], "getBid")
		if e != nil {
			p.reason = e.Error()
			if rr[i].Err == nil || SourceError(r.RPC, rr[i]) != "contract_revert" {
				unknown = append(unknown, p)
			}
			continue
		}
		p.capacity = v[0].(*big.Int)
		p.bid = v[1].(*big.Int)
		if p.capacity.Sign() > 0 && p.bid.Sign() > 0 {
			positive = append(positive, p)
		}
	}
	positive = append(positive, unknown...)
	selected := []pair{}
	if len(positive) > 0 {
		offset := int(s.BlockNumber % uint64(len(positive)))
		for i := 0; i < min(maxPairs, len(positive)); i++ {
			selected = append(selected, positive[(offset+i)%len(positive)])
		}
	}
	skipped := uint32(max(0, len(positive)-len(selected)) * len(budgets))
	quotes := []Quote{}
	legs := []Leg{}
	picks := []pair{}
	for _, p := range selected {
		for _, budget := range budgets {
			q := quoteBase(s, "auction", budget, "auction:"+Hex(p.sell)+":"+Hex(p.buy))
			q.ExpectedLegs = 2
			q.AuctionId = Copy(s.AuctionId)
			q.SellToken = Ptr(p.sell)
			q.BuyToken = Ptr(p.buy)
			q.Reason = p.reason
			quotes = append(quotes, q)
			picks = append(picks, p)
			legs = append(legs, NewLeg(uint16(len(legs)), "entry", "exact_input", Address(USDC), p.buy, budget))
		}
	}
	legs = r.BestLegs(ctx, stateHash(s), legs)
	calls = []ethereum.Call{}
	indices := []int{}
	for i := range quotes {
		q, p := &quotes[i], picks[i]
		if q.Reason != "" {
			continue
		}
		if legs[i].Status != "ok" {
			q.Reason = "buy_token_sizing_failed"
			continue
		}
		requested := Copy(p.capacity)
		if legs[i].AmountOutRaw.Cmp(p.bid) < 0 {
			requested.Mul(requested, legs[i].AmountOutRaw).Div(requested, p.bid)
			requested.Mul(requested, Uint(9999)).Div(requested, Uint(10000))
		}
		q.RequestedMaxSellRaw = Copy(requested)
		if requested.Sign() == 0 {
			q.Reason = "zero_bid_size"
			continue
		}
		calls = append(calls, Call(folioAddress(s), stateHash(s), "getBid", s.AuctionId, ethRaw(p.sell), ethRaw(p.buy), requested))
		indices = append(indices, i)
	}
	rr = r.Batch(ctx, calls)
	legs = []Leg{}
	done := []int{}
	for j, i := range indices {
		q := &quotes[i]
		v, e := Decode(rr[j], "getBid")
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		q.AuctionSellRaw = v[0].(*big.Int)
		q.AuctionBuyRaw = v[1].(*big.Int)
		q.AuctionPriceD27 = v[2].(*big.Int)
		if q.AuctionSellRaw.Sign() == 0 || q.AuctionBuyRaw.Sign() == 0 {
			q.Reason = "zero_bid_capacity"
			continue
		}
		done = append(done, i)
		legs = append(legs, NewLeg(0, "entry", "exact_output", Address(USDC), *q.BuyToken, q.AuctionBuyRaw), NewLeg(1, "exit", "exact_input", *q.SellToken, Address(USDC), q.AuctionSellRaw))
	}
	legs = r.BestLegs(ctx, stateHash(s), legs)
	for j, i := range done {
		q := &quotes[i]
		part := legs[2*j : 2*j+2]
		AddLegs(q, part)
		if !CompleteLegs(part) {
			q.Reason = "auction_dex_leg_failed"
			continue
		}
		q.AmountInRaw = Copy(part[0].AmountInRaw)
		q.AmountOutRaw = Copy(part[1].AmountOutRaw)
	}
	return quotes, skipped
}
