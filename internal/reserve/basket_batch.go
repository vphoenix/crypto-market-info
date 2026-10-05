package reserve

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
)

// BasketQuotes executes the independent paths in stages so every configured
// size receives the same deadline opportunity. No quotes from different state
// anchors or failed attempts are stitched together.
func (r *Reader) BasketQuotes(ctx context.Context, s State, budgets []*big.Int) []Quote {
	n := len(budgets)
	redeems, mints := make([]Quote, n), make([]Quote, n)
	entries := []Leg{}
	for i, b := range budgets {
		redeems[i] = quoteBase(s, "redeem", b, "redeem")
		mints[i] = quoteBase(s, "mint", b, "mint")
		redeems[i].ExpectedLegs = uint16(len(s.Basket) + 1)
		mints[i].ExpectedLegs = uint16(len(s.Basket) + 1)
		entries = append(entries, NewLeg(uint16(i), "entry", "exact_input", Address(USDC), s.Folio, b))
	}
	if !Usable(s) {
		out := []Quote{}
		reason := "protocol_state_unusable"
		if s.Reason != "" {
			reason += ":" + s.Reason
		}
		if !s.StateComplete {
			reason = "state_incomplete:" + s.Reason
		} else if s.SyncStateChangeActive != nil && *s.SyncStateChangeActive || s.AsyncStateChangeActive != nil && *s.AsyncStateChangeActive {
			reason = "protocol_state_change_active"
		}
		for i := range budgets {
			redeems[i].Reason = reason
			mints[i].Reason = reason
			out = append(out, redeems[i], mints[i])
		}
		return out
	}
	entries = r.BestLegs(ctx, stateHash(s), entries)
	calls := []ethereum.Call{}
	type mapping struct {
		index int
		mint  bool
	}
	maps := []mapping{}
	for i, l := range entries {
		AddLegs(&redeems[i], []Leg{l})
		if l.Status != "ok" {
			redeems[i].Reason = "share_entry_failed"
			mints[i].Reason = "share_sizing_unavailable"
			continue
		}
		redeems[i].GrossSharesRaw = Copy(l.AmountOutRaw)
		redeems[i].AmountInRaw = Copy(l.AmountInRaw)
		mints[i].GrossSharesRaw = Copy(l.AmountOutRaw)
		calls = append(calls, Call(folioAddress(s), stateHash(s), "toAssets", l.AmountOutRaw, uint8(0)))
		maps = append(maps, mapping{i, false})
		if MintAllowed(s) {
			calls = append(calls, Call(folioAddress(s), stateHash(s), "toAssets", l.AmountOutRaw, uint8(1)))
			maps = append(maps, mapping{i, true})
		} else {
			mints[i].Reason = "mint_deprecated"
		}
	}
	rr := r.Batch(ctx, calls)
	for j, m := range maps {
		q := &redeems[m.index]
		if m.mint {
			q = &mints[m.index]
		}
		v, e := Decode(rr[j], "toAssets")
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		tokens, amounts := v[0].([]common.Address), v[1].([]*big.Int)
		if len(tokens) != len(s.Basket) || len(amounts) != len(tokens) {
			q.Reason = "to_assets_basket_mismatch"
			continue
		}
		for i, a := range tokens {
			if Address(Addr(a)) != s.Basket[i].Token {
				q.Reason = "to_assets_order_mismatch"
				break
			}
			q.BasketAmounts = append(q.BasketAmounts, Amount{Address(Addr(a)), amounts[i]})
		}
	}
	type group struct {
		index, start, end int
		mint              bool
	}
	groups := []group{}
	legs := []Leg{}
	for i := range budgets {
		for _, mint := range []bool{false, true} {
			q := &redeems[i]
			if mint {
				q = &mints[i]
			}
			if q.Reason != "" || len(q.BasketAmounts) == 0 {
				continue
			}
			start := len(legs)
			legs = append(legs, basketLegs(q.BasketAmounts, mint)...)
			groups = append(groups, group{i, start, len(legs), mint})
		}
	}
	legs = r.BestLegs(ctx, stateHash(s), legs)
	resize := []int{}
	for _, g := range groups {
		q := &redeems[g.index]
		if g.mint {
			q = &mints[g.index]
		}
		part := legs[g.start:g.end]
		if !CompleteLegs(part) {
			AddLegs(q, part)
			q.Reason = "basket_leg_failed"
			continue
		}
		if !g.mint {
			AddLegs(q, part)
			q.AmountOutRaw, _ = LegSum(part, false)
			q.ExpectedLegs = uint16(len(q.DexLegs))
			continue
		}
		cost, e := LegSum(part, true)
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		if cost.Cmp(budgets[g.index]) > 0 {
			q.GrossSharesRaw.Mul(q.GrossSharesRaw, budgets[g.index]).Div(q.GrossSharesRaw, cost)
			q.GrossSharesRaw.Mul(q.GrossSharesRaw, Uint(9999)).Div(q.GrossSharesRaw, Uint(10000))
			resize = append(resize, g.index)
		} else {
			AddLegs(q, part)
			q.AmountInRaw = cost
		}
	}
	// Only sizes that exceeded their budget get one further deterministic sizing
	// round. A residual overspend is kept as incomplete, never relabelled smaller.
	calls = []ethereum.Call{}
	for _, i := range resize {
		calls = append(calls, Call(folioAddress(s), stateHash(s), "toAssets", mints[i].GrossSharesRaw, uint8(1)))
	}
	rr = r.Batch(ctx, calls)
	groups = []group{}
	legs = []Leg{}
	for j, i := range resize {
		q := &mints[i]
		v, e := Decode(rr[j], "toAssets")
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		tokens, amounts := v[0].([]common.Address), v[1].([]*big.Int)
		if len(tokens) != len(s.Basket) || len(tokens) != len(amounts) {
			q.Reason = "resized_basket_mismatch"
			continue
		}
		q.BasketAmounts = []Amount{}
		for k, a := range tokens {
			if Address(Addr(a)) != s.Basket[k].Token {
				q.Reason = "resized_basket_order"
				break
			}
			q.BasketAmounts = append(q.BasketAmounts, Amount{Address(Addr(a)), amounts[k]})
		}
		if q.Reason != "" {
			continue
		}
		start := len(legs)
		legs = append(legs, basketLegs(q.BasketAmounts, true)...)
		groups = append(groups, group{i, start, len(legs), true})
	}
	legs = r.BestLegs(ctx, stateHash(s), legs)
	for _, g := range groups {
		q := &mints[g.index]
		part := legs[g.start:g.end]
		AddLegs(q, part)
		if !CompleteLegs(part) {
			q.Reason = "resized_basket_entry_failed"
			continue
		}
		cost, e := LegSum(part, true)
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		q.AmountInRaw = cost
		if cost.Cmp(budgets[g.index]) > 0 {
			q.Reason = "budget_sizing_failed"
		}
	}
	legs = []Leg{}
	indices := []int{}
	for i := range mints {
		q := &mints[i]
		if q.Reason != "" || q.AmountInRaw == nil {
			continue
		}
		fee, e := MintFee(q.GrossSharesRaw, s.MintFeeD18, s.DaoFeeNumerator, s.DaoFeeDenominator, s.DaoFeeFloorD18)
		if e != nil {
			q.Reason = e.Error()
			continue
		}
		q.FeeSharesRaw = fee
		q.NetSharesRaw = new(big.Int).Sub(q.GrossSharesRaw, fee)
		if q.NetSharesRaw.Sign() == 0 {
			q.Reason = "zero_net_shares"
			continue
		}
		indices = append(indices, i)
		legs = append(legs, NewLeg(uint16(i), "exit", "exact_input", s.Folio, Address(USDC), q.NetSharesRaw))
	}
	legs = r.BestLegs(ctx, stateHash(s), legs)
	for j, i := range indices {
		q := &mints[i]
		AddLegs(q, []Leg{legs[j]})
		if legs[j].Status != "ok" {
			q.Reason = "share_exit_failed"
			continue
		}
		q.AmountOutRaw = Copy(legs[j].AmountOutRaw)
		q.ExpectedLegs = uint16(len(q.DexLegs))
	}
	out := []Quote{}
	for i := range budgets {
		out = append(out, redeems[i], mints[i])
	}
	return out
}
