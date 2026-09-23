package dexarb

import (
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
)

var wad = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
var factor = new(big.Int).Exp(big.NewInt(10), big.NewInt(12), nil)
var maxUint = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
var ErrHalted = errors.New("psm_direction_halted")
var ErrInventory = errors.New("psm_inventory_or_allowance")
var ErrArithmetic = errors.New("uint256_arithmetic")
var ErrState = errors.New("sky_state_incomplete_or_unsupported")

type PSM struct{ Tin, Tout, DAI, USDC, Allowance *big.Int }

func NewPSM(s dex.Sky) (*PSM, error) {
	for _, n := range []*big.Int{s.Tin, s.Tout, s.DAICash, s.USDCCash, s.Allowance} {
		if !dex.ValidUint(n, 256) {
			return nil, ErrState
		}
	}
	if !s.Complete || !s.IdentityOK {
		return nil, ErrState
	}
	return &PSM{dex.Copy(s.Tin), dex.Copy(s.Tout), dex.Copy(s.DAICash), dex.Copy(s.USDCCash), dex.Copy(s.Allowance)}, nil
}
func mul(a, b *big.Int) (*big.Int, error) {
	n := new(big.Int).Mul(a, b)
	if !dex.ValidUint(n, 256) {
		return nil, ErrArithmetic
	}
	return n, nil
}
func add(a, b *big.Int) (*big.Int, error) {
	n := new(big.Int).Add(a, b)
	if !dex.ValidUint(n, 256) {
		return nil, ErrArithmetic
	}
	return n, nil
}
func feeAmount(g, fee *big.Int) (base, charge *big.Int, err error) {
	if !dex.ValidUint(g, 256) || !dex.ValidUint(fee, 256) {
		return nil, nil, ErrArithmetic
	}
	if fee.Cmp(maxUint) == 0 {
		return nil, nil, ErrHalted
	}
	if fee.Cmp(wad) > 0 {
		return nil, nil, ErrState
	}
	base, err = mul(g, factor)
	if err != nil {
		return
	}
	charge, err = mul(base, fee)
	if err == nil {
		charge.Quo(charge, wad)
	}
	return
}

// Sell deposits USDC into Pocket and transfers pre-minted DAI. No fill or NoFee entry.
func (p *PSM) Sell(g *big.Int) (*big.Int, error) {
	base, fee, e := feeAmount(g, p.Tin)
	if e != nil {
		return nil, e
	}
	out := new(big.Int).Sub(base, fee)
	if out.Cmp(p.DAI) > 0 {
		return nil, ErrInventory
	}
	cash, e := add(p.USDC, g)
	if e != nil {
		return nil, e
	}
	p.DAI.Sub(p.DAI, out)
	p.USDC = cash
	return out, nil
}
func (p *PSM) buyCost(g *big.Int) (*big.Int, error) {
	b, f, e := feeAmount(g, p.Tout)
	if e != nil {
		return nil, e
	}
	return add(b, f)
}

// Buy spends the maximum representable USDC amount for a DAI budget. The dust
// remains outside the PSM and is never valued as profit.
func (p *PSM) Buy(budget *big.Int) (out, dust *big.Int, err error) {
	if !dex.ValidUint(budget, 256) {
		return nil, nil, ErrArithmetic
	}
	if p.Tout.Cmp(maxUint) == 0 {
		return nil, nil, ErrHalted
	}
	if p.Tout.Cmp(wad) > 0 {
		return nil, nil, ErrState
	}
	lo := new(big.Int)
	hi := new(big.Int).Quo(budget, factor)
	for lo.Cmp(hi) < 0 {
		mid := new(big.Int).Add(lo, hi)
		mid.Add(mid, big.NewInt(1))
		mid.Rsh(mid, 1)
		cost, e := p.buyCost(mid)
		if e != nil || cost.Cmp(budget) > 0 {
			hi.Sub(mid, big.NewInt(1))
		} else {
			lo.Set(mid)
		}
	}
	cost, e := p.buyCost(lo)
	if e != nil {
		return nil, nil, e
	}
	if lo.Cmp(p.USDC) > 0 || lo.Cmp(p.Allowance) > 0 {
		return nil, nil, ErrInventory
	}
	cash, e := add(p.DAI, cost)
	if e != nil {
		return nil, nil, e
	}
	p.DAI = cash
	p.USDC.Sub(p.USDC, lo)
	if p.Allowance.Cmp(maxUint) != 0 {
		p.Allowance.Sub(p.Allowance, lo)
	}
	return lo, new(big.Int).Sub(budget, cost), nil
}
