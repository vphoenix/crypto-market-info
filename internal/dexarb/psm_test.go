package dexarb

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"testing"
)

func testSky() dex.Sky {
	return dex.Sky{Tin: new(big.Int), Tout: new(big.Int), Buf: new(big.Int), DAICash: new(big.Int).Mul(big.NewInt(1_000_000), factor), USDCCash: big.NewInt(1_000_000), Allowance: dex.Copy(maxUint), Complete: true, IdentityOK: true, VatLive: true, DAIJoinLive: true, DAIJoinWard: true, USDSJoinWard: true}
}
func TestPSMExactFeeAndMaximalBuy(t *testing.T) {
	s := testSky()
	s.Tin = big.NewInt(123456789012345)
	s.Tout = big.NewInt(987654321098765)
	p, e := NewPSM(s)
	if e != nil {
		t.Fatal(e)
	}
	g := big.NewInt(1001)
	out, e := p.Sell(g)
	if e != nil {
		t.Fatal(e)
	}
	base := new(big.Int).Mul(g, factor)
	want := new(big.Int).Sub(base, new(big.Int).Quo(new(big.Int).Mul(base, s.Tin), wad))
	if out.Cmp(want) != 0 {
		t.Fatal("sell fee rounding", out, want)
	}
	for i := int64(0); i < 100; i++ {
		p, _ := NewPSM(s)
		budget := new(big.Int).Add(new(big.Int).Mul(big.NewInt(i), factor), big.NewInt(777))
		out, dust, e := p.Buy(budget)
		if e != nil {
			t.Fatal(e)
		}
		cost, _ := p.buyCost(out)
		next, _ := p.buyCost(new(big.Int).Add(out, big.NewInt(1)))
		if cost.Cmp(budget) > 0 || next.Cmp(budget) <= 0 || new(big.Int).Add(cost, dust).Cmp(budget) != 0 {
			t.Fatal("buy not maximal or dust lost")
		}
	}
	if s.DAICash.Cmp(new(big.Int).Mul(big.NewInt(1_000_000), factor)) != 0 {
		t.Fatal("state aliases source")
	}
}
func TestPSMHaltInventoryAndOverflow(t *testing.T) {
	s := testSky()
	s.Tin = dex.Copy(maxUint)
	p, _ := NewPSM(s)
	if _, e := p.Sell(big.NewInt(1)); !errors.Is(e, ErrHalted) {
		t.Fatal(e)
	}
	s = testSky()
	s.Tout = dex.Copy(maxUint)
	p, _ = NewPSM(s)
	if _, _, e := p.Buy(big.NewInt(1)); !errors.Is(e, ErrHalted) {
		t.Fatal(e)
	}
	s = testSky()
	s.Allowance = big.NewInt(9)
	p, _ = NewPSM(s)
	if _, _, e := p.Buy(new(big.Int).Mul(big.NewInt(10), factor)); !errors.Is(e, ErrInventory) {
		t.Fatal(e)
	}
	p, _ = NewPSM(testSky())
	if _, e := p.Sell(maxUint); !errors.Is(e, ErrArithmetic) {
		t.Fatal(e)
	}
	s = testSky()
	s.USDCCash = dex.Copy(maxUint)
	p, _ = NewPSM(s)
	if _, e := p.Sell(big.NewInt(1)); !errors.Is(e, ErrArithmetic) {
		t.Fatal(e)
	}
}

type identityQuoter struct{}

func (identityQuoter) Quotes(_ context.Context, _ dex.Hash, rr []dex.SwapRequest) []dex.SwapResult {
	out := make([]dex.SwapResult, len(rr))
	for i, r := range rr {
		out[i] = dex.SwapResult{Amount: dex.Copy(r.Amount), SqrtAfter: big.NewInt(1), Gas: big.NewInt(123), Payload: dex.Digest([]byte("quote")), At: dex.Now()}
	}
	return out
}
func TestTwoPSMLegsUseCandidateLocalUpdatedState(t *testing.T) {
	s := testSky()
	s.USDCCash = new(big.Int)
	s.Allowance = big.NewInt(100)
	r := Route{ID: "roundtrip", PreSell: true, PostBuy: true}
	qs := Scan(context.Background(), identityQuoter{}, dex.Anchor{}, s, []Route{r}, []*big.Int{big.NewInt(100), big.NewInt(100)}, dex.Address{})
	for _, q := range qs {
		if q.Status != "ok" || q.AmountOut.Cmp(big.NewInt(100)) != 0 {
			t.Fatal("second PSM leg used original cash or candidates shared allowance", q.Reason)
		}
	}
	if s.USDCCash.Sign() != 0 || s.Allowance.Int64() != 100 {
		t.Fatal("mutated source")
	}
}
