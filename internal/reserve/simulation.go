package reserve

import (
	"context"
	_ "embed"
	"encoding/hex"
	"errors"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
	"strings"
	"time"
)

//go:embed simulator/probe-abi.json
var probeABIJSON string

//go:embed simulator/probe-runtime.hex
var probeRuntime string

//go:embed simulator/RouteProbe.sol
var probeSource string

//go:embed simulator/compiler.json
var probeCompiler string
var probeABI = func() abi.ABI {
	v, e := abi.JSON(strings.NewReader(probeABIJSON))
	if e != nil {
		panic(e)
	}
	return v
}()
var probeActor = dex.MustAddress("0x0000000000000000000000000000000000c0ffee")
var swapRouter = dex.MustAddress("0xe592427a0aece92de3edee1f18e0157c05861564")
var fundingRaw = Uint(1_000_000_000_000) // 1,000,000 synthetic USDC, never real funds.

type Simulation struct {
	ChainId       uint64    `ch:"chain_id"`
	ManifestHash  string    `ch:"manifest_hash"`
	QuoteId       string    `ch:"quote_id"`
	SimulationId  string    `ch:"simulation_id"`
	BlockNumber   uint64    `ch:"block_number"`
	BlockHash     string    `ch:"block_hash"`
	BlockTime     time.Time `ch:"block_time"`
	AvailableAt   time.Time `ch:"available_at"`
	Folio         string    `ch:"folio"`
	RouteKind     string    `ch:"route_kind"`
	BudgetRaw     *big.Int  `ch:"budget_raw"`
	FundedRaw     *big.Int  `ch:"funded_raw"`
	AmountInRaw   *big.Int  `ch:"amount_in_raw"`
	AmountOutRaw  *big.Int  `ch:"amount_out_raw"`
	SharesRaw     *big.Int  `ch:"shares_raw"`
	GasInternal   *uint64   `ch:"gas_internal"`
	WithinBudget  *bool     `ch:"within_budget"`
	Status        string    `ch:"status"`
	Reason        string    `ch:"reason"`
	SimulatorHash string    `ch:"simulator_hash"`
	PayloadHash   string    `ch:"payload_hash"`
}
type SimulationStore interface {
	WriteReserveSimulation(context.Context, Simulation) error
	ReserveSimulations(context.Context, string) ([]Simulation, error)
}

type ProbePlan struct {
	Folio                 common.Address
	Kind                  uint8
	Shares, MinShares     *big.Int
	Assets                []common.Address
	AssetPaths            [][]byte
	Amounts               []*big.Int
	SharePath             []byte
	AuctionId             *big.Int
	SellToken, BuyToken   common.Address
	SellAmount, BuyAmount *big.Int
}

func selectedPath(m Manifest, l Leg, reverse bool) ([]byte, error) {
	if l.Status != "ok" {
		return nil, errors.New("simulation_incomplete_leg")
	}
	if l.TokenIn == l.TokenOut {
		return []byte{}, nil
	}
	reader := Reader{Manifest: m}
	for _, p := range reader.Paths(l.TokenIn, l.TokenOut) {
		tokens := []string{}
		pools := []string{}
		for _, v := range p.Tokens {
			tokens = append(tokens, Address(v))
		}
		for _, v := range p.Pools {
			pools = append(pools, Address(v))
		}
		if ID(tokens) == ID(l.PathTokens) && ID(pools) == ID(l.PathPools) && ID(p.Fees) == ID(l.PoolFees) {
			return PathBytes(p, reverse), nil
		}
	}
	return nil, errors.New("simulation_path_identity_mismatch")
}
func planFor(m Manifest, q Quote) (ProbePlan, error) {
	p := ProbePlan{Folio: common.BytesToAddress([]byte(q.Folio)), Shares: Uint(0), MinShares: Uint(0), AuctionId: Uint(0), SellAmount: Uint(0), BuyAmount: Uint(0), Assets: []common.Address{}, AssetPaths: [][]byte{}, Amounts: []*big.Int{}, SharePath: []byte{}}
	if q.Status != "ok" || q.Quality == "incomplete" || !CompleteLegs(q.DexLegs) {
		return p, errors.New("simulation_requires_complete_quote")
	}
	var e error
	switch q.RouteKind {
	case "mint", "redeem":
		if !Uint256(q.GrossSharesRaw) || q.GrossSharesRaw.Sign() == 0 {
			return p, errors.New("simulation_invalid_shares")
		}
		p.Shares = Copy(q.GrossSharesRaw)
		if q.RouteKind == "mint" {
			p.Kind = 0
			if !Uint256(q.NetSharesRaw) || q.NetSharesRaw.Sign() == 0 {
				return p, errors.New("simulation_invalid_net_shares")
			}
			p.MinShares = Copy(q.NetSharesRaw)
		} else {
			p.Kind = 1
		}
		seen := map[string]bool{}
		for _, a := range q.BasketAmounts {
			if seen[a.Token] || len(a.Token) != 20 || !Uint256(a.AmountRaw) {
				return p, errors.New("simulation_invalid_basket")
			}
			seen[a.Token] = true
			p.Assets = append(p.Assets, common.BytesToAddress([]byte(a.Token)))
			p.Amounts = append(p.Amounts, Copy(a.AmountRaw))
			path := []byte{}
			found := a.Token == Address(USDC) || a.AmountRaw.Sign() == 0
			for _, l := range q.DexLegs {
				if q.RouteKind == "mint" && l.Side == "entry" && l.TokenOut == a.Token || q.RouteKind == "redeem" && l.Side == "exit" && l.TokenIn == a.Token {
					path, e = selectedPath(m, l, q.RouteKind == "mint")
					if e != nil {
						return p, e
					}
					found = true
					break
				}
			}
			if !found {
				return p, errors.New("simulation_missing_asset_path")
			}
			p.AssetPaths = append(p.AssetPaths, path)
		}
		found := false
		for _, l := range q.DexLegs {
			if q.RouteKind == "mint" && l.Side == "exit" && l.TokenIn == q.Folio || q.RouteKind == "redeem" && l.Side == "entry" && l.TokenOut == q.Folio {
				p.SharePath, e = selectedPath(m, l, q.RouteKind == "redeem")
				if e != nil {
					return p, e
				}
				found = true
				break
			}
		}
		if !found || len(p.SharePath) == 0 {
			return p, errors.New("simulation_missing_share_path")
		}
	case "auction":
		p.Kind = 2
		if q.BuyToken == nil || q.SellToken == nil || !Uint256(q.AuctionId) || !Uint256(q.AuctionBuyRaw) || !Uint256(q.AuctionSellRaw) {
			return p, errors.New("simulation_invalid_bid")
		}
		p.AuctionId = Copy(q.AuctionId)
		p.BuyToken = common.BytesToAddress([]byte(*q.BuyToken))
		p.SellToken = common.BytesToAddress([]byte(*q.SellToken))
		p.BuyAmount = Copy(q.AuctionBuyRaw)
		p.SellAmount = Copy(q.AuctionSellRaw)
		p.Assets = []common.Address{p.BuyToken, p.SellToken}
		p.Amounts = []*big.Int{p.BuyAmount, p.SellAmount}
		p.AssetPaths = make([][]byte, 2)
		for _, l := range q.DexLegs {
			idx := 0
			if l.Side == "exit" {
				idx = 1
			}
			p.AssetPaths[idx], e = selectedPath(m, l, idx == 0)
			if e != nil {
				return p, e
			}
		}
	default:
		return p, errors.New("simulation_unsupported_route")
	}
	return p, nil
}
func balanceSlot(a dex.Address) string {
	key := make([]byte, 64)
	copy(key[12:32], a[:])
	key[63] = 9
	return crypto.Keccak256Hash(key).Hex()
}
func viewCall(a dex.Address, h dex.Hash, name string, args ...common.Address) ethereum.Call {
	sig := name + "()"
	if len(args) > 0 {
		sig = name + "(address)"
	}
	data := append([]byte{}, crypto.Keccak256([]byte(sig))[:4]...)
	for _, v := range args {
		data = append(data, common.LeftPadBytes(v[:], 32)...)
	}
	return ethereum.EthCall(a, "0x"+hex.EncodeToString(data), h)
}

// SimulateRoute validates the exact selected paths, then executes swaps and the
// actual Folio call sequentially in one discarded EVM state. Only actor runtime
// and the actor's USDC balance are overridden; pool/protocol state is untouched.
func (c *Collector) SimulateRoute(ctx context.Context, b dex.Block, q Quote) (Simulation, error) {
	runtime, e := hex.DecodeString(strings.TrimSpace(probeRuntime))
	if e != nil {
		return Simulation{}, e
	}
	simulatorHash := dex.Digest(runtime)
	s := Simulation{ChainId: 1, ManifestHash: q.ManifestHash, QuoteId: q.QuoteId, BlockNumber: q.BlockNumber, BlockHash: q.BlockHash, BlockTime: q.BlockTime, AvailableAt: dex.Now(), Folio: q.Folio, RouteKind: q.RouteKind, BudgetRaw: Copy(q.RequestedBudgetRaw), FundedRaw: Copy(fundingRaw), Status: "failed", SimulatorHash: Hash(simulatorHash)}
	r := NewReader(c.RPC, c.Manifest)
	r.Limit = 100
	for _, artifact := range [][]byte{runtime, []byte(probeABIJSON), []byte(probeSource), []byte(probeCompiler)} {
		h, e := c.RPC.Archive.Put(artifact)
		if e != nil {
			return s, e
		}
		r.Members = append(r.Members, h)
	}
	finish := func(reason string) (Simulation, error) {
		s.Reason = reason
		s.AvailableAt = dex.Now()
		h, e := r.Proof(struct {
			Quote        string
			QuotePayload string
			Simulator    dex.Hash
			Actor        dex.Address
			Funded       *big.Int
			Reason       string
		}{Hex(q.QuoteId), Hex(q.PayloadHash), simulatorHash, probeActor, fundingRaw, reason})
		if e != nil {
			return s, e
		}
		s.PayloadHash = h
		s.SimulationId = ID(struct{ Quote, Payload string }{q.QuoteId, h})
		return s, ValidateSimulation(s)
	}
	if q.ManifestHash != Hash(c.Manifest.Hash) || q.BlockHash != Hash(b.Hash) || q.BlockNumber != b.Number {
		return finish("simulation_anchor_mismatch")
	}
	plan, e := planFor(c.Manifest, q)
	if e != nil {
		return finish(e.Error())
	}
	// Verify the USDC balance mapping empirically at this exact hash against a
	// funded public pool. A zero==zero comparison cannot certify the slot.
	pool := dex.MustAddress("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640")
	checks := r.Batch(ctx, []ethereum.Call{
		{Method: "eth_getCode", Params: []any{probeActor.String(), ethereum.BlockRef(b.Hash)}},
		viewCall(USDC, b.Hash, "balanceOf", Eth(pool)),
		{Method: "eth_getStorageAt", Params: []any{USDC.String(), balanceSlot(pool), ethereum.BlockRef(b.Hash)}},
		viewCall(swapRouter, b.Hash, "factory"), viewCall(swapRouter, b.Hash, "WETH9"),
		{Method: "eth_getCode", Params: []any{swapRouter.String(), ethereum.BlockRef(b.Hash)}},
		{Method: "eth_getStorageAt", Params: []any{USDC.String(), balanceSlot(probeActor), ethereum.BlockRef(b.Hash)}},
	})
	vals := make([][]byte, len(checks))
	for i, v := range checks {
		vals[i], e = ethereum.HexResult(v)
		if e != nil {
			reason := SourceError(c.RPC, v)
			if reason == "" {
				reason = e.Error()
			}
			return finish(reason)
		}
	}
	if len(vals[0]) != 0 || len(vals[1]) != 32 || new(big.Int).SetBytes(vals[1]).Sign() == 0 || string(vals[1]) != string(vals[2]) || len(vals[6]) != 32 || new(big.Int).SetBytes(vals[6]).Sign() != 0 {
		return finish("simulation_actor_or_usdc_slot_unverified")
	}
	if len(vals[3]) != 32 || string(vals[3][12:]) != Address(c.Manifest.Factory) || len(vals[4]) != 32 || string(vals[4][12:]) != Address(WETH) || len(vals[5]) == 0 {
		return finish("simulation_router_identity_unverified")
	}
	data, e := probeABI.Pack("execute", plan)
	if e != nil {
		return finish("simulation_plan_encoding_failed")
	}
	overrides := map[string]any{
		probeActor.String(): map[string]any{"code": "0x" + hex.EncodeToString(runtime)},
		USDC.String():       map[string]any{"stateDiff": map[string]string{balanceSlot(probeActor): "0x" + common.Bytes2Hex(common.LeftPadBytes(fundingRaw.Bytes(), 32))}},
	}
	v := r.Batch(ctx, []ethereum.Call{{Method: "eth_call", Params: []any{map[string]string{"from": probeActor.String(), "to": probeActor.String(), "data": "0x" + hex.EncodeToString(data), "gas": "0x7a1200"}, ethereum.BlockRef(b.Hash), overrides}}})[0]
	if v.Err != nil {
		return finish(SourceError(c.RPC, v))
	}
	raw, e := ethereum.HexResult(v)
	if e != nil {
		return finish(e.Error())
	}
	values, e := probeABI.Unpack("execute", raw)
	if e != nil {
		return finish("simulation_response_encoding")
	}
	canonical, e := probeABI.Methods["execute"].Outputs.Pack(values...)
	if e != nil || string(canonical) != string(raw) {
		return finish("simulation_noncanonical_response")
	}
	s.AmountInRaw = values[0].(*big.Int)
	s.AmountOutRaw = values[1].(*big.Int)
	s.SharesRaw = values[2].(*big.Int)
	gas := values[3].(*big.Int)
	if gas.BitLen() > 64 {
		return finish("simulation_gas_overflow")
	}
	s.GasInternal = Ptr(gas.Uint64())
	s.WithinBudget = Ptr(s.AmountInRaw.Cmp(q.RequestedBudgetRaw) <= 0)
	if !*s.WithinBudget {
		s.Reason = "budget_exceeded"
	}
	checked, e := c.Header(ctx, ethereum.Height(b.Number))
	if e != nil {
		return finish("simulation_canonical_check_failed:" + e.Error())
	}
	r.Members = append(r.Members, checked.Payload)
	if checked.Hash != b.Hash {
		return finish("simulation_block_no_longer_canonical")
	}
	s.Status = "joint_call_simulated"
	reason := s.Reason
	return finish(reason)
}
func ValidateSimulation(s Simulation) error {
	if s.ChainId != 1 || len(s.ManifestHash) != 32 || len(s.QuoteId) != 32 || len(s.SimulationId) != 32 || len(s.BlockHash) != 32 || len(s.PayloadHash) != 32 || len(s.SimulatorHash) != 32 || len(s.Folio) != 20 || s.BlockTime.IsZero() || s.AvailableAt.IsZero() || !Uint256(s.BudgetRaw) || !Uint256(s.FundedRaw) {
		return errors.New("invalid_simulation_identity")
	}
	if s.SimulationId != ID(struct{ Quote, Payload string }{s.QuoteId, s.PayloadHash}) {
		return errors.New("simulation_identity_digest_mismatch")
	}
	if s.RouteKind != "mint" && s.RouteKind != "redeem" && s.RouteKind != "auction" || s.Status != "failed" && s.Status != "joint_call_simulated" {
		return errors.New("invalid_simulation_status_or_route")
	}
	for _, value := range []*big.Int{s.AmountInRaw, s.AmountOutRaw, s.SharesRaw} {
		if value != nil && !Uint256(value) {
			return errors.New("invalid_simulation_amount")
		}
	}
	if s.Status == "failed" && s.Reason == "" {
		return errors.New("simulation_failure_reason_missing")
	}
	if s.Status == "joint_call_simulated" && (s.AmountInRaw == nil || s.AmountOutRaw == nil || s.SharesRaw == nil || s.GasInternal == nil || s.WithinBudget == nil) {
		return errors.New("incomplete_simulation")
	}
	if s.Status == "joint_call_simulated" && (*s.WithinBudget != (s.AmountInRaw.Cmp(s.BudgetRaw) <= 0) || s.AmountInRaw.Cmp(s.FundedRaw) > 0) {
		return errors.New("simulation_funding_or_budget_mismatch")
	}
	return nil
}

func SimulationMatchesQuote(s Simulation, q Quote) bool {
	return s.QuoteId == q.QuoteId && s.BlockHash == q.BlockHash && s.BlockNumber == q.BlockNumber && s.ManifestHash == q.ManifestHash && s.Folio == q.Folio && s.RouteKind == q.RouteKind && s.BudgetRaw != nil && q.RequestedBudgetRaw != nil && s.BudgetRaw.Cmp(q.RequestedBudgetRaw) == 0
}
