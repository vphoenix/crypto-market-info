package dexarb

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"strings"
	"time"
)

func ParseAtoms(s string, decimals int) (*big.Int, error) {
	if len(s) == 0 || len(s) > 90 || strings.ContainsAny(s, "eE+-") {
		return nil, errors.New("use a nonnegative plain decimal amount")
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return nil, errors.New("invalid decimal")
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > decimals {
		return nil, errors.New("amount exceeds token precision")
	}
	raw := parts[0] + fraction + strings.Repeat("0", decimals-len(fraction))
	for _, c := range raw {
		if c < '0' || c > '9' {
			return nil, errors.New("invalid decimal digit")
		}
	}
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok || !dex.ValidUint(n, 256) {
		return nil, errors.New("amount overflow")
	}
	return n, nil
}

type CostScenario struct {
	GasUnits                             uint64
	PriorityWei, OrderingUSDC, OtherUSDC *big.Int
	CapitalUSDT                          *big.Int
}

func (s CostScenario) Validate() error {
	if s.GasUnits == 0 {
		return errors.New("gas units must be explicit and positive")
	}
	for _, n := range []*big.Int{s.PriorityWei, s.OrderingUSDC, s.OtherUSDC, s.CapitalUSDT} {
		if !dex.ValidUint(n, 256) {
			return errors.New("all cost and capital inputs must be explicit UInt256 atoms")
		}
	}
	return nil
}

type CostRPC interface {
	Quoter
	Canonical(context.Context, dex.Block) error
}
type CostQuote struct {
	Request      dex.SwapRequest
	ResultAmount *big.Int
	Payload      dex.Hash
	CollectedAt  time.Time
	Reason       string
}
type CostResult struct {
	SelectedAmountUSDC, SelectedGrossUSDC                        *big.Int
	ScenarioHash, BlockHash, WindowID                            dex.Hash
	SourceID                                                     string
	Status, Reason                                               string
	GasWei, GasUSDC, EntryUSDC, NetUSDC, SettlementReferenceUSDT *big.Int
	BudgetOK                                                     bool
	Quotes                                                       []CostQuote
}

// QuoteCosts uses at most four RPC members: canonical header, entry quote,
// exact-output gas replacement and an indicative settlement quote. Every quote
// uses the original block hash. The exit is NOT a sequential USDT profit proof.
func QuoteCosts(ctx context.Context, rpc CostRPC, w Window, s CostScenario, usdc, usdt, weth dex.Address) CostResult {
	result := CostResult{ScenarioHash: dex.ObjectHash(s), BlockHash: w.Best.Block.Hash, WindowID: w.ID, Status: "unknown"}
	fail := func(reason string) CostResult { result.Reason = reason; return result }
	if e := s.Validate(); e != nil {
		return fail(e.Error())
	}
	if e := rpc.Canonical(ctx, w.Best.Block); e != nil {
		return fail("canonical_check_failed")
	}
	if !dex.ValidUint(w.Best.Block.BaseFee, 256) {
		return fail("base_fee_unknown")
	}
	price := new(big.Int).Add(w.Best.Block.BaseFee, s.PriorityWei)
	gas := new(big.Int).Mul(price, new(big.Int).SetUint64(s.GasUnits))
	if !dex.ValidUint(gas, 256) || gas.Sign() == 0 {
		return fail("gas_amount_invalid")
	}
	result.GasWei = gas
	requests := []dex.SwapRequest{{TokenIn: usdt, TokenOut: usdc, Fee: 100, Amount: s.CapitalUSDT}, {TokenIn: usdc, TokenOut: weth, Fee: 500, Amount: gas, ExactOutput: true}}
	rr := rpc.Quotes(ctx, result.BlockHash, requests)
	if len(rr) != 2 {
		return fail("cost_quote_batch_missing")
	}
	for i, v := range rr {
		proof := CostQuote{Request: requests[i], ResultAmount: v.Amount, Payload: v.Payload, CollectedAt: v.At}
		if v.Err != nil {
			proof.Reason = v.Err.Error()
		}
		result.Quotes = append(result.Quotes, proof)
	}
	for _, v := range rr {
		if v.Err != nil || !dex.ValidUint(v.Amount, 256) || v.Payload == (dex.Hash{}) {
			return fail("cost_quote_unavailable")
		}
	}
	result.EntryUSDC = dex.Copy(rr[0].Amount)
	result.GasUSDC = dex.Copy(rr[1].Amount)
	costs := new(big.Int).Add(result.GasUSDC, s.OrderingUSDC)
	costs.Add(costs, s.OtherUSDC)
	choices := w.Alternatives
	if len(choices) == 0 {
		choices = []Candidate{w.Best}
	}
	var selected *Candidate
	for _, candidate := range choices {
		if candidate.Block.Hash != w.Best.Block.Hash || candidate.Quote.Status != "ok" || candidate.Gross == nil {
			continue
		}
		need := new(big.Int).Add(candidate.Quote.Requested, costs)
		if need.Cmp(result.EntryUSDC) > 0 {
			continue
		}
		if selected == nil || candidate.Gross.Cmp(selected.Gross) > 0 {
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return fail("all_sizes_plus_cost_reserve_exceed_capital")
	}
	result.BudgetOK = true
	result.SelectedAmountUSDC = dex.Copy(selected.Quote.Requested)
	result.SelectedGrossUSDC = dex.Copy(selected.Gross)
	result.NetUSDC = new(big.Int).Sub(selected.Gross, costs)
	// Entry fees are already reflected in EntryUSDC. An independent same-state
	// reverse quote is shown only as a reference, not charged repeatedly per block
	// and not presented as an executable entry+exit round trip.
	settle := new(big.Int).Add(result.EntryUSDC, result.NetUSDC)
	if settle.Sign() > 0 && dex.ValidUint(settle, 256) {
		req := dex.SwapRequest{TokenIn: usdc, TokenOut: usdt, Fee: 100, Amount: settle}
		out := rpc.Quotes(ctx, result.BlockHash, []dex.SwapRequest{req})
		if len(out) == 1 {
			v := out[0]
			proof := CostQuote{Request: req, ResultAmount: v.Amount, Payload: v.Payload, CollectedAt: v.At}
			if v.Err != nil {
				proof.Reason = v.Err.Error()
			}
			result.Quotes = append(result.Quotes, proof)
			if v.Err == nil {
				result.SettlementReferenceUSDT = dex.Copy(v.Amount)
			}
		}
	}
	result.Status = "gross_candidate"
	result.Reason = "scenario_net_nonpositive"
	if result.NetUSDC.Sign() > 0 {
		result.Status = "scenario_positive"
		result.Reason = "USDC_inventory_scenario_only; USDT_settlement_not_execution_validated"
	}
	return result
}
