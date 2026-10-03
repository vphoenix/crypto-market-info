package reserve

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
	"sort"
)

func PathBytes(p Path, reverse bool) []byte {
	var b []byte
	n := len(p.Tokens)
	for i := 0; i < n; i++ {
		j := i
		if reverse {
			j = n - 1 - i
		}
		b = append(b, p.Tokens[j][:]...)
		if i < n-1 {
			k := i
			if reverse {
				k = n - 2 - i
			}
			f := p.Fees[k]
			b = append(b, byte(f>>16), byte(f>>8), byte(f))
		}
	}
	return b
}
func Reverse(p Path) Path {
	q := Path{}
	for i := len(p.Tokens) - 1; i >= 0; i-- {
		q.Tokens = append(q.Tokens, p.Tokens[i])
	}
	for i := len(p.Pools) - 1; i >= 0; i-- {
		q.Pools = append(q.Pools, p.Pools[i])
		q.Fees = append(q.Fees, p.Fees[i])
	}
	return q
}
func (r *Reader) Paths(in, out string) []Path {
	if in == out {
		return []Path{}
	}
	var result []Path
	for _, p := range r.Manifest.Paths {
		a, z := Address(p.Tokens[0]), Address(p.Tokens[len(p.Tokens)-1])
		if a == in && z == out {
			result = append(result, p)
		} else if a == out && z == in {
			result = append(result, Reverse(p))
		}
	}
	return result
}
func NewLeg(index uint16, side, mode, in, out string, amount *big.Int) Leg {
	return Leg{LegIndex: index, Side: side, QuoteMode: mode, TokenIn: in, TokenOut: out, RequestedRaw: Copy(amount), PathTokens: []string{}, PathPools: []string{}, PoolFees: []uint32{}, Status: "unquoted", AvailableAt: dex.Now()}
}

// BestLegs batches all finite whitelisted path alternatives. Each returned leg
// still represents an independent state quote; pool overlaps are handled later.
func (r *Reader) BestLegs(ctx context.Context, h dex.Hash, legs []Leg) []Leg {
	calls := []ethereum.Call{}
	type candidate struct {
		index int
		path  Path
	}
	candidates := []candidate{}
	for i := range legs {
		l := &legs[i]
		if !Uint256(l.RequestedRaw) {
			l.Status = "invalid_amount"
			continue
		}
		if l.TokenIn == l.TokenOut {
			l.QuoteMode = "identity"
			l.AmountInRaw = Copy(l.RequestedRaw)
			l.AmountOutRaw = Copy(l.RequestedRaw)
			l.Status = "ok"
			proof, e := r.RPC.Archive.PutObject(struct {
				Block  dex.Hash
				Token  string
				Amount *big.Int
			}{h, Hex(l.TokenIn), l.RequestedRaw})
			if e != nil {
				l.Status = "evidence_write_failed"
				l.AmountInRaw = nil
				l.AmountOutRaw = nil
			} else {
				l.PayloadHash = Hash(proof)
			}
			continue
		}
		paths := r.Paths(l.TokenIn, l.TokenOut)
		if len(paths) == 0 {
			l.Status = "path_not_whitelisted"
			continue
		}
		for _, p := range paths {
			method := "quoteExactInput"
			if l.QuoteMode == "exact_output" {
				method = "quoteExactOutput"
			}
			calls = append(calls, Call(r.Manifest.Quoter, h, method, PathBytes(p, l.QuoteMode == "exact_output"), l.RequestedRaw))
			candidates = append(candidates, candidate{i, p})
		}
		l.Status = "rpc_missing"
	}
	rr := r.Batch(ctx, calls)
	for j, c := range candidates {
		l := &legs[c.index]
		method := "quoteExactInput"
		if l.QuoteMode == "exact_output" {
			method = "quoteExactOutput"
		}
		v, e := Decode(rr[j], method)
		if e != nil {
			if l.Status != "ok" {
				l.Status = e.Error()
				l.AvailableAt = rr[j].At
				l.PayloadHash = Hash(rr[j].Payload)
			}
			continue
		}
		n := v[0].(*big.Int)
		if len(v[1].([]*big.Int)) != len(c.path.Pools) || len(v[2].([]uint32)) != len(c.path.Pools) {
			if l.Status != "ok" {
				l.Status = "quoter_path_result_mismatch"
			}
			continue
		}
		if !Uint256(n) || n.Sign() == 0 {
			if l.Status != "ok" {
				l.Status = "zero_or_invalid_quote"
			}
			continue
		}
		better := l.Status != "ok"
		if !better {
			if l.QuoteMode == "exact_output" {
				better = n.Cmp(l.AmountInRaw) < 0
			} else {
				better = n.Cmp(l.AmountOutRaw) > 0
			}
		}
		if !better {
			continue
		}
		l.Status = "ok"
		l.AvailableAt = rr[j].At
		l.PayloadHash = Hash(rr[j].Payload)
		l.QuoterGasEstimate = v[3].(*big.Int)
		if l.QuoteMode == "exact_output" {
			l.AmountInRaw = n
			l.AmountOutRaw = Copy(l.RequestedRaw)
		} else {
			l.AmountInRaw = Copy(l.RequestedRaw)
			l.AmountOutRaw = n
		}
		l.PathTokens = []string{}
		l.PathPools = []string{}
		l.PoolFees = append([]uint32{}, c.path.Fees...)
		for _, a := range c.path.Tokens {
			l.PathTokens = append(l.PathTokens, Address(a))
		}
		for _, a := range c.path.Pools {
			l.PathPools = append(l.PathPools, Address(a))
		}
	}
	return legs
}
func CompleteLegs(legs []Leg) bool {
	for _, l := range legs {
		if l.Status != "ok" || l.AmountInRaw == nil || l.AmountOutRaw == nil {
			return false
		}
	}
	return true
}
func AddLegs(q *Quote, legs []Leg) {
	for _, l := range legs {
		l.LegIndex = uint16(len(q.DexLegs))
		q.DexLegs = append(q.DexLegs, l)
	}
}
func (r *Reader) Finish(q *Quote, reason string) error {
	q.AvailableAt = dex.Now()
	q.Quality = "incomplete"
	q.Status = "incomplete"
	q.Reason = reason
	q.SuccessfulLegs = 0
	for _, l := range q.DexLegs {
		if l.Status == "ok" {
			q.SuccessfulLegs++
		}
	}
	seen := map[string]bool{}
	overlap := map[string]bool{}
	for _, l := range q.DexLegs {
		for _, p := range l.PathPools {
			if seen[p] {
				overlap[p] = true
			}
			seen[p] = true
		}
	}
	for p := range overlap {
		q.SharedPools = append(q.SharedPools, p)
	}
	sort.Strings(q.SharedPools)
	if reason == "" && q.AmountInRaw != nil && q.AmountOutRaw != nil && q.ExpectedLegs == q.SuccessfulLegs {
		q.WithinBudget = Ptr(q.AmountInRaw.Cmp(q.RequestedBudgetRaw) <= 0)
		if *q.WithinBudget {
			q.Quality = "quoted_complete"
			if len(q.SharedPools) > 0 {
				q.Quality = "indicative_overlap"
			}
			q.Status = "ok"
		} else {
			q.Reason = "budget_exceeded"
		}
	}
	q.QuoteId = ID(struct {
		Batch, Folio, Route string
		Budget              *big.Int
	}{q.BatchId, q.Folio, q.RouteId, q.RequestedBudgetRaw})
	h, e := r.RPC.Archive.PutObject(struct {
		Quote   string
		Members []dex.Hash
		Reason  string
		At      any
	}{Hex(q.QuoteId), append([]dex.Hash{}, r.Members...), q.Reason, q.AvailableAt})
	if e != nil {
		return e
	}
	q.PayloadHash = Hash(h)
	for i := range q.DexLegs {
		l := &q.DexLegs[i]
		if len(l.PayloadHash) != 32 || l.PayloadHash == string(make([]byte, 32)) {
			h, e := r.RPC.Archive.PutObject(struct {
				Quote  string
				Index  uint16
				Status string
			}{Hex(q.QuoteId), l.LegIndex, l.Status})
			if e != nil {
				return e
			}
			l.PayloadHash = Hash(h)
		}
	}
	return nil
}
func LegSum(legs []Leg, input bool) (*big.Int, error) {
	n := new(big.Int)
	for _, l := range legs {
		a := l.AmountOutRaw
		if input {
			a = l.AmountInRaw
		}
		if l.Status != "ok" || a == nil {
			return nil, errors.New("missing_leg")
		}
		n.Add(n, a)
		if n.BitLen() > 256 {
			return nil, errors.New("sum_overflow")
		}
	}
	return n, nil
}
