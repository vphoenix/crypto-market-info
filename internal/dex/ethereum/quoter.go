package ethereum

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
)

func (c *Client) Quotes(ctx context.Context, h dex.Hash, requests []dex.SwapRequest) []dex.SwapResult {
	m := DefaultManifest()
	calls := make([]Call, len(requests))
	out := make([]dex.SwapResult, len(requests))
	for i, r := range requests {
		if !dex.ValidUint(r.Amount, 256) || r.Fee > 0xffffff {
			for j := range out {
				out[j].Err = errors.New("invalid_quote_request")
			}
			return out
		}
		sig := "quoteExactInputSingle((address,address,uint256,uint24,uint160))"
		if r.ExactOutput {
			sig = "quoteExactOutputSingle((address,address,uint256,uint24,uint160))"
		}
		d := m.Selectors[sig] + addressWord(r.TokenIn) + addressWord(r.TokenOut) + word(r.Amount) + word(new(big.Int).SetUint64(uint64(r.Fee))) + word(new(big.Int))
		calls[i] = EthCall(m.Addresses["quoter"], d, h)
	}
	for i, r := range c.Batch(ctx, calls) {
		q := dex.SwapResult{Payload: r.Payload, At: r.At}
		b, e := HexResult(r)
		if e == nil && len(b) != 128 {
			e = errors.New("quoter_abi_length")
		}
		if e == nil {
			q.Amount = new(big.Int).SetBytes(b[:32])
			q.SqrtAfter = new(big.Int).SetBytes(b[32:64])
			ticks := new(big.Int).SetBytes(b[64:96])
			q.Gas = new(big.Int).SetBytes(b[96:128])
			if q.SqrtAfter.BitLen() > 160 || ticks.BitLen() > 32 {
				e = errors.New("quoter_abi_width")
			} else {
				q.Ticks = uint32(ticks.Uint64())
			}
		}
		q.Err = e
		out[i] = q
	}
	return out
}
