package ethereum

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"sort"
)

// ReadSky pins code, proxy implementation slots, pool identity and decimals at the
// same hash as cash/fees. Unknown versions invalidate this block, never update pins.
func (c *Client) ReadSky(ctx context.Context, a dex.Anchor) dex.Sky {
	m := DefaultManifest()
	s := dex.Sky{Anchor: a, Module: "sky-litepsm-usdc-v1"}
	var calls []Call
	var labels []string
	var checks []func(Result) bool
	appendCheck := func(label string, call Call, check func(Result) bool) {
		labels = append(labels, label)
		calls = append(calls, call)
		checks = append(checks, check)
	}
	eqWord := func(expected *big.Int) func(Result) bool {
		return func(r Result) bool { n, e := ABIWord(r, 256); return e == nil && n.Cmp(expected) == 0 }
	}
	for _, p := range m.Codes {
		appendCheck("code:"+p.Name, Call{"eth_getCode", []any{p.Address.String(), BlockRef(a.Hash)}}, func(r Result) bool { b, e := HexResult(r); return e == nil && dex.Digest(b) == p.Hash })
	}
	for _, p := range m.Storage {
		appendCheck("implementation:"+p.Address.String(), Call{"eth_getStorageAt", []any{p.Address.String(), p.Slot, BlockRef(a.Hash)}}, func(r Result) bool { b, e := HexResult(r); return e == nil && "0x"+hex.EncodeToString(b) == p.Expected })
	}
	tokens := []string{"DAI", "USDC", "USDS", "USDT", "WETH"}
	for _, name := range tokens {
		appendCheck("decimals:"+name, EthCall(m.Addresses[name], data(m, "decimals()"), a.Hash), eqWord(big.NewInt(int64(m.Decimals[name]))))
	}
	for _, p := range m.Pools {
		d := data(m, "getPool(address,address,uint24)", m.Addresses[p.Token0], m.Addresses[p.Token1]) + word(big.NewInt(int64(p.Fee)))
		appendCheck("pool:"+p.ID, EthCall(m.Addresses["factory"], d, a.Hash), eqWord(new(big.Int).SetBytes(p.Address[:])))
	}
	appendCheck("usdt_not_deprecated", EthCall(m.Addresses["USDT"], data(m, "deprecated()"), a.Hash), eqWord(new(big.Int)))
	identityCount := len(calls)
	type stateCall struct {
		label, contract, sig string
		args                 []dex.Address
		dest                 **big.Int
		flag                 *bool
	}
	fields := []stateCall{
		{"tin", "psm", "tin()", nil, &s.Tin, nil}, {"tout", "psm", "tout()", nil, &s.Tout, nil}, {"buf", "psm", "buf()", nil, &s.Buf, nil},
		{"dai_cash", "DAI", "balanceOf(address)", []dex.Address{m.Addresses["psm"]}, &s.DAICash, nil},
		{"usdc_cash", "USDC", "balanceOf(address)", []dex.Address{m.Addresses["pocket"]}, &s.USDCCash, nil},
		{"pocket_allowance", "USDC", "allowance(address,address)", []dex.Address{m.Addresses["pocket"], m.Addresses["psm"]}, &s.Allowance, nil},
		{"vat_live", "vat", "live()", nil, nil, &s.VatLive}, {"dai_join_live", "daiJoin", "live()", nil, nil, &s.DAIJoinLive},
		{"dai_join_ward", "DAI", "wards(address)", []dex.Address{m.Addresses["daiJoin"]}, nil, &s.DAIJoinWard},
		{"usds_join_ward", "USDS", "wards(address)", []dex.Address{m.Addresses["usdsJoin"]}, nil, &s.USDSJoinWard},
	}
	for _, f := range fields {
		appendCheck(f.label, EthCall(m.Addresses[f.contract], data(m, f.sig, f.args...), a.Hash), func(r Result) bool {
			n, e := ABIWord(r, 256)
			if e != nil {
				return false
			}
			if f.dest != nil {
				*f.dest = n
			} else {
				if n.BitLen() > 1 {
					return false
				}
				*f.flag = n.Sign() != 0
			}
			return true
		})
	}
	results := c.Batch(ctx, calls)
	s.IdentityOK = true
	s.Complete = true
	type proof struct {
		Label   string
		Payload dex.Hash
		OK      bool
	}
	proofs := make([]proof, len(results))
	for i, r := range results {
		ok := checks[i](r)
		proofs[i] = proof{labels[i], r.Payload, ok}
		if !ok {
			s.Complete = false
			if i < identityCount {
				s.IdentityOK = false
			}
			if s.Reason == "" {
				s.Reason = "invalid_or_missing:" + labels[i]
			}
		}
	}
	h, e := c.Archive.PutObject(struct {
		Anchor  dex.Anchor
		Source  string
		Members []proof
	}{a, c.SourceID, proofs})
	if e != nil {
		s.Complete = false
		s.Reason = "evidence_write_failed"
	} else {
		s.Payload = h
	}
	return s
}
func (c *Client) VerifyChain(ctx context.Context) error {
	r := c.One(ctx, "eth_chainId", []any{})
	b, e := quantityResult(r, 64)
	if e != nil {
		return e
	}
	if b.Uint64() != 1 {
		return errors.New("unexpected_chain_id")
	}
	return nil
}
func uniqueHashes(hashes []dex.Hash) []dex.Hash {
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].String() < hashes[j].String() })
	out := hashes[:0]
	for _, h := range hashes {
		if len(out) == 0 || out[len(out)-1] != h {
			out = append(out, h)
		}
	}
	return out
}
