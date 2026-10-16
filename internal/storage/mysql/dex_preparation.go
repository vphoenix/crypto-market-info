package mysql

import (
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

type dexFactRows struct {
	sky, quotes, logs []record
}

func prepareDEXFactRows(b dex.Batch) (dexFactRows, error) {
	var out dexFactRows
	if s := b.Sky; s != nil {
		if !sameBatch(s.Anchor, b.Block.Anchor) || !validAnchor(s.Anchor) {
			return out, fmt.Errorf("DEX state anchor invalid")
		}
		if s.Complete && (s.Tin == nil || s.Tout == nil || s.Buf == nil || s.DAICash == nil || s.USDCCash == nil || s.Allowance == nil) {
			return out, fmt.Errorf("DEX complete state missing values")
		}
		values := append(anchorValues(s.Anchor), s.Module, s.Tin, s.Tout, s.Buf, s.DAICash, s.USDCCash, s.Allowance, s.VatLive, s.DAIJoinLive, s.DAIJoinWard, s.USDSJoinWard, s.IdentityOK, s.Complete, s.Reason)
		r, err := namedRecord(dexAnchorColumns+",module_id,tin,tout,buf,dai_cash,usdc_pocket_cash,pocket_allowance,vat_live,dai_join_live,dai_join_ward,usds_join_ward,identity_ok,state_complete,reason", values)
		if err != nil {
			return out, err
		}
		out.sky = []record{r}
	}
	seen := map[dex.Hash]bool{}
	for _, q := range b.Quotes {
		if !sameBatch(q.Anchor, b.Block.Anchor) || !validAnchor(q.Anchor) || !dex.ValidUint(q.Requested, 256) || seen[q.ID] {
			return out, fmt.Errorf("DEX quote identity invalid")
		}
		want := q
		want.SetID()
		if want.ID != q.ID {
			return out, fmt.Errorf("DEX quote hash invalid")
		}
		seen[q.ID] = true
		if q.Status == "ok" && (q.AmountIn == nil || q.AmountOut == nil || q.V3In == nil || q.V3Out == nil || q.SqrtAfter == nil || q.SqrtAfter.BitLen() > 160 || q.GasEstimate == nil) {
			return out, fmt.Errorf("DEX quote values missing")
		}
		r, err := namedRecord(dexQuoteColumns, quoteValues(q))
		if err != nil {
			return out, err
		}
		out.quotes = append(out.quotes, r)
	}
	for _, l := range b.Logs {
		if !sameBatch(l.Anchor, b.Block.Anchor) || !validAnchor(l.Anchor) || l.Removed {
			return out, fmt.Errorf("DEX log identity invalid")
		}
		r, err := namedRecord(dexLogColumns, logValues(l))
		if err != nil {
			return out, err
		}
		out.logs = append(out.logs, r)
	}
	for _, r := range b.Receipts {
		if r.Hash != b.Block.Hash || r.Number != b.Block.Number || !validAnchor(r.Anchor) {
			return out, fmt.Errorf("DEX receipt identity invalid")
		}
	}
	return out, nil
}
