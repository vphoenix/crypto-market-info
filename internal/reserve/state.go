package reserve

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
)

type weights struct{ Low, Spot, High *big.Int }
type prices struct{ Low, High *big.Int }
type tokenParams struct {
	Token          common.Address
	Weight         weights
	Price          prices
	MaxAuctionSize *big.Int
	InRebalance    bool
}
type timestamps struct{ StartedAt, RestrictedUntil, AvailableUntil *big.Int }

func uint64Time(v *big.Int) (*uint64, error) {
	if !dex.ValidUint(v, 64) {
		return nil, errors.New("timestamp_overflow")
	}
	return Ptr(v.Uint64()), nil
}
func stateBase(c Capture, f dex.Address, b dex.Block) State {
	return State{ChainId: 1, ManifestHash: c.ManifestHash, CaptureId: c.CaptureId, BatchId: c.BatchId, BlockNumber: b.Number, BlockHash: Hash(b.Hash), BlockTime: b.Time, Folio: Address(f), BaseFeeWei: Copy(b.BaseFee), MinimumMintFeeD18: Uint(300_000_000_000_000), Basket: []Asset{}}
}
func (r *Reader) State(ctx context.Context, b dex.Block, c Capture, f dex.Address) (s State) {
	s = stateBase(c, f, b)
	defer func() {
		s.AvailableAt = dex.Now()
		p, e := r.Proof(struct {
			Folio  dex.Address
			Block  dex.Hash
			Reason string
		}{f, b.Hash, s.Reason})
		if e != nil {
			s.StateComplete = false
			s.Reason = "evidence_write_failed"
		}
		s.PayloadHash = p
	}()
	names := []string{"version", "decimals", "totalSupply", "getPendingFeeShares", "mintFee", "tvlFee", "daoFeeRegistry", "isDeprecated", "stateChangeActive", "trustedFillerRegistry", "trustedFillerEnabled", "bidsEnabled", "nextAuctionId", "totalAssets", "getRebalance"}
	calls := []ethereum.Call{{Method: "eth_getStorageAt", Params: []any{f.String(), ImplementationSlot, ethereum.BlockRef(b.Hash)}}}
	for _, n := range names {
		calls = append(calls, Call(f, b.Hash, n))
	}
	rr := r.Batch(ctx, calls)
	slot, e := ethereum.HexResult(rr[0])
	if e != nil {
		s.Reason = e.Error()
		return
	}
	if len(slot) != 32 {
		s.Reason = "implementation_slot_width"
		return
	}
	for _, v := range slot[:12] {
		if v != 0 {
			s.Reason = "implementation_slot_padding"
			return
		}
	}
	impl := dex.Address(common.BytesToAddress(slot[12:]))
	s.Implementation = Ptr(Address(impl))
	if impl != r.Manifest.Implementation {
		s.Reason = "unsupported_implementation"
		return
	}
	v := make([][]any, len(names))
	for i, n := range names {
		v[i], e = Decode(rr[i+1], n)
		if e != nil {
			s.Reason = e.Error()
			return
		}
	}
	s.ProtocolVersion = v[0][0].(string)
	if s.ProtocolVersion != "5.0.0" {
		s.Reason = "unsupported_protocol_version"
		return
	}
	code := r.Batch(ctx, []ethereum.Call{{Method: "eth_getCode", Params: []any{impl.String(), ethereum.BlockRef(b.Hash)}}})[0]
	h, e := CodeHash(code)
	if e != nil {
		s.Reason = e.Error()
		return
	}
	s.ImplementationCodeHash = Ptr(Hash(h))
	if h != r.Manifest.ImplementationCodeHash {
		s.Reason = "implementation_code_hash_mismatch"
		return
	}
	s.IdentityOk = true
	s.ShareDecimals = Ptr(v[1][0].(uint8))
	s.TotalSupplyRaw = v[2][0].(*big.Int)
	s.PendingFeeSharesRaw = v[3][0].(*big.Int)
	s.MintFeeD18 = v[4][0].(*big.Int)
	s.TvlFeePerSecondD18 = v[5][0].(*big.Int)
	registry := Addr(v[6][0].(common.Address))
	s.DaoFeeRegistry = Ptr(Address(registry))
	s.Deprecated = Ptr(v[7][0].(bool))
	s.SyncStateChangeActive = Ptr(v[8][0].(bool))
	s.AsyncStateChangeActive = Ptr(v[8][1].(bool))
	s.TrustedFillerRegistry = Ptr(Address(Addr(v[9][0].(common.Address))))
	s.TrustedFillerEnabled = Ptr(v[10][0].(bool))
	s.GlobalBidsEnabled = Ptr(v[11][0].(bool))
	s.NextAuctionId = v[12][0].(*big.Int)
	assets := v[13][0].([]common.Address)
	amounts := v[13][1].([]*big.Int)
	params := *abi.ConvertType(v[14][2], new([]tokenParams)).(*[]tokenParams)
	if len(assets) == 0 || len(assets) != len(amounts) || len(params) != len(assets) {
		s.Reason = "basket_members_mismatch"
		return
	}
	s.RebalanceNonce = v[14][0].(*big.Int)
	s.PriceControl = Ptr(v[14][1].(uint8))
	s.RebalanceBidsEnabled = Ptr(v[14][5].(bool))
	w := *abi.ConvertType(v[14][3], new(weights)).(*weights)
	t := *abi.ConvertType(v[14][4], new(timestamps)).(*timestamps)
	s.RebalanceLimitLowD18 = w.Low
	s.RebalanceLimitSpotD18 = w.Spot
	s.RebalanceLimitHighD18 = w.High
	for _, pair := range []struct {
		src *big.Int
		dst **uint64
	}{{t.StartedAt, &s.RebalanceStartedAt}, {t.RestrictedUntil, &s.RebalanceRestrictedUntil}, {t.AvailableUntil, &s.RebalanceAvailableUntil}} {
		*pair.dst, e = uint64Time(pair.src)
		if e != nil {
			s.Reason = e.Error()
			return
		}
	}
	seen := map[common.Address]bool{}
	for i, a := range assets {
		if seen[a] || params[i].Token != a {
			s.Reason = "basket_order_or_duplicate"
			return
		}
		seen[a] = true
		p := params[i]
		s.Basket = append(s.Basket, Asset{Token: Address(Addr(a)), AmountRaw: amounts[i], InRebalance: Ptr(p.InRebalance), WeightLowD27: p.Weight.Low, WeightSpotD27: p.Weight.Spot, WeightHighD27: p.Weight.High, InitialPriceLowD27: p.Price.Low, InitialPriceHighD27: p.Price.High, MaxAuctionSizeRaw: p.MaxAuctionSize})
	}
	calls = []ethereum.Call{Call(registry, b.Hash, "getFeeDetails", Eth(f))}
	for _, a := range assets {
		calls = append(calls, Call(Addr(a), b.Hash, "decimals"))
	}
	if s.NextAuctionId.Sign() > 0 {
		s.AuctionId = new(big.Int).Sub(s.NextAuctionId, Uint(1))
		calls = append(calls, Call(f, b.Hash, "auctions", s.AuctionId))
	}
	rr = r.Batch(ctx, calls)
	dao, e := Decode(rr[0], "getFeeDetails")
	if e != nil {
		s.Reason = e.Error()
		return
	}
	s.DaoFeeNumerator = dao[1].(*big.Int)
	s.DaoFeeDenominator = dao[2].(*big.Int)
	s.DaoFeeFloorD18 = dao[3].(*big.Int)
	if s.DaoFeeDenominator.Sign() == 0 {
		s.Reason = "dao_zero_denominator"
		return
	}
	for i := range assets {
		d, e := Decode(rr[i+1], "decimals")
		if e != nil {
			s.Reason = e.Error()
			return
		}
		s.Basket[i].Decimals = Ptr(d[0].(uint8))
	}
	if s.AuctionId != nil {
		a, e := Decode(rr[len(rr)-1], "auctions")
		if e != nil {
			s.Reason = e.Error()
			return
		}
		s.AuctionRebalanceNonce = a[0].(*big.Int)
		s.AuctionStartTime, e = uint64Time(a[1].(*big.Int))
		if e != nil {
			s.Reason = e.Error()
			return
		}
		s.AuctionEndTime, e = uint64Time(a[2].(*big.Int))
		if e != nil {
			s.Reason = e.Error()
			return
		}
		calls = []ethereum.Call{}
		for _, a := range assets {
			calls = append(calls, Call(f, b.Hash, "getAuctionPrice", s.AuctionId, a))
		}
		rr = r.Batch(ctx, calls)
		for i := range assets {
			v, e := Decode(rr[i], "getAuctionPrice")
			if e != nil {
				continue
			}
			p := *abi.ConvertType(v[0], new(prices)).(*prices)
			s.Basket[i].AuctionPriceLowD27 = p.Low
			s.Basket[i].AuctionPriceHighD27 = p.High
		}
	}
	s.StateComplete = true
	for _, a := range s.Basket {
		known := false
		for _, m := range r.Manifest.Assets {
			if Address(m.Address) == a.Token && a.Decimals != nil && m.Decimals == *a.Decimals {
				known = true
				break
			}
		}
		if !known {
			s.Reason = "basket_metadata_not_whitelisted"
		}
	}
	for _, f := range r.Manifest.Folios {
		if f.Address == folioAddress(s) && s.ShareDecimals != nil && f.ShareDecimals != *s.ShareDecimals {
			s.Reason = "share_decimals_changed"
		}
	}
	if len(s.Basket) > 8 {
		s.Reason = "basket_exceeds_quote_scope"
	}
	return
}
func Usable(s State) bool {
	return s.IdentityOk && s.StateComplete && s.Reason == "" && len(s.Basket) <= 8 && s.SyncStateChangeActive != nil && !*s.SyncStateChangeActive && s.AsyncStateChangeActive != nil && !*s.AsyncStateChangeActive && s.TotalSupplyRaw != nil && s.TotalSupplyRaw.Sign() > 0
}
func AuctionActive(s State) bool {
	return MintAllowed(s) && s.AuctionId != nil && s.AuctionStartTime != nil && s.AuctionEndTime != nil && uint64(s.BlockTime.Unix()) >= *s.AuctionStartTime && uint64(s.BlockTime.Unix()) <= *s.AuctionEndTime && s.RebalanceBidsEnabled != nil && *s.RebalanceBidsEnabled && s.AuctionRebalanceNonce != nil && s.RebalanceNonce != nil && s.AuctionRebalanceNonce.Cmp(s.RebalanceNonce) == 0
}

func MintAllowed(s State) bool { return Usable(s) && s.Deprecated != nil && !*s.Deprecated }
