package solana

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

var epochsPerYear = decimal.RequireFromString("182.625")

type BSOLCollector struct {
	Reader PoolReader
	Now    func() time.Time
}

func (c *BSOLCollector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Reader == nil {
		return marketyield.Batch{}, fmt.Errorf("bSOL collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	snapshot, err := c.Reader.Read(ctx, PoolConfig{Address: BSOLPoolAddress, Mint: BSOLMintAddress})
	if err != nil {
		return marketyield.Batch{}, err
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	current := decimalUint(snapshot.State.TotalLamports).Div(decimalUint(snapshot.State.PoolTokenSupply))
	previous := decimalUint(snapshot.State.LastEpochTotalLamports).Div(decimalUint(snapshot.State.LastEpochPoolTokenSupply))
	rate := current.Div(previous).Sub(decimal.NewFromInt(1)).Mul(epochsPerYear)
	if !current.IsPositive() || !previous.IsPositive() || rate.IsNegative() {
		return marketyield.Batch{}, fmt.Errorf("bSOL exchange ratios or APR are invalid")
	}
	tvl := decimalUint(snapshot.State.TotalLamports).Div(decimal.NewFromInt(1_000_000_000))
	entry := feeDecimal(snapshot.State.SOLDepositFee)
	exit := feeDecimal(snapshot.State.StakeWithdrawalFee)
	performance := feeDecimal(snapshot.State.EpochFee)
	hash := marketyield.HashPayloads(snapshot.Payloads...)
	network, address, exposure, finality := "solana-mainnet", BSOLPoolAddress, SOLAsset, "finalized"
	height, blockHash := snapshot.BlockHeight, snapshot.BlockHash
	route := marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "BlazeStake", ProductCode: "bsol", ProductName: "BlazeStake bSOL", YieldType: "liquid_staking",
		DepositAssetKey: SOLAsset, PositionAssetKey: BSOLAsset, RedeemAssetKey: SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
		IncomeSource: "combined", SourceURL: MainnetRPCURL, CollectionEnabled: true}
	observation := marketyield.YieldObservation{ObservationTime: time.Unix(snapshot.BlockTime, 0).UTC(), CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero,
		TierMode: "none", Rate: &rate, RateKind: "apr", RateOrigin: "derived", RateMode: "variable", RewardAssetKeys: []string{SOLAsset}, RewardComponentRates: []*decimal.Decimal{&rate},
		EntryFeeRate: &entry, ExitFeeRate: &exit, PerformanceFeeRate: &performance, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &current,
		TVL: &tvl, Availability: "unknown", BlockHeight: &height, BlockHash: &blockHash, Finality: &finality, SourcePayloadHash: &hash}
	return marketyield.Batch{Source: "solana-stakepool-bsol", CollectedAt: collected, Items: []marketyield.CollectedYield{{Route: route, Observation: observation}}}, nil
}

func decimalUint(value uint64) decimal.Decimal {
	return decimal.NewFromBigInt(new(big.Int).SetUint64(value), 0)
}
func feeDecimal(fee Fee) decimal.Decimal {
	if fee.Denominator == 0 || fee.Numerator == 0 {
		return decimal.Zero
	}
	return decimalUint(fee.Numerator).Div(decimalUint(fee.Denominator))
}
