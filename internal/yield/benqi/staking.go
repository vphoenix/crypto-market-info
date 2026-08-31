package benqi

import (
	"context"
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
)

const (
	SAVAX = "0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be"
	// getPooledAvaxByShares(10^18), with one 32-byte uint256 argument.
	pooledAVAXCall = "0x4a36d6c10000000000000000000000000000000000000000000000000de0b6b3a7640000"
)

type StakingCollector struct{ Client *avalanche.Client }

func NewStakingCollector(client *avalanche.Client) *StakingCollector {
	return &StakingCollector{Client: client}
}

func (c *StakingCollector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Client == nil {
		return marketyield.Batch{}, fmt.Errorf("BENQI staking collector is not configured")
	}
	snapshot, err := c.Client.Read(ctx, []avalanche.Read{
		{Address: SAVAX, Data: "0x313ce567"}, {Address: SAVAX, Data: pooledAVAXCall},
		{Address: SAVAX, Data: "0x629e8056"}, {Address: SAVAX, Data: "0x5cd47487"},
		{Address: SAVAX, Data: "0x6e34637c"}, {Address: SAVAX, Data: "0x04646a49"},
		{Address: SAVAX, Data: "0x40a233a6"}, {Address: SAVAX, Data: "0x5c975abb"},
		{Address: SAVAX, Data: "0xe1a283d6"}, {Address: SAVAX},
	})
	if err != nil {
		return marketyield.Batch{}, err
	}
	values := make([]*big.Int, 7)
	for i := range values {
		values[i], err = avalanche.Uint256(snapshot.Results[i])
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("BENQI staking invalid ABI integer at field %d", i)
		}
	}
	paused, pausedErr := avalanche.Bool(snapshot.Results[7])
	mintPaused, mintErr := avalanche.Bool(snapshot.Results[8])
	cashRaw, cashErr := avalanche.Quantity(snapshot.Results[9])
	if pausedErr != nil || mintErr != nil || cashErr != nil || values[0].Cmp(big.NewInt(18)) != 0 || !values[5].IsUint64() || !values[6].IsUint64() {
		return marketyield.Batch{}, fmt.Errorf("BENQI staking invalid decimals, flags, cash or exit period")
	}
	exposure, exposureErr := avalanche.PositiveScaled(values[1], -18)
	tvl, tvlErr := avalanche.Scaled(values[2], -18)
	fee, feeErr := avalanche.Scaled(values[4], -18)
	cash, cashErr := avalanche.Scaled(cashRaw, -18)
	if exposureErr != nil || tvlErr != nil || feeErr != nil || cashErr != nil || fee.GreaterThan(decimal.NewFromInt(1)) {
		return marketyield.Batch{}, fmt.Errorf("BENQI staking invalid amounts or protocol reward share")
	}
	observation := baseObservation(snapshot)
	observation.ExposureRatio, observation.TVL, observation.PoolCash, observation.PerformanceFeeRate = &exposure, &tvl, &cash, &fee
	observation.EntryFeeRate, observation.ExitFeeRate = marketyield.Ptr(decimal.Zero), marketyield.Ptr(decimal.Zero)
	observation.UnbondingSeconds = marketyield.Ptr(values[5].Uint64())
	observation.RedemptionWindowSeconds = marketyield.Ptr(values[6].Uint64())
	observation.Availability = "available"
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if values[3].Cmp(maxUint256) != 0 {
		cap, capErr := avalanche.Scaled(values[3], -18)
		if capErr != nil {
			return marketyield.Batch{}, fmt.Errorf("BENQI staking finite capacity exceeds Decimal(38,18)")
		}
		remaining := cap.Sub(tvl)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
		observation.Capacity, observation.RemainingCapacity = &cap, &remaining
		if remaining.IsZero() {
			observation.Availability = "unavailable"
		}
	}
	if paused || mintPaused {
		observation.Availability = "paused"
	}
	if values[6].Sign() == 0 {
		observation.RuleEligibility = "unknown"
		observation.EligibilityReason = marketyield.Ptr("redemption_window_empty")
	}
	definition := route("avalanche-savax-staking", "BENQI sAVAX 质押兑换率", "liquid_staking", SAVAX, "issuance", "https://docs.benqi.fi/resources/contracts/liquid-staking")
	return completeBatch("benqi-savax", definition, observation)
}

func baseObservation(snapshot avalanche.Snapshot) marketyield.YieldObservation {
	return marketyield.YieldObservation{
		ObservationTime: snapshot.BlockTime, CollectedAt: snapshot.CollectedAt, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none",
		RateKind: "unknown", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{avalanche.NativeAsset}, RewardComponentRates: []*decimal.Decimal{nil},
		RulePrincipalLossMode: "none", RuleEligibility: "candidate", Availability: "unknown",
		BlockHeight: &snapshot.BlockHeight, BlockHash: &snapshot.BlockHash, Finality: marketyield.Ptr("finalized"), SourcePayloadHash: &snapshot.PayloadHash,
	}
}

func route(code, name, kind, token, income, sourceURL string) marketyield.YieldRouteDefinition {
	return marketyield.YieldRouteDefinition{
		ProviderType: "protocol", Provider: "BENQI", ProductCode: code, ProductName: name, YieldType: kind,
		DepositAssetKey: avalanche.NativeAsset, RedeemAssetKey: avalanche.NativeAsset, PositionAssetKey: "eip155:43114:erc20:" + token,
		Network: marketyield.Ptr(avalanche.Network), ContractAddress: &token, PriceExposureAsset: marketyield.Ptr("AVAX"),
		IncomeSource: income, SourceURL: sourceURL, CollectionEnabled: true,
	}
}

func completeBatch(source string, route marketyield.YieldRouteDefinition, observation marketyield.YieldObservation) (marketyield.Batch, error) {
	batch := marketyield.Batch{Source: source, CollectedAt: observation.CollectedAt, Items: []marketyield.CollectedYield{{Route: route, Observation: observation}}}
	if err := batch.NormalizeAndValidateForLiveCollection(); err != nil {
		return marketyield.Batch{}, fmt.Errorf("BENQI invalid observation: %w", err)
	}
	return batch, nil
}
