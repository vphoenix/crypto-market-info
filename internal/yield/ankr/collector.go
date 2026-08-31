package ankr

import (
	"context"
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
)

const (
	Token = "0xc3344870d52688874b06d844e0c36cc39fc727f6"
	Pool  = "0x7baa1e3bfe49db8361680785182b80bb420a836d"
)

type Collector struct{ Client *avalanche.Client }

func NewCollector(client *avalanche.Client) *Collector { return &Collector{Client: client} }

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Client == nil {
		return marketyield.Batch{}, fmt.Errorf("Ankr collector is not configured")
	}
	snapshot, err := c.Client.Read(ctx, []avalanche.Read{
		{Address: Token, Data: "0x313ce567"}, {Address: Token, Data: "0x71ca337d"}, {Address: Pool},
	})
	if err != nil {
		return marketyield.Batch{}, err
	}
	decimals, decimalsErr := avalanche.Uint64(snapshot.Results[0])
	ratio, ratioErr := avalanche.Uint256(snapshot.Results[1])
	cashRaw, cashErr := avalanche.Quantity(snapshot.Results[2])
	if decimalsErr != nil || ratioErr != nil || cashErr != nil || decimals != 18 || ratio.Sign() <= 0 {
		return marketyield.Batch{}, fmt.Errorf("Ankr invalid decimals, ratio or cash")
	}
	// The token getter is ankrAVAX/AVAX; the observation needs AVAX/ankrAVAX.
	numerator := new(big.Int).Exp(big.NewInt(10), big.NewInt(36), nil)
	exposure, exposureErr := avalanche.PositiveScaled(new(big.Int).Quo(numerator, ratio), -18)
	cash, cashErr := avalanche.Scaled(cashRaw, -18)
	if exposureErr != nil || cashErr != nil {
		return marketyield.Batch{}, fmt.Errorf("Ankr exchange ratio or cash outside Decimal(38,18)")
	}
	route := marketyield.YieldRouteDefinition{
		ProviderType: "protocol", Provider: "Ankr", ProductCode: "avalanche-ankravax-staking", ProductName: "Ankr ankrAVAX 质押兑换率", YieldType: "liquid_staking",
		DepositAssetKey: avalanche.NativeAsset, RedeemAssetKey: avalanche.NativeAsset, PositionAssetKey: "eip155:43114:erc20:" + Token,
		Network: marketyield.Ptr(avalanche.Network), ContractAddress: marketyield.Ptr(Pool), PriceExposureAsset: marketyield.Ptr("AVAX"),
		IncomeSource: "issuance", SourceURL: "https://www.ankr.com/docs/staking-for-developers/smart-contract-api/avax-api/", CollectionEnabled: true,
	}
	observation := marketyield.YieldObservation{
		ObservationTime: snapshot.BlockTime, CollectedAt: snapshot.CollectedAt, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none",
		RateKind: "unknown", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{avalanche.NativeAsset}, RewardComponentRates: []*decimal.Decimal{nil},
		RulePrincipalLossMode: "unknown", RuleEligibility: "unknown", EligibilityReason: marketyield.Ptr("redemption_rules_not_fully_reviewed"), Availability: "unknown",
		ExposureRatio: &exposure, PoolCash: &cash, BlockHeight: &snapshot.BlockHeight, BlockHash: &snapshot.BlockHash,
		Finality: marketyield.Ptr("finalized"), SourcePayloadHash: &snapshot.PayloadHash,
	}
	batch := marketyield.Batch{Source: "ankr-ankravax", CollectedAt: snapshot.CollectedAt, Items: []marketyield.CollectedYield{{Route: route, Observation: observation}}}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Ankr invalid observation: %w", err)
	}
	return batch, nil
}
