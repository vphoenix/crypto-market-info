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
	if c == nil {
		return marketyield.Batch{}, fmt.Errorf("bSOL collector is not configured")
	}
	return (&StakePoolCollector{Reader: c.Reader, Product: BSOLProduct, Now: c.Now}).Collect(ctx)
}

type StakePoolProduct struct {
	PoolConfig
	Provider         string
	ProductCode      string
	ProductName      string
	PositionAssetKey string
}

var (
	BSOLProduct     = StakePoolProduct{PoolConfig: PoolConfig{Program: StakePoolProgram, Address: BSOLPoolAddress, Mint: BSOLMintAddress}, Provider: "BlazeStake", ProductCode: "bsol", ProductName: "BlazeStake bSOL", PositionAssetKey: BSOLAsset}
	LaineSOLProduct = StakePoolProduct{PoolConfig: PoolConfig{Program: StakePoolProgram, Address: LaineSOLPoolAddress, Mint: LaineSOLMintAddress}, Provider: "Laine", ProductCode: "lainesol", ProductName: "Laine laineSOL", PositionAssetKey: LaineSOLAsset}
	JupSOLProduct   = StakePoolProduct{PoolConfig: PoolConfig{Program: SanctumMultiStakePoolProgram, Address: JupSOLPoolAddress, Mint: JupSOLMintAddress}, Provider: "Jupiter", ProductCode: "jupsol", ProductName: "Jupiter JupSOL", PositionAssetKey: JupSOLAsset}
	HSOLProduct     = StakePoolProduct{PoolConfig: PoolConfig{Program: SanctumStakePoolProgram, Address: HSOLPoolAddress, Mint: HSOLMintAddress}, Provider: "Helius", ProductCode: "hsol", ProductName: "Helius hSOL", PositionAssetKey: HSOLAsset}
)

type StakePoolCollector struct {
	Reader  PoolReader
	Product StakePoolProduct
	Now     func() time.Time
}

func (c *StakePoolCollector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Reader == nil {
		return marketyield.Batch{}, fmt.Errorf("stake pool collector is not configured")
	}
	product := c.Product
	for name, value := range map[string]string{"provider": product.Provider, "product code": product.ProductCode, "product name": product.ProductName, "position asset": product.PositionAssetKey} {
		if value == "" {
			return marketyield.Batch{}, fmt.Errorf("stake pool product %s is empty", name)
		}
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	snapshot, err := c.Reader.Read(ctx, product.PoolConfig)
	if err != nil {
		return marketyield.Batch{}, err
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	current := decimalUint(snapshot.State.TotalLamports).Div(decimalUint(snapshot.State.PoolTokenSupply))
	previous := decimalUint(snapshot.State.LastEpochTotalLamports).Div(decimalUint(snapshot.State.LastEpochPoolTokenSupply))
	rate := current.Div(previous).Sub(decimal.NewFromInt(1)).Mul(epochsPerYear)
	if !current.IsPositive() || !previous.IsPositive() || rate.IsNegative() {
		return marketyield.Batch{}, fmt.Errorf("%s exchange ratios or APR are invalid", product.ProductCode)
	}
	tvl := decimalUint(snapshot.State.TotalLamports).Div(decimal.NewFromInt(1_000_000_000))
	entry := feeDecimal(snapshot.State.SOLDepositFee)
	exit := feeDecimal(snapshot.State.StakeWithdrawalFee)
	performance := feeDecimal(snapshot.State.EpochFee)
	hash := marketyield.HashPayloads(snapshot.Payloads...)
	network, address, exposure, finality := "solana-mainnet", product.Address, SOLAsset, "finalized"
	height, blockHash := snapshot.BlockHeight, snapshot.BlockHash
	route := marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: product.Provider, ProductCode: product.ProductCode, ProductName: product.ProductName, YieldType: "liquid_staking",
		DepositAssetKey: SOLAsset, PositionAssetKey: product.PositionAssetKey, RedeemAssetKey: SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
		IncomeSource: "combined", SourceURL: MainnetRPCURL, CollectionEnabled: true}
	observation := marketyield.YieldObservation{ObservationTime: time.Unix(snapshot.BlockTime, 0).UTC(), CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero,
		TierMode: "none", Rate: &rate, RateKind: "apr", RateOrigin: "derived", RateMode: "variable", RewardAssetKeys: []string{SOLAsset}, RewardComponentRates: []*decimal.Decimal{&rate},
		EntryFeeRate: &entry, ExitFeeRate: &exit, PerformanceFeeRate: &performance, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &current,
		TVL: &tvl, Availability: "unknown", BlockHeight: &height, BlockHash: &blockHash, Finality: &finality, SourcePayloadHash: &hash}
	return marketyield.Batch{Source: "solana-stakepool-" + product.ProductCode, CollectedAt: collected, Items: []marketyield.CollectedYield{{Route: route, Observation: observation}}}, nil
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
