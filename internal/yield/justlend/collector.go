package justlend

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	trxAsset          = "tron:mainnet:native:TRX"
	usddAsset         = "tron:mainnet:trc20:TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz"
	strxAddress       = "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5"
	jtrxAddress       = "TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwP"
	jstrxAddress      = "TJQ9rbVe9ei3nNtyGgBL22Fuu2xYjZaLAQ"
	vaultAddress      = "THpxp8RpCUGk55dV7oL1LfxDeP9QvouxmM"
	nativePlaceholder = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
	wtrxAddress       = "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR"
)

var plainDecimal = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?$`)

type SnapshotSource interface {
	Fetch(context.Context) (Snapshot, error)
}

type Collector struct {
	Client SnapshotSource
	Now    func() time.Time
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Client == nil {
		return marketyield.Batch{}, fmt.Errorf("JustLend client is required")
	}
	snapshot, err := c.Client.Fetch(ctx)
	if err != nil {
		return marketyield.Batch{}, err
	}
	if len(snapshot.RawSTRX) == 0 || len(snapshot.RawJToken) == 0 || len(snapshot.RawMining) == 0 || len(snapshot.RawVault) == 0 {
		return marketyield.Batch{}, fmt.Errorf("JustLend snapshot is missing raw source payload")
	}
	if err = validateSTRX(snapshot.STRX.StakeInfo); err != nil {
		return marketyield.Batch{}, err
	}
	jtrx, jtrxFound, err := oneJToken(snapshot.JTokens, jtrxAddress)
	if err != nil {
		return marketyield.Batch{}, err
	}
	jstrx, jstrxFound, err := oneJToken(snapshot.JTokens, jstrxAddress)
	if err != nil {
		return marketyield.Batch{}, err
	}
	if jtrxFound {
		if err = validateJTRX(jtrx); err != nil {
			return marketyield.Batch{}, err
		}
	}
	if jstrxFound {
		if err = validateJSTRX(jstrx); err != nil {
			return marketyield.Batch{}, err
		}
	}
	vault, vaultFound, err := oneVault(snapshot.Vaults, vaultAddress)
	if err != nil {
		return marketyield.Batch{}, err
	}
	if vaultFound {
		if err = validateVault(vault); err != nil {
			return marketyield.Batch{}, err
		}
	}
	jtrxMining, err := miningAPY(snapshot.Mining, jtrxAddress)
	if err != nil {
		return marketyield.Batch{}, err
	}
	jstrxMining, err := miningAPY(snapshot.Mining, jstrxAddress)
	if err != nil {
		return marketyield.Batch{}, err
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	strxRate, _ := parseDecimal(snapshot.STRX.StakeInfo.SupplyRate)
	strxExchange, _ := parsePositive(snapshot.STRX.StakeInfo.ExchangeRate)
	strxTVL, _ := parseDecimal(snapshot.STRX.StakeInfo.TotalUnderlying)
	items := []marketyield.CollectedYield{
		makeItem(route("strx", "JustLend sTRX", "liquid_staking", "combined", strxAddress, strxAddress), baseObservation(collected, &strxRate, "reported", []string{trxAsset}, []*decimal.Decimal{&strxRate}, &strxExchange, &strxTVL, "none", "candidate", nil, 14*86400, sourceHash(marketyield.Payload{Name: "/lend/strx", Body: snapshot.RawSTRX}))),
	}
	items = append(items, lendingItem("jtrx", "JustLend jTRX", jtrxAddress, jtrxFound, jtrx, jtrxMining, decimal.Zero, collected,
		sourceHash(marketyield.Payload{Name: "/lend/jtoken", Body: snapshot.RawJToken}, marketyield.Payload{Name: "/mining/apy", Body: snapshot.RawMining}), false, strxExchange))
	items = append(items, lendingItem("strx-jstrx", "JustLend sTRX to jsTRX", jstrxAddress, jstrxFound, jstrx, jstrxMining, strxRate, collected,
		sourceHash(marketyield.Payload{Name: "/lend/strx", Body: snapshot.RawSTRX}, marketyield.Payload{Name: "/lend/jtoken", Body: snapshot.RawJToken}, marketyield.Payload{Name: "/mining/apy", Body: snapshot.RawMining}), true, strxExchange))
	items = append(items, vaultItem(vault, vaultFound, collected, snapshot.V2Time, sourceHash(marketyield.Payload{Name: "/v2/index/vault/list", Body: snapshot.RawVault})))
	return marketyield.Batch{Source: "justlend", CollectedAt: collected, Items: items}, nil
}

func makeItem(route marketyield.YieldRouteDefinition, observation marketyield.YieldObservation) marketyield.CollectedYield {
	return marketyield.CollectedYield{Route: route, Observation: observation}
}

func route(code, name, kind, income, contract, position string) marketyield.YieldRouteDefinition {
	network, exposure := "tron-mainnet", "TRX"
	return marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "JustLend", ProductCode: code, ProductName: name, YieldType: kind,
		DepositAssetKey: trxAsset, PositionAssetKey: "tron:mainnet:trc20:" + position, RedeemAssetKey: trxAsset, Network: &network,
		ContractAddress: &contract, PriceExposureAsset: &exposure, IncomeSource: income, SourceURL: "https://docs.justlend.org/developers/apis/", CollectionEnabled: true}
}

func baseObservation(at time.Time, rate *decimal.Decimal, origin string, rewardKeys []string, components []*decimal.Decimal, exposure, tvl *decimal.Decimal, loss, eligibility string, reason *string, unbonding uint64, hash *string) marketyield.YieldObservation {
	var unbondingSeconds *uint64
	if unbonding > 0 {
		unbondingSeconds = &unbonding
	} else {
		zero := uint64(0)
		unbondingSeconds = &zero
	}
	return marketyield.YieldObservation{ObservationTime: at, CollectedAt: at, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: rate, RateKind: "apy", RateOrigin: origin, RateMode: "variable",
		RewardAssetKeys: rewardKeys, RewardComponentRates: components, UnbondingSeconds: unbondingSeconds, RulePrincipalLossMode: loss, RuleEligibility: eligibility,
		EligibilityReason: reason, ExposureRatio: exposure, TVL: tvl, Availability: "available", SourcePayloadHash: hash}
}

func lendingItem(code, name, address string, found bool, token JToken, mining, base decimal.Decimal, at time.Time, hash *string, combined bool, strxExchange decimal.Decimal) marketyield.CollectedYield {
	reason := "collateral_bad_debt"
	if !found {
		observation := baseObservation(at, nil, "derived", []string{trxAsset}, []*decimal.Decimal{nil}, nil, nil, "variable", "rejected", &reason, map[bool]uint64{true: 14 * 86400}[combined], hash)
		observation.Availability = "unavailable"
		return makeItem(route(code, name, "lending", map[bool]string{true: "combined", false: "borrow_interest"}[combined], address, address), observation)
	}
	supply, _ := parseDecimal(token.SupplyRate)
	exchangeRate, _ := parsePositive(token.ExchangeRate)
	totalSupply, _ := parseDecimal(token.TotalSupply)
	exposure := exchangeRate
	tvl := totalSupply.Mul(exchangeRate)
	trxComponent := supply.Add(base)
	if combined {
		exposure = exchangeRate.Mul(strxExchange)
		tvl = totalSupply.Mul(exposure)
	}
	total := trxComponent.Add(mining)
	keys := []string{trxAsset}
	components := []*decimal.Decimal{marketyield.Ptr(trxComponent)}
	if !mining.IsZero() {
		keys = append(keys, usddAsset)
		components = append(components, marketyield.Ptr(mining))
	}
	observation := baseObservation(at, &total, "derived", keys, components, &exposure, &tvl, "variable", "rejected", &reason, map[bool]uint64{true: 14 * 86400}[combined], hash)
	return makeItem(route(code, name, "lending", map[bool]string{true: "combined", false: "borrow_interest"}[combined], address, address), observation)
}

func vaultItem(vault Vault, found bool, collected, observationTime time.Time, hash *string) marketyield.CollectedYield {
	reason := "collateral_bad_debt"
	item := makeItem(route("trx-v2-vault", "JustLend V2 TRX Vault", "lending", "borrow_interest", vaultAddress, vaultAddress), baseObservation(collected, nil, "reported", []string{trxAsset}, []*decimal.Decimal{nil}, nil, nil, "variable", "rejected", &reason, 0, hash))
	item.Observation.ObservationTime = time.UnixMilli(observationTime.UTC().UnixMilli()).UTC()
	if !found {
		item.Observation.Availability = "unavailable"
		return item
	}
	rate, _ := parseDecimal(vault.APY)
	tvl, _ := parseDecimal(vault.TotalSupplyAmount)
	feePercent, _ := parseDecimal(vault.PerformanceFee)
	fee := feePercent.Div(decimal.NewFromInt(100))
	item.Observation.Rate = &rate
	item.Observation.RewardComponentRates = []*decimal.Decimal{&rate}
	item.Observation.TVL = &tvl
	item.Observation.PerformanceFeeRate = &fee
	return item
}

func validateSTRX(item STRXStakeInfo) error {
	if item.Address != strxAddress || item.Symbol != "sTRX" || item.Decimals != "18" || item.UnderlyingDecimal != "6" {
		return fmt.Errorf("unexpected sTRX identity")
	}
	for name, value := range map[string]string{"supplyRate": item.SupplyRate, "exchangeRate": item.ExchangeRate, "totalUnderlying": item.TotalUnderlying, "totalSupply": item.TotalSupply} {
		if _, err := parseDecimal(value); err != nil {
			return fmt.Errorf("sTRX %s: %w", name, err)
		}
	}
	if _, err := parsePositive(item.ExchangeRate); err != nil {
		return fmt.Errorf("sTRX exchangeRate: %w", err)
	}
	return nil
}
func validateJTRX(item JToken) error {
	if item.Symbol != "jTRX" || item.UnderlyingSymbol != "TRX" || item.UnderlyingAddress != nativePlaceholder || item.UnderlyingDecimal != 6 {
		return fmt.Errorf("unexpected jTRX identity")
	}
	return validateJTokenNumbers(item)
}
func validateJSTRX(item JToken) error {
	if item.Symbol != "jsTRX" || item.UnderlyingSymbol != "sTRX" || item.UnderlyingAddress != strxAddress || item.UnderlyingDecimal != 18 {
		return fmt.Errorf("unexpected jsTRX identity")
	}
	return validateJTokenNumbers(item)
}
func validateJTokenNumbers(item JToken) error {
	for name, value := range map[string]string{"supplyRate": item.SupplyRate, "exchangeRate": item.ExchangeRate, "totalSupply": item.TotalSupply} {
		if _, err := parseDecimal(value); err != nil {
			return fmt.Errorf("%s %s: %w", item.Symbol, name, err)
		}
	}
	if _, err := parsePositive(item.ExchangeRate); err != nil {
		return err
	}
	return nil
}
func validateVault(item Vault) error {
	if item.Chain != "tron" || item.Symbol != "jTRXv2" || item.AssetSymbol != "WTRX" || item.AssetAddress != wtrxAddress || item.AssetDecimals != 6 {
		return fmt.Errorf("unexpected TRX vault identity")
	}
	for name, value := range map[string]string{"apy": item.APY, "totalSupplyAmount": item.TotalSupplyAmount, "performanceFee": item.PerformanceFee} {
		value, err := parseDecimal(value)
		if err != nil {
			return fmt.Errorf("vault %s: %w", name, err)
		}
		if name == "performanceFee" && value.GreaterThan(decimal.NewFromInt(100)) {
			return fmt.Errorf("vault performanceFee exceeds 100")
		}
	}
	return nil
}
func oneJToken(items []JToken, address string) (JToken, bool, error) {
	var found *JToken
	for index := range items {
		if items[index].Address == address {
			if found != nil {
				return JToken{}, false, fmt.Errorf("duplicate jToken %s", address)
			}
			found = &items[index]
		}
	}
	if found == nil {
		return JToken{}, false, nil
	}
	return *found, true, nil
}
func oneVault(items []Vault, address string) (Vault, bool, error) {
	var found *Vault
	for index := range items {
		if items[index].Address == address {
			if found != nil {
				return Vault{}, false, fmt.Errorf("duplicate vault %s", address)
			}
			found = &items[index]
		}
	}
	if found == nil {
		return Vault{}, false, nil
	}
	return *found, true, nil
}
func miningAPY(data map[string]map[string]string, address string) (decimal.Decimal, error) {
	rewards, ok := data[address]
	if !ok {
		return decimal.Zero, nil
	}
	for key := range rewards {
		if key != "USDD" {
			return decimal.Zero, fmt.Errorf("unexpected mining reward %q for %s", key, address)
		}
	}
	raw, ok := rewards["USDD"]
	if !ok {
		return decimal.Zero, nil
	}
	return parseDecimal(raw)
}
func parseDecimal(raw string) (decimal.Decimal, error) {
	if !plainDecimal.MatchString(raw) {
		return decimal.Decimal{}, fmt.Errorf("%q is not a non-negative ordinary decimal", raw)
	}
	value, err := decimal.NewFromString(raw)
	if err != nil || value.IsNegative() {
		return decimal.Decimal{}, fmt.Errorf("invalid decimal %q", raw)
	}
	return value, nil
}
func parsePositive(raw string) (decimal.Decimal, error) {
	value, err := parseDecimal(raw)
	if err != nil {
		return value, err
	}
	if !value.IsPositive() {
		return value, fmt.Errorf("value must be positive")
	}
	return value, nil
}
