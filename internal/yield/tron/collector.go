package tron

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	maintenanceIntervalMillis int64 = 21_600_000
	trxAsset                        = "tron:mainnet:native:TRX"
)

type SnapshotSource interface {
	Fetch(context.Context) (Snapshot, error)
}

type Collector struct {
	Client SnapshotSource
	Now    func() time.Time
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Client == nil {
		return marketyield.Batch{}, fmt.Errorf("TRON client is required")
	}
	snapshot, err := c.Client.Fetch(ctx)
	if err != nil {
		return marketyield.Batch{}, err
	}
	if err = validateRawPayloads(snapshot); err != nil {
		return marketyield.Batch{}, err
	}
	params := snapshot.Parameters
	maintenance, err := requiredParameter(params, "getMaintenanceTimeInterval")
	if err != nil {
		return marketyield.Batch{}, err
	}
	witnessPay, err := requiredParameter(params, "getWitnessPayPerBlock")
	if err != nil {
		return marketyield.Batch{}, err
	}
	voterPay, err := requiredParameter(params, "getWitness127PayPerBlock")
	if err != nil {
		return marketyield.Batch{}, err
	}
	unfreezeDays, err := requiredParameter(params, "getUnfreezeDelayDays")
	if err != nil {
		return marketyield.Batch{}, err
	}
	if maintenance != maintenanceIntervalMillis {
		return marketyield.Batch{}, fmt.Errorf("unsupported maintenance interval %d", maintenance)
	}
	if uint64(unfreezeDays) > math.MaxUint64/86400 {
		return marketyield.Batch{}, fmt.Errorf("unfreeze delay overflows seconds")
	}
	if len(snapshot.Witnesses) != 127 || len(snapshot.Brokerage) != 127 {
		return marketyield.Batch{}, fmt.Errorf("expected 127 witnesses and brokerages, got %d and %d", len(snapshot.Witnesses), len(snapshot.Brokerage))
	}
	periodStart := time.UnixMilli(snapshot.NextMaintenance - maintenance).UTC()
	periodEnd := time.UnixMilli(snapshot.NextMaintenance).UTC()
	if snapshot.StartBlock.Time.Before(periodStart) || !snapshot.StartBlock.Time.Before(periodEnd) || snapshot.EndBlock.Time.Before(periodStart) || !snapshot.EndBlock.Time.Before(periodEnd) || snapshot.EndBlock.Time.Before(snapshot.StartBlock.Time) || snapshot.EndBlock.Number < snapshot.StartBlock.Number {
		return marketyield.Batch{}, fmt.Errorf("solid block anchors do not share the current maintenance period")
	}
	seen := make(map[string]struct{}, 127)
	votes := make([]decimal.Decimal, 127)
	totalVotes := decimal.Zero
	var previous decimal.Decimal
	for index, witness := range snapshot.Witnesses {
		if !validBase58Address(witness.Address) {
			return marketyield.Batch{}, fmt.Errorf("witness %d has invalid address", index+1)
		}
		if _, ok := seen[witness.Address]; ok {
			return marketyield.Batch{}, fmt.Errorf("duplicate witness %s", witness.Address)
		}
		seen[witness.Address] = struct{}{}
		parsed, parseErr := decimal.NewFromString(witness.VoteCount.String())
		if parseErr != nil || !parsed.IsInteger() || !parsed.IsPositive() {
			return marketyield.Batch{}, fmt.Errorf("witness %d has invalid voteCount", index+1)
		}
		if index > 0 && parsed.GreaterThan(previous) {
			return marketyield.Batch{}, fmt.Errorf("witness voteCount is not non-increasing")
		}
		previous, votes[index] = parsed, parsed
		totalVotes = totalVotes.Add(parsed)
		brokerage, ok := snapshot.Brokerage[witness.Address]
		if !ok || brokerage < 0 || brokerage > 100 {
			return marketyield.Batch{}, fmt.Errorf("invalid brokerage for %s", witness.Address)
		}
	}
	if !totalVotes.IsPositive() {
		return marketyield.Batch{}, fmt.Errorf("total votes must be positive")
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	anchorHeight, anchorHash, finality := snapshot.EndBlock.Number, snapshot.EndBlock.ID, "finalized_anchor"
	items := make([]marketyield.CollectedYield, 0, 127)
	for index, witness := range snapshot.Witnesses {
		brokerage := snapshot.Brokerage[witness.Address]
		rate, calcErr := APR(index+1, votes[index], totalVotes, brokerage, witnessPay, voterPay)
		if calcErr != nil {
			return marketyield.Batch{}, calcErr
		}
		name := strings.TrimSpace(witness.URL)
		if name == "" {
			name = shortAddress(witness.Address)
		}
		network, exposure := "tron-mainnet", "TRX"
		route := marketyield.YieldRouteDefinition{ProviderType: "native", Provider: "TRON", ProductCode: "sr:" + witness.Address, ProductName: "TRON vote for " + name, YieldType: "native_staking", DepositAssetKey: trxAsset, PositionAssetKey: trxAsset, RedeemAssetKey: trxAsset, Network: &network, PriceExposureAsset: &exposure, IncomeSource: "issuance", SourceURL: "https://developers.tron.network/docs/reward-calculation", CollectionEnabled: true}
		hash := marketyield.HashPayloads(marketyield.Payload{Name: "maintenance:start", Body: snapshot.RawMaintenanceStart}, marketyield.Payload{Name: "block:start", Body: snapshot.RawStartBlock}, marketyield.Payload{Name: "witnesses", Body: snapshot.RawWitnesses}, marketyield.Payload{Name: "chain-parameters", Body: snapshot.RawParameters}, marketyield.Payload{Name: "brokerage:" + witness.Address, Body: snapshot.RawBrokerage[witness.Address]}, marketyield.Payload{Name: "block:end", Body: snapshot.RawEndBlock}, marketyield.Payload{Name: "maintenance:end", Body: snapshot.RawMaintenanceEnd})
		unbonding := uint64(unfreezeDays) * 86400
		observation := marketyield.YieldObservation{ObservationTime: time.UnixMilli(snapshot.EndBlock.Time.UnixMilli()).UTC(), CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate, RateKind: "apr", RateOrigin: "derived", RateMode: "variable", RewardAssetKeys: []string{trxAsset}, RewardComponentRates: []*decimal.Decimal{&rate}, UnbondingSeconds: &unbonding, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: marketyield.Ptr(decimal.NewFromInt(1)), TVL: &votes[index], Availability: "available", BlockHeight: &anchorHeight, BlockHash: &anchorHash, Finality: &finality, SourcePayloadHash: &hash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	return marketyield.Batch{Source: "tron-native-staking", CollectedAt: collected, Items: items}, nil
}

func requiredParameter(parameters map[string]int64, key string) (int64, error) {
	value, ok := parameters[key]
	if !ok || value < 0 {
		return 0, fmt.Errorf("missing or negative chain parameter %s", key)
	}
	return value, nil
}
func validBase58Address(value string) bool {
	if len(value) != 34 || value[0] != 'T' {
		return false
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	number := new(big.Int)
	base := big.NewInt(58)
	for _, char := range value {
		index := strings.IndexRune(alphabet, char)
		if index < 0 {
			return false
		}
		number.Mul(number, base)
		number.Add(number, big.NewInt(int64(index)))
	}
	decoded := number.Bytes()
	leadingZeros := 0
	for leadingZeros < len(value) && value[leadingZeros] == '1' {
		leadingZeros++
	}
	if leadingZeros > 0 {
		decoded = append(make([]byte, leadingZeros), decoded...)
	}
	if len(decoded) != 25 || decoded[0] != 0x41 {
		return false
	}
	first := sha256.Sum256(decoded[:21])
	second := sha256.Sum256(first[:])
	for index := 0; index < 4; index++ {
		if decoded[21+index] != second[index] {
			return false
		}
	}
	return true
}

func validateRawPayloads(snapshot Snapshot) error {
	for name, body := range map[string][]byte{"maintenance:start": snapshot.RawMaintenanceStart, "block:start": snapshot.RawStartBlock, "witnesses": snapshot.RawWitnesses, "chain-parameters": snapshot.RawParameters, "block:end": snapshot.RawEndBlock, "maintenance:end": snapshot.RawMaintenanceEnd} {
		if len(body) == 0 {
			return fmt.Errorf("TRON snapshot is missing raw payload %s", name)
		}
	}
	for _, witness := range snapshot.Witnesses {
		if len(snapshot.RawBrokerage[witness.Address]) == 0 {
			return fmt.Errorf("TRON snapshot is missing raw brokerage for %s", witness.Address)
		}
	}
	return nil
}
func shortAddress(value string) string {
	if len(value) < 12 {
		return value
	}
	return value[:6] + "..." + value[len(value)-6:]
}
