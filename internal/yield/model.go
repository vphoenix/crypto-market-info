package yield

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type YieldRouteDefinition struct {
	ID                 uint32
	ProviderType       string
	Provider           string
	ProductCode        string
	ProductName        string
	YieldType          string
	DepositAssetKey    string
	PositionAssetKey   string
	RedeemAssetKey     string
	Network            *string
	ContractAddress    *string
	PriceExposureAsset *string
	IncomeSource       string
	SourceURL          string
	CollectionEnabled  bool
}

type YieldObservation struct {
	ObservationTime         time.Time
	CollectedAt             time.Time
	TierNo                  uint16
	TierMinAmount           decimal.Decimal
	TierMaxAmount           *decimal.Decimal
	TierMode                string
	Rate                    *decimal.Decimal
	RateKind                string
	RateOrigin              string
	RateMode                string
	RewardAssetKeys         []string
	RewardComponentRates    []*decimal.Decimal
	EntryFeeRate            *decimal.Decimal
	ExitFeeRate             *decimal.Decimal
	FixedPenaltyRate        *decimal.Decimal
	PerformanceFeeRate      *decimal.Decimal
	EntryFeeAmount          *decimal.Decimal
	ExitFeeAmount           *decimal.Decimal
	FixedFeeAssetKey        *string
	LockSeconds             uint64
	UnbondingSeconds        *uint64
	RedemptionWindowSeconds *uint64
	RulePrincipalLossMode   string
	FixedPrincipalLossRate  *decimal.Decimal
	RuleEligibility         string
	EligibilityReason       *string
	ExposureRatio           *decimal.Decimal
	Capacity                *decimal.Decimal
	RemainingCapacity       *decimal.Decimal
	TVL                     *decimal.Decimal
	PoolCash                *decimal.Decimal
	Availability            string
	BlockHeight             *uint64
	BlockHash               *string
	Finality                *string
	SourcePayloadHash       *string
}

type CollectedYield struct {
	Route       YieldRouteDefinition
	Observation YieldObservation
}

type Batch struct {
	Source      string
	CollectedAt time.Time
	Items       []CollectedYield
}

type Collector interface {
	Collect(context.Context) (Batch, error)
}

type Sink interface {
	WriteYieldBatch(context.Context, Batch) error
}

func (r YieldRouteDefinition) Identity() string {
	return strings.Join([]string{r.Provider, r.ProductCode, stringValue(r.Network), stringValue(r.ContractAddress), r.DepositAssetKey, r.PositionAssetKey, r.RedeemAssetKey}, "\x00")
}

func (r YieldRouteDefinition) SameDefinition(other YieldRouteDefinition) bool {
	r.ID, other.ID = 0, 0
	// Display names, source URLs, and collection_enabled are mutable metadata.
	// Identity and economic semantics must remain stable for a reused route ID.
	return r.Identity() == other.Identity() && r.ProviderType == other.ProviderType && r.YieldType == other.YieldType &&
		stringValue(r.PriceExposureAsset) == stringValue(other.PriceExposureAsset) && r.IncomeSource == other.IncomeSource
}

func (r YieldRouteDefinition) SameMetadata(other YieldRouteDefinition) bool {
	return r.ProductName == other.ProductName && r.SourceURL == other.SourceURL && r.CollectionEnabled == other.CollectionEnabled
}

func (r YieldRouteDefinition) ValidateDefinition() error {
	if r.ID != 0 {
		return fmt.Errorf("route definition must not contain an id")
	}
	if !oneOf(r.ProviderType, "native", "cex", "protocol") {
		return fmt.Errorf("invalid provider_type %q", r.ProviderType)
	}
	for name, value := range map[string]string{"provider": r.Provider, "product_code": r.ProductCode, "product_name": r.ProductName, "deposit_asset_key": r.DepositAssetKey, "position_asset_key": r.PositionAssetKey, "redeem_asset_key": r.RedeemAssetKey, "source_url": r.SourceURL} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if !oneOf(r.YieldType, "native_staking", "liquid_staking", "lending", "fee_share", "resource_rental", "single_asset_incentive", "stablecoin_savings", "cex_earn") {
		return fmt.Errorf("invalid yield_type %q", r.YieldType)
	}
	if !oneOf(r.IncomeSource, "issuance", "borrow_interest", "protocol_fee", "resource_rent", "offchain_interest", "subsidy", "combined") {
		return fmt.Errorf("invalid income_source %q", r.IncomeSource)
	}
	if r.Network != nil && strings.TrimSpace(*r.Network) == "" {
		return fmt.Errorf("network cannot be empty")
	}
	if r.ContractAddress != nil && strings.TrimSpace(*r.ContractAddress) == "" {
		return fmt.Errorf("contract_address cannot be empty")
	}
	return nil
}

func (b *Batch) NormalizeAndValidate() error {
	b.CollectedAt = millisecondUTC(b.CollectedAt)
	if strings.TrimSpace(b.Source) == "" || b.CollectedAt.IsZero() || len(b.Items) == 0 {
		return fmt.Errorf("yield batch requires source, collected_at, and at least one item")
	}
	seen := make(map[string]struct{}, len(b.Items))
	definitions := make(map[string]YieldRouteDefinition, len(b.Items))
	for index := range b.Items {
		item := &b.Items[index]
		item.Route.ID = 0
		item.Observation.CollectedAt = millisecondUTC(item.Observation.CollectedAt)
		item.Observation.ObservationTime = millisecondUTC(item.Observation.ObservationTime)
		if !item.Observation.CollectedAt.Equal(b.CollectedAt) {
			return fmt.Errorf("item %d collected_at differs from batch", index)
		}
		if err := item.Route.ValidateDefinition(); err != nil {
			return fmt.Errorf("item %d route: %w", index, err)
		}
		identity := item.Route.Identity()
		if previous, ok := definitions[identity]; ok && (!item.Route.SameDefinition(previous) || !item.Route.SameMetadata(previous)) {
			return fmt.Errorf("item %d route differs from another observation of the same identity", index)
		}
		definitions[identity] = item.Route
		key := identity + fmt.Sprintf("\x00%d\x00%d", item.Observation.ObservationTime.UnixMilli(), item.Observation.TierNo)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("item %d duplicates route identity, observation time, and tier", index)
		}
		seen[key] = struct{}{}
		if err := item.Observation.Validate(); err != nil {
			return fmt.Errorf("item %d observation: %w", index, err)
		}
	}
	return nil
}

// NormalizeAndValidateForLiveCollection applies the additional evidence rule
// for batches produced by an automatic, real-time collector. The ordinary
// NormalizeAndValidate path intentionally permits a nil payload hash so that
// historical migrations and explicit manual imports remain possible.
func (b *Batch) NormalizeAndValidateForLiveCollection() error {
	if err := b.NormalizeAndValidate(); err != nil {
		return err
	}
	for index := range b.Items {
		if b.Items[index].Observation.SourcePayloadHash == nil {
			return fmt.Errorf("item %d observation: source_payload_hash is required for live collection", index)
		}
	}
	return nil
}

func (o YieldObservation) Validate() error {
	if o.ObservationTime.IsZero() || o.CollectedAt.IsZero() || o.TierNo == 0 {
		return fmt.Errorf("observation_time, collected_at and positive tier_no are required")
	}
	if !o.ObservationTime.Equal(millisecondUTC(o.ObservationTime)) || !o.CollectedAt.Equal(millisecondUTC(o.CollectedAt)) {
		return fmt.Errorf("times must be UTC millisecond precision")
	}
	if o.TierMinAmount.IsNegative() || (o.TierMaxAmount != nil && (o.TierMaxAmount.IsNegative() || o.TierMaxAmount.LessThan(o.TierMinAmount))) {
		return fmt.Errorf("invalid tier bounds")
	}
	if !oneOf(o.TierMode, "none", "marginal", "whole_balance", "unknown") || !oneOf(o.RateKind, "apr", "apy", "unknown") || !oneOf(o.RateOrigin, "reported", "derived") || !oneOf(o.RateMode, "fixed", "variable", "unknown") {
		return fmt.Errorf("invalid tier or rate enum")
	}
	if o.Rate != nil && o.Rate.IsNegative() {
		return fmt.Errorf("rate cannot be negative")
	}
	if len(o.RewardAssetKeys) != len(o.RewardComponentRates) {
		return fmt.Errorf("reward arrays differ in length")
	}
	for index, key := range o.RewardAssetKeys {
		if strings.TrimSpace(key) == "" || (o.RewardComponentRates[index] != nil && o.RewardComponentRates[index].IsNegative()) {
			return fmt.Errorf("invalid reward component %d", index)
		}
	}
	for name, value := range map[string]*decimal.Decimal{"entry_fee_rate": o.EntryFeeRate, "exit_fee_rate": o.ExitFeeRate, "fixed_penalty_rate": o.FixedPenaltyRate, "performance_fee_rate": o.PerformanceFeeRate, "entry_fee_amount": o.EntryFeeAmount, "exit_fee_amount": o.ExitFeeAmount, "fixed_principal_loss_rate": o.FixedPrincipalLossRate, "capacity": o.Capacity, "remaining_capacity": o.RemainingCapacity, "tvl": o.TVL} {
		if value != nil && value.IsNegative() {
			return fmt.Errorf("%s cannot be negative", name)
		}
	}
	for name, value := range map[string]*decimal.Decimal{"entry_fee_rate": o.EntryFeeRate, "exit_fee_rate": o.ExitFeeRate, "fixed_penalty_rate": o.FixedPenaltyRate, "performance_fee_rate": o.PerformanceFeeRate, "fixed_principal_loss_rate": o.FixedPrincipalLossRate} {
		if value != nil && value.GreaterThan(decimal.NewFromInt(1)) {
			return fmt.Errorf("%s cannot exceed one", name)
		}
	}
	if o.ExposureRatio != nil && !o.ExposureRatio.IsPositive() {
		return fmt.Errorf("exposure_ratio must be positive")
	}
	if o.PoolCash != nil && (o.PoolCash.IsNegative() || o.PoolCash.GreaterThanOrEqual(decimal.New(1, 20)) || !o.PoolCash.Equal(o.PoolCash.Truncate(18))) {
		return fmt.Errorf("pool_cash must fit non-negative Decimal(38,18)")
	}
	if o.FixedFeeAssetKey != nil && strings.TrimSpace(*o.FixedFeeAssetKey) == "" {
		return fmt.Errorf("fixed_fee_asset_key cannot be empty")
	}
	if !oneOf(o.RulePrincipalLossMode, "none", "fixed", "variable", "unknown") || !oneOf(o.RuleEligibility, "candidate", "rejected", "unknown") {
		return fmt.Errorf("invalid rule enum")
	}
	if o.RulePrincipalLossMode == "variable" && o.RuleEligibility != "rejected" {
		return fmt.Errorf("variable principal loss must be rejected")
	}
	if o.RulePrincipalLossMode == "fixed" && o.FixedPrincipalLossRate == nil {
		return fmt.Errorf("fixed principal loss requires a rate")
	}
	if o.RulePrincipalLossMode != "fixed" && o.FixedPrincipalLossRate != nil {
		return fmt.Errorf("fixed principal loss rate requires fixed mode")
	}
	if !oneOf(o.Availability, "available", "paused", "closed", "unavailable", "unknown") {
		return fmt.Errorf("invalid availability %q", o.Availability)
	}
	if (o.BlockHeight == nil) != (o.BlockHash == nil) {
		return fmt.Errorf("block height and hash must both be present or absent")
	}
	if o.BlockHash != nil && strings.TrimSpace(*o.BlockHash) == "" {
		return fmt.Errorf("block_hash cannot be empty")
	}
	if o.Finality != nil && !oneOf(*o.Finality, "unfinalized", "safe", "finalized", "finalized_anchor") {
		return fmt.Errorf("invalid finality %q", *o.Finality)
	}
	if o.Finality != nil && o.BlockHeight == nil {
		return fmt.Errorf("finality requires a block anchor")
	}
	if o.SourcePayloadHash != nil && !isLowerSHA256Hex(*o.SourcePayloadHash) {
		return fmt.Errorf("source_payload_hash must be 64 lowercase hexadecimal characters")
	}
	return nil
}

func Ptr[T any](value T) *T { return &value }

type Payload struct {
	Name string
	Body []byte
}

func HashPayloads(payloads ...Payload) string {
	hash := sha256.New()
	var length [8]byte
	for _, payload := range payloads {
		binary.BigEndian.PutUint64(length[:], uint64(len(payload.Name)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(payload.Name))
		binary.BigEndian.PutUint64(length[:], uint64(len(payload.Body)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(payload.Body)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func DecodeJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JSON response contains more than one value")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func millisecondUTC(value time.Time) time.Time { return time.UnixMilli(value.UTC().UnixMilli()).UTC() }
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func isLowerSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
