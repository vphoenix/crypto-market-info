package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const BookDepth = 50

type MarketType string

const (
	MarketSpot      MarketType = "spot"
	MarketPerpetual MarketType = "perpetual"
	MarketDelivery  MarketType = "delivery"
)

type Instrument struct {
	ID                   uint32
	Exchange             string
	MarketType           MarketType
	ExchangeSymbol       string
	VenueContractVersion string
	BaseAsset            string
	QuoteAsset           string
	SettleAsset          *string
	ContractMultiplier   decimal.Decimal
	PriceTickSize        decimal.Decimal
	QuantityStepSize     decimal.Decimal
	ExpiryTime           *time.Time
}

func (i Instrument) Validate() error {
	if i.ID == 0 {
		return fmt.Errorf("instrument_id must be greater than zero")
	}
	return i.ValidateDefinition()
}

func (i Instrument) ValidateDefinition() error {
	for name, value := range map[string]string{
		"exchange": i.Exchange, "exchange_symbol": i.ExchangeSymbol,
		"base_asset": i.BaseAsset, "quote_asset": i.QuoteAsset,
	} {
		if value == "" || strings.TrimSpace(value) != value {
			return fmt.Errorf("%s must be an exact non-empty string", name)
		}
	}
	if i.VenueContractVersion != "" && strings.TrimSpace(i.VenueContractVersion) != i.VenueContractVersion {
		return fmt.Errorf("venue_contract_version must be an exact string")
	}
	switch i.MarketType {
	case MarketSpot, MarketPerpetual, MarketDelivery:
	default:
		return fmt.Errorf("unsupported market_type %q", i.MarketType)
	}
	if i.MarketType == MarketSpot {
		if i.SettleAsset != nil || i.ExpiryTime != nil || !i.ContractMultiplier.Equal(decimal.NewFromInt(1)) {
			return fmt.Errorf("spot instrument must have no settle/expiry and multiplier 1")
		}
	} else if i.SettleAsset == nil || *i.SettleAsset == "" || strings.TrimSpace(*i.SettleAsset) != *i.SettleAsset {
		return fmt.Errorf("derivative instrument requires an exact settle_asset")
	}
	if i.MarketType != MarketDelivery && i.ExpiryTime != nil {
		return fmt.Errorf("only delivery instruments may have expiry_time")
	}
	for name, value := range map[string]decimal.Decimal{
		"contract_multiplier": i.ContractMultiplier,
		"price_tick_size":     i.PriceTickSize,
		"quantity_step_size":  i.QuantityStepSize,
	} {
		if !value.IsPositive() {
			return fmt.Errorf("%s must be positive", name)
		}
		if value.Exponent() < -18 {
			return fmt.Errorf("%s exceeds Decimal(38,18) scale", name)
		}
	}
	return nil
}

func (i Instrument) SameDefinition(other Instrument) bool {
	settleEqual := (i.SettleAsset == nil && other.SettleAsset == nil) ||
		(i.SettleAsset != nil && other.SettleAsset != nil && *i.SettleAsset == *other.SettleAsset)
	expiryEqual := (i.ExpiryTime == nil && other.ExpiryTime == nil) ||
		(i.ExpiryTime != nil && other.ExpiryTime != nil && i.ExpiryTime.UTC().Equal(other.ExpiryTime.UTC()))
	return i.Exchange == other.Exchange && i.MarketType == other.MarketType &&
		i.ExchangeSymbol == other.ExchangeSymbol && i.VenueContractVersion == other.VenueContractVersion && i.BaseAsset == other.BaseAsset &&
		i.QuoteAsset == other.QuoteAsset && settleEqual && expiryEqual &&
		i.ContractMultiplier.Equal(other.ContractMultiplier) &&
		i.PriceTickSize.Equal(other.PriceTickSize) &&
		i.QuantityStepSize.Equal(other.QuantityStepSize)
}

type Level struct {
	PriceTick int64
	QtyLot    uint64
}

type BookSnapshot struct {
	InstrumentID uint32
	SourceTime   time.Time
	Sequence     int64
	Bids         []Level
	Asks         []Level
}

func (s BookSnapshot) Validate(depth int) error {
	if s.InstrumentID == 0 || s.SourceTime.IsZero() {
		return fmt.Errorf("book snapshot requires instrument and source time")
	}
	if depth <= 0 || len(s.Bids) > depth || len(s.Asks) > depth || len(s.Bids) == 0 || len(s.Asks) == 0 {
		return fmt.Errorf("book snapshot sides must contain between 1 and %d levels", depth)
	}
	if err := validateLevels(s.Bids, true); err != nil {
		return fmt.Errorf("bids: %w", err)
	}
	if err := validateLevels(s.Asks, false); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	if s.Bids[0].PriceTick >= s.Asks[0].PriceTick {
		return fmt.Errorf("book is locked or crossed")
	}
	return nil
}

func validateLevels(levels []Level, bids bool) error {
	for index, level := range levels {
		if level.PriceTick <= 0 || level.QtyLot == 0 {
			return fmt.Errorf("level %d must have positive price and quantity", index)
		}
		if index == 0 {
			continue
		}
		if bids && levels[index-1].PriceTick <= level.PriceTick {
			return fmt.Errorf("prices are not strictly descending")
		}
		if !bids && levels[index-1].PriceTick >= level.PriceTick {
			return fmt.Errorf("prices are not strictly ascending")
		}
	}
	return nil
}

type BookDelta struct {
	MinuteID       uint64
	SecondOffset   uint8
	BidChangePrice []int64
	BidChangeQty   []uint64
	AskChangePrice []int64
	AskChangeQty   []uint64
}

type MinuteBook struct {
	ID           uint64
	InstrumentID uint32
	MinuteTime   time.Time
	ValidBitmap  uint64
	Bids         [BookDepth]Level
	Asks         [BookDepth]Level
}

type MinuteBatch struct {
	Minute MinuteBook
	Deltas []BookDelta
}

func MinuteID(instrumentID uint32, t time.Time) (uint64, error) {
	if instrumentID == 0 {
		return 0, fmt.Errorf("instrument_id must be greater than zero")
	}
	unixMinute := t.UTC().Unix() / 60
	if unixMinute < 0 || unixMinute > int64(^uint32(0)) {
		return 0, fmt.Errorf("UTC minute is outside minute_id range")
	}
	return uint64(unixMinute)<<32 | uint64(instrumentID), nil
}

func MinuteTimeFromID(id uint64) time.Time {
	return time.Unix(int64(id>>32)*60, 0).UTC()
}

type FundingRate struct {
	InstrumentID uint32
	HourTime     time.Time
	FundingTime  time.Time
	Rate         decimal.Decimal
	IsActual     bool
}

func (f FundingRate) Validate() error {
	if f.InstrumentID == 0 || f.HourTime.IsZero() || f.FundingTime.IsZero() {
		return fmt.Errorf("funding rate requires instrument, hour_time and funding_time")
	}
	if !f.HourTime.Equal(f.HourTime.UTC().Truncate(time.Hour)) {
		return fmt.Errorf("hour_time must be an exact UTC hour")
	}
	if !f.FundingTime.Equal(time.UnixMilli(f.FundingTime.UnixMilli()).UTC()) {
		return fmt.Errorf("funding_time must have millisecond precision")
	}
	if f.Rate.Exponent() < -18 {
		return fmt.Errorf("funding rate exceeds Decimal(38,18) scale")
	}
	return nil
}

type PendingFundingConfirmation struct {
	InstrumentID uint32
	FundingTime  time.Time
}

func (p PendingFundingConfirmation) Validate() error {
	if p.InstrumentID == 0 || p.FundingTime.IsZero() {
		return fmt.Errorf("pending funding confirmation requires instrument and funding_time")
	}
	if !p.FundingTime.Equal(time.UnixMilli(p.FundingTime.UnixMilli()).UTC()) {
		return fmt.Errorf("pending funding confirmation time must have millisecond precision")
	}
	return nil
}

type FundingEstimate struct {
	InstrumentID uint32
	FundingTime  time.Time
	Rate         decimal.Decimal
	SourceTime   time.Time
}

func (f FundingEstimate) Validate() error {
	if f.InstrumentID == 0 || f.FundingTime.IsZero() || f.SourceTime.IsZero() {
		return fmt.Errorf("funding estimate requires instrument, funding_time and source_time")
	}
	if !f.FundingTime.Equal(time.UnixMilli(f.FundingTime.UnixMilli()).UTC()) {
		return fmt.Errorf("funding_time must have millisecond precision")
	}
	if !f.SourceTime.Equal(time.UnixMilli(f.SourceTime.UnixMilli()).UTC()) {
		return fmt.Errorf("source_time must have millisecond precision")
	}
	if f.Rate.Exponent() < -18 {
		return fmt.Errorf("funding estimate exceeds Decimal(38,18) scale")
	}
	return nil
}
