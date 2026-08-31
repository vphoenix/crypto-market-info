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
	QiAVAX     = "0x5c0401e81bc07ca70fad469b451682c0d747ef1c"
	Controller = "0x486af39519b4dc9a7fccd318217352830e8ad9b4"
)

type LendingCollector struct{ Client *avalanche.Client }

func NewLendingCollector(client *avalanche.Client) *LendingCollector {
	return &LendingCollector{Client: client}
}

func (c *LendingCollector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Client == nil {
		return marketyield.Batch{}, fmt.Errorf("BENQI lending collector is not configured")
	}
	argument := "000000000000000000000000" + QiAVAX[2:]
	snapshot, err := c.Client.Read(ctx, []avalanche.Read{
		{Address: QiAVAX, Data: "0x313ce567"}, {Address: QiAVAX, Data: "0x840bbeac"},
		{Address: QiAVAX, Data: "0x5fe3b567"}, {Address: QiAVAX, Data: "0xd3bd2c72"},
		{Address: QiAVAX, Data: "0xbd6d894d"}, {Address: QiAVAX, Data: "0x18160ddd"},
		{Address: QiAVAX, Data: "0x3b1d21a2"}, {Address: Controller, Data: "0x8e8f294b" + argument},
		{Address: Controller, Data: "0x731f0c2b" + argument},
	})
	if err != nil {
		return marketyield.Batch{}, err
	}
	decimals, decimalsErr := avalanche.Uint64(snapshot.Results[0])
	isQi, qiErr := avalanche.Bool(snapshot.Results[1])
	controller, controllerErr := avalanche.Address(snapshot.Results[2])
	listed, listedErr := avalanche.Markets(snapshot.Results[7])
	paused, pausedErr := avalanche.Bool(snapshot.Results[8])
	if decimalsErr != nil || qiErr != nil || controllerErr != nil || listedErr != nil || pausedErr != nil || decimals != 8 || !isQi || controller != Controller {
		return marketyield.Batch{}, fmt.Errorf("BENQI lending invalid identity, market or pause flag")
	}
	values := make([]*big.Int, 4)
	for i := range values {
		values[i], err = avalanche.Uint256(snapshot.Results[i+3])
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("BENQI lending invalid ABI integer at field %d", i+3)
		}
	}
	// Multiply raw integers before the one final Decimal truncation. In
	// particular the qiAVAX exchange rate has 28 places, not 18.
	rate, rateErr := avalanche.Scaled(new(big.Int).Mul(values[0], big.NewInt(31536000)), -18)
	exposure, exposureErr := avalanche.PositiveScaled(values[1], -28)
	tvl, tvlErr := avalanche.Scaled(new(big.Int).Mul(values[1], values[2]), -36)
	cash, cashErr := avalanche.Scaled(values[3], -18)
	if rateErr != nil || exposureErr != nil || tvlErr != nil || cashErr != nil {
		return marketyield.Batch{}, fmt.Errorf("BENQI lending amounts or exchange ratio outside Decimal(38,18)")
	}
	observation := baseObservation(snapshot)
	observation.Rate, observation.RateKind, observation.RateOrigin = &rate, "apr", "derived"
	observation.RewardComponentRates = []*decimal.Decimal{&rate}
	observation.ExposureRatio, observation.TVL, observation.PoolCash = &exposure, &tvl, &cash
	observation.UnbondingSeconds = marketyield.Ptr(uint64(0))
	observation.Availability = "available"
	if !listed {
		observation.Availability = "closed"
	} else if paused {
		observation.Availability = "paused"
	}
	definition := route("avalanche-qiavax-supply", "BENQI AVAX 基础借贷收益", "lending", QiAVAX, "borrow_interest", "https://docs.benqi.fi/resources/contracts/core-markets")
	return completeBatch("benqi-avax-lending", definition, observation)
}
