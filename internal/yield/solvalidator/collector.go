package solvalidator

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

const DefaultBaseURL = "https://validators-api.marinade.finance"

type Collector struct {
	BaseURL     string
	VoteAccount string
	HTTPClient  *http.Client
	Retry       exchange.HTTPRetryConfig
	Now         func() time.Time
}

func NewCollector(baseURL, voteAccount string) (*Collector, error) {
	if _, err := solana.DecodePubkey(voteAccount); err != nil {
		return nil, fmt.Errorf("validator vote account: %w", err)
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), VoteAccount: voteAccount, HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig()}, nil
}

type response struct {
	TotalCount json.Number `json:"total_count"`
	Validators []struct {
		VoteAccount string  `json:"vote_account"`
		InfoName    *string `json:"info_name"`
		EpochStats  []struct {
			Epoch               json.Number  `json:"epoch"`
			EpochEndAt          *string      `json:"epoch_end_at"`
			APR                 *json.Number `json:"apr"`
			APY                 *json.Number `json:"apy"`
			CommissionEffective *json.Number `json:"commission_effective"`
			MEVCommissionBPS    *json.Number `json:"mev_commission_bps"`
			ActivatedStake      string       `json:"activated_stake"`
		} `json:"epoch_stats"`
	} `json:"validators"`
}

type completedEpoch struct {
	epoch      uint64
	at         time.Time
	apy        decimal.Decimal
	commission decimal.Decimal
	stake      decimal.Decimal
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return marketyield.Batch{}, fmt.Errorf("Solana validator collector is not configured")
	}
	if _, err := solana.DecodePubkey(c.VoteAccount); err != nil {
		return marketyield.Batch{}, fmt.Errorf("validator vote account: %w", err)
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	query := url.Values{}
	query.Set("epochs", "10")
	query.Set("limit", "1")
	query.Set("query_vote_accounts", c.VoteAccount)
	rawURL := c.BaseURL + "/validators?" + query.Encode()
	body, err := exchange.Get(ctx, c.HTTPClient, rawURL, c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	var data response
	if err = marketyield.DecodeJSON(body, &data); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Marinade validators: %w", err)
	}
	total, err := unsignedNumber(data.TotalCount, "total_count")
	if err != nil || total != 1 || len(data.Validators) != 1 || data.Validators[0].VoteAccount != c.VoteAccount {
		return marketyield.Batch{}, fmt.Errorf("validator response does not uniquely match %s", c.VoteAccount)
	}
	validator := data.Validators[0]
	epochSeen := make(map[uint64]struct{}, len(validator.EpochStats))
	timeSeen := make(map[int64]struct{}, len(validator.EpochStats))
	completed := make([]completedEpoch, 0, len(validator.EpochStats))
	for index, stat := range validator.EpochStats {
		epoch, epochErr := unsignedNumber(stat.Epoch, "epoch")
		if epochErr != nil {
			return marketyield.Batch{}, fmt.Errorf("validator epoch_stats[%d]: %w", index, epochErr)
		}
		if _, exists := epochSeen[epoch]; exists {
			return marketyield.Batch{}, fmt.Errorf("validator has duplicate epoch %d", epoch)
		}
		epochSeen[epoch] = struct{}{}
		if stat.EpochEndAt == nil {
			if stat.APR != nil || stat.APY != nil || stat.CommissionEffective != nil {
				return marketyield.Batch{}, fmt.Errorf("unfinished epoch %d has completed reward fields", epoch)
			}
			continue
		}
		if stat.APR == nil || stat.APY == nil || stat.CommissionEffective == nil {
			return marketyield.Batch{}, fmt.Errorf("completed epoch %d is missing reward fields", epoch)
		}
		at, parseErr := time.Parse(time.RFC3339Nano, *stat.EpochEndAt)
		if parseErr != nil {
			return marketyield.Batch{}, fmt.Errorf("epoch %d end time: %w", epoch, parseErr)
		}
		at = time.UnixMilli(at.UTC().UnixMilli()).UTC()
		if at.After(collected) {
			return marketyield.Batch{}, fmt.Errorf("epoch %d ends in the future", epoch)
		}
		if _, exists := timeSeen[at.UnixMilli()]; exists {
			return marketyield.Batch{}, fmt.Errorf("validator has duplicate epoch end time")
		}
		timeSeen[at.UnixMilli()] = struct{}{}
		apr, parseErr := decimalNumber(*stat.APR, "apr")
		if parseErr != nil {
			return marketyield.Batch{}, parseErr
		}
		apy, parseErr := decimalNumber(*stat.APY, "apy")
		if parseErr != nil || apr.IsNegative() || apy.IsNegative() || !apr.LessThan(decimal.NewFromInt(1)) || !apy.LessThan(decimal.NewFromInt(1)) || apy.LessThan(apr) {
			return marketyield.Batch{}, fmt.Errorf("epoch %d APR/APY is invalid", epoch)
		}
		commissionInteger, parseErr := unsignedNumber(*stat.CommissionEffective, "commission_effective")
		if parseErr != nil || commissionInteger > 100 {
			return marketyield.Batch{}, fmt.Errorf("epoch %d commission is invalid", epoch)
		}
		if stat.MEVCommissionBPS != nil {
			mev, mevErr := unsignedNumber(*stat.MEVCommissionBPS, "mev_commission_bps")
			if mevErr != nil || mev > 10000 {
				return marketyield.Batch{}, fmt.Errorf("epoch %d MEV commission is invalid", epoch)
			}
		}
		stakeInt, ok := new(big.Int).SetString(stat.ActivatedStake, 10)
		if !ok || stakeInt.Sign() <= 0 {
			return marketyield.Batch{}, fmt.Errorf("epoch %d activated_stake is invalid", epoch)
		}
		stake := decimal.NewFromBigInt(stakeInt, 0).Div(decimal.NewFromInt(1_000_000_000))
		commission := decimal.NewFromBigInt(new(big.Int).SetUint64(commissionInteger), 0).Div(decimal.NewFromInt(100))
		completed = append(completed, completedEpoch{epoch: epoch, at: at, apy: apy, commission: commission, stake: stake})
	}
	if len(completed) == 0 || len(completed) > 10 {
		return marketyield.Batch{}, fmt.Errorf("validator completed epoch count %d is invalid", len(completed))
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].at.Before(completed[j].at) })
	if collected.Sub(completed[len(completed)-1].at) > 7*24*time.Hour {
		return marketyield.Batch{}, fmt.Errorf("validator latest completed epoch is stale")
	}
	hash := marketyield.HashPayloads(marketyield.Payload{Name: "validators", Body: body})
	name := shortVote(c.VoteAccount)
	if validator.InfoName != nil && strings.TrimSpace(*validator.InfoName) != "" {
		name = strings.TrimSpace(*validator.InfoName)
	}
	network, address, exposure := "solana-mainnet", c.VoteAccount, solana.SOLAsset
	route := marketyield.YieldRouteDefinition{ProviderType: "native", Provider: "Solana", ProductCode: "validator:" + c.VoteAccount, ProductName: "Solana validator " + name, YieldType: "native_staking",
		DepositAssetKey: solana.SOLAsset, PositionAssetKey: solana.SOLAsset, RedeemAssetKey: solana.SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
		IncomeSource: "combined", SourceURL: c.BaseURL + "/validators", CollectionEnabled: true}
	items := make([]marketyield.CollectedYield, 0, len(completed))
	for _, item := range completed {
		rate, fee, tvl, ratio := item.apy, item.commission, item.stake, decimal.NewFromInt(1)
		observation := marketyield.YieldObservation{ObservationTime: item.at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.SOLAsset}, RewardComponentRates: []*decimal.Decimal{&rate}, PerformanceFeeRate: &fee,
			RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &ratio, TVL: &tvl, Availability: "unknown", SourcePayloadHash: &hash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	return marketyield.Batch{Source: "solana-validator:" + c.VoteAccount, CollectedAt: collected, Items: items}, nil
}

func unsignedNumber(number json.Number, name string) (uint64, error) {
	value, ok := new(big.Int).SetString(number.String(), 10)
	if !ok || value.Sign() < 0 || !value.IsUint64() {
		return 0, fmt.Errorf("%s is not an unsigned integer", name)
	}
	return value.Uint64(), nil
}
func decimalNumber(number json.Number, name string) (decimal.Decimal, error) {
	value, err := decimal.NewFromString(number.String())
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}
func shortVote(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:6] + "…" + value[len(value)-6:]
}
