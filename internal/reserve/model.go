// Package reserve collects public, hash-pinned Reserve r5 observations.

// FixedString hashes/addresses below are binary bytes, not hexadecimal text.

package reserve

import (
	"math/big"
	"time"
)

type Capture struct {
	ChainId          uint64       `ch:"chain_id" json:"chain_id"`
	ManifestHash     string       `ch:"manifest_hash" json:"manifest_hash"`
	CaptureId        string       `ch:"capture_id" json:"capture_id"`
	BatchId          string       `ch:"batch_id" json:"batch_id"`
	CaptureKind      string       `ch:"capture_kind" json:"capture_kind"`
	CaptureMode      string       `ch:"capture_mode" json:"capture_mode"`
	FromBlock        uint64       `ch:"from_block" json:"from_block"`
	FromHash         string       `ch:"from_hash" json:"from_hash"`
	ToBlock          uint64       `ch:"to_block" json:"to_block"`
	ToHash           string       `ch:"to_hash" json:"to_hash"`
	FromTime         time.Time    `ch:"from_time" json:"from_time"`
	ToTime           time.Time    `ch:"to_time" json:"to_time"`
	ReceivedAt       time.Time    `ch:"received_at" json:"received_at"`
	AvailableAt      time.Time    `ch:"available_at" json:"available_at"`
	Canonical        bool         `ch:"canonical" json:"canonical"`
	Finality         string       `ch:"finality" json:"finality"`
	Revision         uint64       `ch:"revision" json:"revision"`
	Committed        bool         `ch:"committed" json:"committed"`
	LogCoverage      string       `ch:"log_coverage" json:"log_coverage"`
	StateCoverage    string       `ch:"state_coverage" json:"state_coverage"`
	QuoteCoverage    string       `ch:"quote_coverage" json:"quote_coverage"`
	ReceiptCoverage  string       `ch:"receipt_coverage" json:"receipt_coverage"`
	ExpectedStates   uint32       `ch:"expected_states" json:"expected_states"`
	ActualStates     uint32       `ch:"actual_states" json:"actual_states"`
	ExpectedQuotes   uint32       `ch:"expected_quotes" json:"expected_quotes"`
	ActualQuotes     uint32       `ch:"actual_quotes" json:"actual_quotes"`
	SkippedRoutes    uint32       `ch:"skipped_routes" json:"skipped_routes"`
	LogCount         uint32       `ch:"log_count" json:"log_count"`
	ExpectedReceipts uint32       `ch:"expected_receipts" json:"expected_receipts"`
	ActualReceipts   uint32       `ch:"actual_receipts" json:"actual_receipts"`
	StateMembers     string       `ch:"state_members" json:"state_members"`
	QuoteMembers     string       `ch:"quote_members" json:"quote_members"`
	LogMembers       string       `ch:"log_members" json:"log_members"`
	ReceiptMembers   string       `ch:"receipt_members" json:"receipt_members"`
	PlanHash         string       `ch:"plan_hash" json:"plan_hash"`
	PayloadHash      string       `ch:"payload_hash" json:"payload_hash"`
	Reason           string       `ch:"reason" json:"reason"`
	ReceiptRefs      []ReceiptRef `ch:"receipt_refs" json:"receipt_refs"`
}

type ReceiptRef struct {
	BlockHash    string `json:"block_hash"`
	TxHash       string `json:"tx_hash"`
	ReceiptHash  string `json:"receipt_hash"`
	CalldataHash string `json:"calldata_hash"`
}

type State struct {
	ChainId                  uint64    `ch:"chain_id" json:"chain_id"`
	ManifestHash             string    `ch:"manifest_hash" json:"manifest_hash"`
	CaptureId                string    `ch:"capture_id" json:"capture_id"`
	BatchId                  string    `ch:"batch_id" json:"batch_id"`
	BlockNumber              uint64    `ch:"block_number" json:"block_number"`
	BlockHash                string    `ch:"block_hash" json:"block_hash"`
	BlockTime                time.Time `ch:"block_time" json:"block_time"`
	AvailableAt              time.Time `ch:"available_at" json:"available_at"`
	PayloadHash              string    `ch:"payload_hash" json:"payload_hash"`
	Folio                    string    `ch:"folio" json:"folio"`
	Implementation           *string   `ch:"implementation" json:"implementation"`
	ImplementationCodeHash   *string   `ch:"implementation_code_hash" json:"implementation_code_hash"`
	ProtocolVersion          string    `ch:"protocol_version" json:"protocol_version"`
	IdentityOk               bool      `ch:"identity_ok" json:"identity_ok"`
	StateComplete            bool      `ch:"state_complete" json:"state_complete"`
	Reason                   string    `ch:"reason" json:"reason"`
	BaseFeeWei               *big.Int  `ch:"base_fee_wei" json:"base_fee_wei"`
	TotalSupplyRaw           *big.Int  `ch:"total_supply_raw" json:"total_supply_raw"`
	PendingFeeSharesRaw      *big.Int  `ch:"pending_fee_shares_raw" json:"pending_fee_shares_raw"`
	ShareDecimals            *uint8    `ch:"share_decimals" json:"share_decimals"`
	MintFeeD18               *big.Int  `ch:"mint_fee_d18" json:"mint_fee_d18"`
	TvlFeePerSecondD18       *big.Int  `ch:"tvl_fee_per_second_d18" json:"tvl_fee_per_second_d18"`
	DaoFeeRegistry           *string   `ch:"dao_fee_registry" json:"dao_fee_registry"`
	DaoFeeNumerator          *big.Int  `ch:"dao_fee_numerator" json:"dao_fee_numerator"`
	DaoFeeDenominator        *big.Int  `ch:"dao_fee_denominator" json:"dao_fee_denominator"`
	DaoFeeFloorD18           *big.Int  `ch:"dao_fee_floor_d18" json:"dao_fee_floor_d18"`
	MinimumMintFeeD18        *big.Int  `ch:"minimum_mint_fee_d18" json:"minimum_mint_fee_d18"`
	Deprecated               *bool     `ch:"deprecated" json:"deprecated"`
	SyncStateChangeActive    *bool     `ch:"sync_state_change_active" json:"sync_state_change_active"`
	AsyncStateChangeActive   *bool     `ch:"async_state_change_active" json:"async_state_change_active"`
	TrustedFillerRegistry    *string   `ch:"trusted_filler_registry" json:"trusted_filler_registry"`
	TrustedFillerEnabled     *bool     `ch:"trusted_filler_enabled" json:"trusted_filler_enabled"`
	GlobalBidsEnabled        *bool     `ch:"global_bids_enabled" json:"global_bids_enabled"`
	RebalanceBidsEnabled     *bool     `ch:"rebalance_bids_enabled" json:"rebalance_bids_enabled"`
	RebalanceNonce           *big.Int  `ch:"rebalance_nonce" json:"rebalance_nonce"`
	PriceControl             *uint8    `ch:"price_control" json:"price_control"`
	RebalanceStartedAt       *uint64   `ch:"rebalance_started_at" json:"rebalance_started_at"`
	RebalanceRestrictedUntil *uint64   `ch:"rebalance_restricted_until" json:"rebalance_restricted_until"`
	RebalanceAvailableUntil  *uint64   `ch:"rebalance_available_until" json:"rebalance_available_until"`
	RebalanceLimitLowD18     *big.Int  `ch:"rebalance_limit_low_d18" json:"rebalance_limit_low_d18"`
	RebalanceLimitSpotD18    *big.Int  `ch:"rebalance_limit_spot_d18" json:"rebalance_limit_spot_d18"`
	RebalanceLimitHighD18    *big.Int  `ch:"rebalance_limit_high_d18" json:"rebalance_limit_high_d18"`
	NextAuctionId            *big.Int  `ch:"next_auction_id" json:"next_auction_id"`
	AuctionId                *big.Int  `ch:"auction_id" json:"auction_id"`
	AuctionRebalanceNonce    *big.Int  `ch:"auction_rebalance_nonce" json:"auction_rebalance_nonce"`
	AuctionStartTime         *uint64   `ch:"auction_start_time" json:"auction_start_time"`
	AuctionEndTime           *uint64   `ch:"auction_end_time" json:"auction_end_time"`
	Basket                   []Asset   `ch:"basket" json:"basket"`
}

type Asset struct {
	Token               string   `ch:"token" json:"token"`
	Decimals            *uint8   `ch:"decimals" json:"decimals"`
	AmountRaw           *big.Int `ch:"amount_raw" json:"amount_raw"`
	InRebalance         *bool    `ch:"in_rebalance" json:"in_rebalance"`
	WeightLowD27        *big.Int `ch:"weight_low_d27" json:"weight_low_d27"`
	WeightSpotD27       *big.Int `ch:"weight_spot_d27" json:"weight_spot_d27"`
	WeightHighD27       *big.Int `ch:"weight_high_d27" json:"weight_high_d27"`
	InitialPriceLowD27  *big.Int `ch:"initial_price_low_d27" json:"initial_price_low_d27"`
	InitialPriceHighD27 *big.Int `ch:"initial_price_high_d27" json:"initial_price_high_d27"`
	MaxAuctionSizeRaw   *big.Int `ch:"max_auction_size_raw" json:"max_auction_size_raw"`
	AuctionPriceLowD27  *big.Int `ch:"auction_price_low_d27" json:"auction_price_low_d27"`
	AuctionPriceHighD27 *big.Int `ch:"auction_price_high_d27" json:"auction_price_high_d27"`
}

type Quote struct {
	ChainId             uint64    `ch:"chain_id" json:"chain_id"`
	ManifestHash        string    `ch:"manifest_hash" json:"manifest_hash"`
	CaptureId           string    `ch:"capture_id" json:"capture_id"`
	BatchId             string    `ch:"batch_id" json:"batch_id"`
	BlockNumber         uint64    `ch:"block_number" json:"block_number"`
	BlockHash           string    `ch:"block_hash" json:"block_hash"`
	BlockTime           time.Time `ch:"block_time" json:"block_time"`
	AvailableAt         time.Time `ch:"available_at" json:"available_at"`
	PayloadHash         string    `ch:"payload_hash" json:"payload_hash"`
	Folio               string    `ch:"folio" json:"folio"`
	RouteId             string    `ch:"route_id" json:"route_id"`
	QuoteId             string    `ch:"quote_id" json:"quote_id"`
	RouteKind           string    `ch:"route_kind" json:"route_kind"`
	BudgetToken         string    `ch:"budget_token" json:"budget_token"`
	RequestedBudgetRaw  *big.Int  `ch:"requested_budget_raw" json:"requested_budget_raw"`
	TokenIn             string    `ch:"token_in" json:"token_in"`
	TokenOut            string    `ch:"token_out" json:"token_out"`
	AmountInRaw         *big.Int  `ch:"amount_in_raw" json:"amount_in_raw"`
	AmountOutRaw        *big.Int  `ch:"amount_out_raw" json:"amount_out_raw"`
	AuctionId           *big.Int  `ch:"auction_id" json:"auction_id"`
	SellToken           *string   `ch:"sell_token" json:"sell_token"`
	BuyToken            *string   `ch:"buy_token" json:"buy_token"`
	RequestedMaxSellRaw *big.Int  `ch:"requested_max_sell_raw" json:"requested_max_sell_raw"`
	AuctionSellRaw      *big.Int  `ch:"auction_sell_raw" json:"auction_sell_raw"`
	AuctionBuyRaw       *big.Int  `ch:"auction_buy_raw" json:"auction_buy_raw"`
	AuctionPriceD27     *big.Int  `ch:"auction_price_d27" json:"auction_price_d27"`
	GrossSharesRaw      *big.Int  `ch:"gross_shares_raw" json:"gross_shares_raw"`
	FeeSharesRaw        *big.Int  `ch:"fee_shares_raw" json:"fee_shares_raw"`
	NetSharesRaw        *big.Int  `ch:"net_shares_raw" json:"net_shares_raw"`
	BasketAmounts       []Amount  `ch:"basket_amounts" json:"basket_amounts"`
	DexLegs             []Leg     `ch:"dex_legs" json:"dex_legs"`
	SharedPools         []string  `ch:"shared_pools" json:"shared_pools"`
	ExpectedLegs        uint16    `ch:"expected_legs" json:"expected_legs"`
	SuccessfulLegs      uint16    `ch:"successful_legs" json:"successful_legs"`
	WithinBudget        *bool     `ch:"within_budget" json:"within_budget"`
	Quality             string    `ch:"quality" json:"quality"`
	Status              string    `ch:"status" json:"status"`
	Reason              string    `ch:"reason" json:"reason"`
}

type Amount struct {
	Token     string   `ch:"token" json:"token"`
	AmountRaw *big.Int `ch:"amount_raw" json:"amount_raw"`
}

type Leg struct {
	LegIndex          uint16    `ch:"leg_index" json:"leg_index"`
	Side              string    `ch:"side" json:"side"`
	QuoteMode         string    `ch:"quote_mode" json:"quote_mode"`
	TokenIn           string    `ch:"token_in" json:"token_in"`
	TokenOut          string    `ch:"token_out" json:"token_out"`
	RequestedRaw      *big.Int  `ch:"requested_raw" json:"requested_raw"`
	AmountInRaw       *big.Int  `ch:"amount_in_raw" json:"amount_in_raw"`
	AmountOutRaw      *big.Int  `ch:"amount_out_raw" json:"amount_out_raw"`
	PathTokens        []string  `ch:"path_tokens" json:"path_tokens"`
	PathPools         []string  `ch:"path_pools" json:"path_pools"`
	PoolFees          []uint32  `ch:"pool_fees" json:"pool_fees"`
	QuoterGasEstimate *big.Int  `ch:"quoter_gas_estimate" json:"quoter_gas_estimate"`
	AvailableAt       time.Time `ch:"available_at" json:"available_at"`
	PayloadHash       string    `ch:"payload_hash" json:"payload_hash"`
	Status            string    `ch:"status" json:"status"`
}
