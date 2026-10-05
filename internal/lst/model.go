// Package lst models public LST redemption observations. FixedString values are raw bytes.
package lst

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"math/big"
	"time"
)

type Capture struct {
	CaptureId            uuid.UUID  `ch:"capture_id" json:"capture_id" lst:"UUID"`
	ManifestHash         string     `ch:"manifest_hash" json:"manifest_hash" lst:"FixedString(32)"`
	CaptureKind          string     `ch:"capture_kind" json:"capture_kind" lst:"LowCardinality(String)"`
	CaptureMode          string     `ch:"capture_mode" json:"capture_mode" lst:"LowCardinality(String)"`
	SourceId             string     `ch:"source_id" json:"source_id" lst:"LowCardinality(String)"`
	StartedAt            time.Time  `ch:"started_at" json:"started_at" lst:"DateTime64(6, 'UTC')"`
	ReceivedAt           *time.Time `ch:"received_at" json:"received_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	AvailableAt          time.Time  `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	WindowFromAt         *time.Time `ch:"window_from_at" json:"window_from_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	WindowToAt           *time.Time `ch:"window_to_at" json:"window_to_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ChainId              *uint64    `ch:"chain_id" json:"chain_id" lst:"Nullable(UInt64)"`
	FromBlock            *uint64    `ch:"from_block" json:"from_block" lst:"Nullable(UInt64)"`
	ToBlock              *uint64    `ch:"to_block" json:"to_block" lst:"Nullable(UInt64)"`
	FromBlockHash        *string    `ch:"from_block_hash" json:"from_block_hash" lst:"Nullable(FixedString(32))"`
	ToBlockHash          *string    `ch:"to_block_hash" json:"to_block_hash" lst:"Nullable(FixedString(32))"`
	FromBlockTime        *time.Time `ch:"from_block_time" json:"from_block_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ToBlockTime          *time.Time `ch:"to_block_time" json:"to_block_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	Finality             string     `ch:"finality" json:"finality" lst:"LowCardinality(String)"`
	Canonical            bool       `ch:"canonical" json:"canonical" lst:"Bool"`
	Revision             uint64     `ch:"revision" json:"revision" lst:"UInt64"`
	Status               string     `ch:"status" json:"status" lst:"LowCardinality(String)"`
	Reason               string     `ch:"reason" json:"reason" lst:"String"`
	ExpectedProtocolRows uint32     `ch:"expected_protocol_rows" json:"expected_protocol_rows" lst:"UInt32"`
	ExpectedQuoteRows    uint32     `ch:"expected_quote_rows" json:"expected_quote_rows" lst:"UInt32"`
	ProtocolRows         uint32     `ch:"protocol_rows" json:"protocol_rows" lst:"UInt32"`
	QuoteRows            uint32     `ch:"quote_rows" json:"quote_rows" lst:"UInt32"`
	RequestRows          uint32     `ch:"request_rows" json:"request_rows" lst:"UInt32"`
	FinalizationRows     uint32     `ch:"finalization_rows" json:"finalization_rows" lst:"UInt32"`
	ClaimRows            uint32     `ch:"claim_rows" json:"claim_rows" lst:"UInt32"`
	FundingRows          uint32     `ch:"funding_rows" json:"funding_rows" lst:"UInt32"`
	FactDigest           string     `ch:"fact_digest" json:"fact_digest" lst:"FixedString(32)"`
	EvidenceRootHash     string     `ch:"evidence_root_hash" json:"evidence_root_hash" lst:"FixedString(32)"`
	Committed            bool       `ch:"committed" json:"committed" lst:"Bool"`
}

type ProtocolState struct {
	CaptureId               uuid.UUID  `ch:"capture_id" json:"capture_id" lst:"UUID"`
	ObservedAt              time.Time  `ch:"observed_at" json:"observed_at" lst:"DateTime64(6, 'UTC')"`
	ChainId                 uint64     `ch:"chain_id" json:"chain_id" lst:"UInt64"`
	BlockNumber             *uint64    `ch:"block_number" json:"block_number" lst:"Nullable(UInt64)"`
	BlockHash               *string    `ch:"block_hash" json:"block_hash" lst:"Nullable(FixedString(32))"`
	BlockTime               *time.Time `ch:"block_time" json:"block_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	QueueAddress            string     `ch:"queue_address" json:"queue_address" lst:"FixedString(20)"`
	IdentityOk              bool       `ch:"identity_ok" json:"identity_ok" lst:"Bool"`
	StateStatus             string     `ch:"state_status" json:"state_status" lst:"LowCardinality(String)"`
	Reason                  string     `ch:"reason" json:"reason" lst:"String"`
	StethTotalPooledEthWei  *big.Int   `ch:"steth_total_pooled_eth_wei" json:"steth_total_pooled_eth_wei" lst:"Nullable(UInt256)"`
	StethTotalSharesRaw     *big.Int   `ch:"steth_total_shares_raw" json:"steth_total_shares_raw" lst:"Nullable(UInt256)"`
	OneWstethToStethWei     *big.Int   `ch:"one_wsteth_to_steth_wei" json:"one_wsteth_to_steth_wei" lst:"Nullable(UInt256)"`
	QueuePaused             *bool      `ch:"queue_paused" json:"queue_paused" lst:"Nullable(Bool)"`
	BunkerActive            *bool      `ch:"bunker_active" json:"bunker_active" lst:"Nullable(Bool)"`
	MinRequestStethWei      *big.Int   `ch:"min_request_steth_wei" json:"min_request_steth_wei" lst:"Nullable(UInt256)"`
	MaxRequestStethWei      *big.Int   `ch:"max_request_steth_wei" json:"max_request_steth_wei" lst:"Nullable(UInt256)"`
	LastRequestId           *big.Int   `ch:"last_request_id" json:"last_request_id" lst:"Nullable(UInt256)"`
	LastFinalizedRequestId  *big.Int   `ch:"last_finalized_request_id" json:"last_finalized_request_id" lst:"Nullable(UInt256)"`
	UnfinalizedStethWei     *big.Int   `ch:"unfinalized_steth_wei" json:"unfinalized_steth_wei" lst:"Nullable(UInt256)"`
	LockedEthWei            *big.Int   `ch:"locked_eth_wei" json:"locked_eth_wei" lst:"Nullable(UInt256)"`
	BaseFeePerGasWei        *big.Int   `ch:"base_fee_per_gas_wei" json:"base_fee_per_gas_wei" lst:"Nullable(UInt256)"`
	PriorityFeeReferenceWei *big.Int   `ch:"priority_fee_reference_wei" json:"priority_fee_reference_wei" lst:"Nullable(UInt256)"`
	RequestedAt             *time.Time `ch:"requested_at" json:"requested_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ReceivedAt              *time.Time `ch:"received_at" json:"received_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	SourceAvailableAt       *time.Time `ch:"source_available_at" json:"source_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	AvailableAt             time.Time  `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	PayloadHashes           []string   `ch:"payload_hashes" json:"payload_hashes" lst:"Array(FixedString(32))"`
	RowHash                 string     `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type Quote struct {
	CaptureId                 uuid.UUID        `ch:"capture_id" json:"capture_id" lst:"UUID"`
	QuoteId                   string           `ch:"quote_id" json:"quote_id" lst:"FixedString(32)"`
	ObservedAt                time.Time        `ch:"observed_at" json:"observed_at" lst:"DateTime64(6, 'UTC')"`
	QuoteRole                 string           `ch:"quote_role" json:"quote_role" lst:"LowCardinality(String)"`
	ReferenceQuoteId          *string          `ch:"reference_quote_id" json:"reference_quote_id" lst:"Nullable(FixedString(32))"`
	IsFollowupSeed            bool             `ch:"is_followup_seed" json:"is_followup_seed" lst:"Bool"`
	TargetDelaySeconds        *uint32          `ch:"target_delay_seconds" json:"target_delay_seconds" lst:"Nullable(UInt32)"`
	PlannedForAt              *time.Time       `ch:"planned_for_at" json:"planned_for_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	RouteId                   string           `ch:"route_id" json:"route_id" lst:"LowCardinality(String)"`
	QuoteAssetAddress         string           `ch:"quote_asset_address" json:"quote_asset_address" lst:"FixedString(20)"`
	LstAddress                string           `ch:"lst_address" json:"lst_address" lst:"FixedString(20)"`
	PurchaseBudgetUsdtRaw     *big.Int         `ch:"purchase_budget_usdt_raw" json:"purchase_budget_usdt_raw" lst:"Nullable(UInt256)"`
	BuyWethOutWei             *big.Int         `ch:"buy_weth_out_wei" json:"buy_weth_out_wei" lst:"Nullable(UInt256)"`
	BuyLstOutRaw              *big.Int         `ch:"buy_lst_out_raw" json:"buy_lst_out_raw" lst:"Nullable(UInt256)"`
	RequestStethWei           *big.Int         `ch:"request_steth_wei" json:"request_steth_wei" lst:"Nullable(UInt256)"`
	RequestSharesRaw          *big.Int         `ch:"request_shares_raw" json:"request_shares_raw" lst:"Nullable(UInt256)"`
	RequestParts              *uint32          `ch:"request_parts" json:"request_parts" lst:"Nullable(UInt32)"`
	NominalRedeemEthWei       *big.Int         `ch:"nominal_redeem_eth_wei" json:"nominal_redeem_eth_wei" lst:"Nullable(UInt256)"`
	EthExitInputWei           *big.Int         `ch:"eth_exit_input_wei" json:"eth_exit_input_wei" lst:"Nullable(UInt256)"`
	EthExitUsdtOutRaw         *big.Int         `ch:"eth_exit_usdt_out_raw" json:"eth_exit_usdt_out_raw" lst:"Nullable(UInt256)"`
	QuoterInternalGas         []uint64         `ch:"quoter_internal_gas" json:"quoter_internal_gas" lst:"Array(UInt64)"`
	BuyStatus                 string           `ch:"buy_status" json:"buy_status" lst:"LowCardinality(String)"`
	BuyReason                 string           `ch:"buy_reason" json:"buy_reason" lst:"String"`
	ConversionStatus          string           `ch:"conversion_status" json:"conversion_status" lst:"LowCardinality(String)"`
	ConversionReason          string           `ch:"conversion_reason" json:"conversion_reason" lst:"String"`
	ExitStatus                string           `ch:"exit_status" json:"exit_status" lst:"LowCardinality(String)"`
	ExitReason                string           `ch:"exit_reason" json:"exit_reason" lst:"String"`
	ChainRequestedAt          *time.Time       `ch:"chain_requested_at" json:"chain_requested_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ChainReceivedAt           *time.Time       `ch:"chain_received_at" json:"chain_received_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ChainAvailableAt          *time.Time       `ch:"chain_available_at" json:"chain_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	ChainPayloadHashes        []string         `ch:"chain_payload_hashes" json:"chain_payload_hashes" lst:"Array(FixedString(32))"`
	HedgeInstrumentId         uint32           `ch:"hedge_instrument_id" json:"hedge_instrument_id" lst:"UInt32"`
	HedgeQuantityLot          *int64           `ch:"hedge_quantity_lot" json:"hedge_quantity_lot" lst:"Nullable(Int64)"`
	HedgeEthWei               *big.Int         `ch:"hedge_eth_wei" json:"hedge_eth_wei" lst:"Nullable(UInt256)"`
	UnhedgedEthResidualWei    *big.Int         `ch:"unhedged_eth_residual_wei" json:"unhedged_eth_residual_wei" lst:"Nullable(UInt256)"`
	HedgeSellNotionalUsdtE8   *big.Int         `ch:"hedge_sell_notional_usdt_e8" json:"hedge_sell_notional_usdt_e8" lst:"Nullable(UInt256)"`
	HedgeBuyNotionalUsdtE8    *big.Int         `ch:"hedge_buy_notional_usdt_e8" json:"hedge_buy_notional_usdt_e8" lst:"Nullable(UInt256)"`
	HedgeDepthLastUpdateId    *uint64          `ch:"hedge_depth_last_update_id" json:"hedge_depth_last_update_id" lst:"Nullable(UInt64)"`
	HedgeDepthEventTime       *time.Time       `ch:"hedge_depth_event_time" json:"hedge_depth_event_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	HedgeDepthTransactionTime *time.Time       `ch:"hedge_depth_transaction_time" json:"hedge_depth_transaction_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	HedgeDepthRequestedAt     *time.Time       `ch:"hedge_depth_requested_at" json:"hedge_depth_requested_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	HedgeDepthReceivedAt      *time.Time       `ch:"hedge_depth_received_at" json:"hedge_depth_received_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	HedgeDepthAvailableAt     *time.Time       `ch:"hedge_depth_available_at" json:"hedge_depth_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	HedgeDepthPayloadHash     *string          `ch:"hedge_depth_payload_hash" json:"hedge_depth_payload_hash" lst:"Nullable(FixedString(32))"`
	HedgeStatus               string           `ch:"hedge_status" json:"hedge_status" lst:"LowCardinality(String)"`
	HedgeReason               string           `ch:"hedge_reason" json:"hedge_reason" lst:"String"`
	MarkPriceTickE8           *int64           `ch:"mark_price_tick_e8" json:"mark_price_tick_e8" lst:"Nullable(Int64)"`
	IndexPriceTickE8          *int64           `ch:"index_price_tick_e8" json:"index_price_tick_e8" lst:"Nullable(Int64)"`
	IndicatedFundingRate      *decimal.Decimal `ch:"indicated_funding_rate" json:"indicated_funding_rate" lst:"Nullable(Decimal(38, 18))"`
	NextFundingTime           *time.Time       `ch:"next_funding_time" json:"next_funding_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	MarkSourceTime            *time.Time       `ch:"mark_source_time" json:"mark_source_time" lst:"Nullable(DateTime64(6, 'UTC'))"`
	MarkRequestedAt           *time.Time       `ch:"mark_requested_at" json:"mark_requested_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	MarkReceivedAt            *time.Time       `ch:"mark_received_at" json:"mark_received_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	MarkAvailableAt           *time.Time       `ch:"mark_available_at" json:"mark_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	MarkPayloadHash           *string          `ch:"mark_payload_hash" json:"mark_payload_hash" lst:"Nullable(FixedString(32))"`
	TimingStatus              string           `ch:"timing_status" json:"timing_status" lst:"LowCardinality(String)"`
	Reason                    string           `ch:"reason" json:"reason" lst:"String"`
	AvailableAt               time.Time        `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	RowHash                   string           `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type WithdrawalRequest struct {
	CaptureId                 uuid.UUID  `ch:"capture_id" json:"capture_id" lst:"UUID"`
	ChainId                   uint64     `ch:"chain_id" json:"chain_id" lst:"UInt64"`
	QueueAddress              string     `ch:"queue_address" json:"queue_address" lst:"FixedString(20)"`
	BlockNumber               uint64     `ch:"block_number" json:"block_number" lst:"UInt64"`
	BlockHash                 string     `ch:"block_hash" json:"block_hash" lst:"FixedString(32)"`
	BlockTime                 time.Time  `ch:"block_time" json:"block_time" lst:"DateTime64(6, 'UTC')"`
	TransactionHash           string     `ch:"transaction_hash" json:"transaction_hash" lst:"FixedString(32)"`
	TransactionIndex          uint32     `ch:"transaction_index" json:"transaction_index" lst:"UInt32"`
	LogIndex                  uint32     `ch:"log_index" json:"log_index" lst:"UInt32"`
	AbiVersion                string     `ch:"abi_version" json:"abi_version" lst:"LowCardinality(String)"`
	RequestId                 *big.Int   `ch:"request_id" json:"request_id" lst:"UInt256"`
	Sender                    string     `ch:"sender" json:"sender" lst:"FixedString(20)"`
	InitialOwner              string     `ch:"initial_owner" json:"initial_owner" lst:"FixedString(20)"`
	AmountStethWei            *big.Int   `ch:"amount_steth_wei" json:"amount_steth_wei" lst:"UInt256"`
	AmountSharesRaw           *big.Int   `ch:"amount_shares_raw" json:"amount_shares_raw" lst:"UInt256"`
	EventPayloadHash          string     `ch:"event_payload_hash" json:"event_payload_hash" lst:"FixedString(32)"`
	AvailableAt               time.Time  `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	ReceiptStatus             *uint8     `ch:"receipt_status" json:"receipt_status" lst:"Nullable(UInt8)"`
	TransactionSender         *string    `ch:"transaction_sender" json:"transaction_sender" lst:"Nullable(FixedString(20))"`
	TransactionTo             *string    `ch:"transaction_to" json:"transaction_to" lst:"Nullable(FixedString(20))"`
	InputSelector             *string    `ch:"input_selector" json:"input_selector" lst:"Nullable(FixedString(4))"`
	TransactionOperationCount *uint32    `ch:"transaction_operation_count" json:"transaction_operation_count" lst:"Nullable(UInt32)"`
	GasSampleClass            string     `ch:"gas_sample_class" json:"gas_sample_class" lst:"LowCardinality(String)"`
	GasUsed                   *uint64    `ch:"gas_used" json:"gas_used" lst:"Nullable(UInt64)"`
	EffectiveGasPriceWei      *big.Int   `ch:"effective_gas_price_wei" json:"effective_gas_price_wei" lst:"Nullable(UInt256)"`
	ReceiptPayloadHash        *string    `ch:"receipt_payload_hash" json:"receipt_payload_hash" lst:"Nullable(FixedString(32))"`
	ReceiptAvailableAt        *time.Time `ch:"receipt_available_at" json:"receipt_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	RowHash                   string     `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type WithdrawalFinalization struct {
	CaptureId        uuid.UUID `ch:"capture_id" json:"capture_id" lst:"UUID"`
	ChainId          uint64    `ch:"chain_id" json:"chain_id" lst:"UInt64"`
	QueueAddress     string    `ch:"queue_address" json:"queue_address" lst:"FixedString(20)"`
	BlockNumber      uint64    `ch:"block_number" json:"block_number" lst:"UInt64"`
	BlockHash        string    `ch:"block_hash" json:"block_hash" lst:"FixedString(32)"`
	BlockTime        time.Time `ch:"block_time" json:"block_time" lst:"DateTime64(6, 'UTC')"`
	TransactionHash  string    `ch:"transaction_hash" json:"transaction_hash" lst:"FixedString(32)"`
	TransactionIndex uint32    `ch:"transaction_index" json:"transaction_index" lst:"UInt32"`
	LogIndex         uint32    `ch:"log_index" json:"log_index" lst:"UInt32"`
	AbiVersion       string    `ch:"abi_version" json:"abi_version" lst:"LowCardinality(String)"`
	FromRequestId    *big.Int  `ch:"from_request_id" json:"from_request_id" lst:"UInt256"`
	ToRequestId      *big.Int  `ch:"to_request_id" json:"to_request_id" lst:"UInt256"`
	EthLockedWei     *big.Int  `ch:"eth_locked_wei" json:"eth_locked_wei" lst:"UInt256"`
	SharesToBurnRaw  *big.Int  `ch:"shares_to_burn_raw" json:"shares_to_burn_raw" lst:"UInt256"`
	EventTimestamp   time.Time `ch:"event_timestamp" json:"event_timestamp" lst:"DateTime64(6, 'UTC')"`
	EventPayloadHash string    `ch:"event_payload_hash" json:"event_payload_hash" lst:"FixedString(32)"`
	AvailableAt      time.Time `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	RowHash          string    `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type WithdrawalClaim struct {
	CaptureId                 uuid.UUID  `ch:"capture_id" json:"capture_id" lst:"UUID"`
	ChainId                   uint64     `ch:"chain_id" json:"chain_id" lst:"UInt64"`
	QueueAddress              string     `ch:"queue_address" json:"queue_address" lst:"FixedString(20)"`
	BlockNumber               uint64     `ch:"block_number" json:"block_number" lst:"UInt64"`
	BlockHash                 string     `ch:"block_hash" json:"block_hash" lst:"FixedString(32)"`
	BlockTime                 time.Time  `ch:"block_time" json:"block_time" lst:"DateTime64(6, 'UTC')"`
	TransactionHash           string     `ch:"transaction_hash" json:"transaction_hash" lst:"FixedString(32)"`
	TransactionIndex          uint32     `ch:"transaction_index" json:"transaction_index" lst:"UInt32"`
	LogIndex                  uint32     `ch:"log_index" json:"log_index" lst:"UInt32"`
	AbiVersion                string     `ch:"abi_version" json:"abi_version" lst:"LowCardinality(String)"`
	RequestId                 *big.Int   `ch:"request_id" json:"request_id" lst:"UInt256"`
	Owner                     string     `ch:"owner" json:"owner" lst:"FixedString(20)"`
	Receiver                  string     `ch:"receiver" json:"receiver" lst:"FixedString(20)"`
	AmountEthWei              *big.Int   `ch:"amount_eth_wei" json:"amount_eth_wei" lst:"UInt256"`
	EventPayloadHash          string     `ch:"event_payload_hash" json:"event_payload_hash" lst:"FixedString(32)"`
	AvailableAt               time.Time  `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	ReceiptStatus             *uint8     `ch:"receipt_status" json:"receipt_status" lst:"Nullable(UInt8)"`
	TransactionSender         *string    `ch:"transaction_sender" json:"transaction_sender" lst:"Nullable(FixedString(20))"`
	TransactionTo             *string    `ch:"transaction_to" json:"transaction_to" lst:"Nullable(FixedString(20))"`
	InputSelector             *string    `ch:"input_selector" json:"input_selector" lst:"Nullable(FixedString(4))"`
	TransactionOperationCount *uint32    `ch:"transaction_operation_count" json:"transaction_operation_count" lst:"Nullable(UInt32)"`
	GasSampleClass            string     `ch:"gas_sample_class" json:"gas_sample_class" lst:"LowCardinality(String)"`
	GasUsed                   *uint64    `ch:"gas_used" json:"gas_used" lst:"Nullable(UInt64)"`
	EffectiveGasPriceWei      *big.Int   `ch:"effective_gas_price_wei" json:"effective_gas_price_wei" lst:"Nullable(UInt256)"`
	ReceiptPayloadHash        *string    `ch:"receipt_payload_hash" json:"receipt_payload_hash" lst:"Nullable(FixedString(32))"`
	ReceiptAvailableAt        *time.Time `ch:"receipt_available_at" json:"receipt_available_at" lst:"Nullable(DateTime64(6, 'UTC'))"`
	RowHash                   string     `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type FundingSettlement struct {
	CaptureId                 uuid.UUID       `ch:"capture_id" json:"capture_id" lst:"UUID"`
	InstrumentId              uint32          `ch:"instrument_id" json:"instrument_id" lst:"UInt32"`
	FundingTime               time.Time       `ch:"funding_time" json:"funding_time" lst:"DateTime64(6, 'UTC')"`
	FundingRate               decimal.Decimal `ch:"funding_rate" json:"funding_rate" lst:"Decimal(38, 18)"`
	SettlementMarkPriceTickE8 *int64          `ch:"settlement_mark_price_tick_e8" json:"settlement_mark_price_tick_e8" lst:"Nullable(Int64)"`
	SourceId                  string          `ch:"source_id" json:"source_id" lst:"LowCardinality(String)"`
	RequestedAt               time.Time       `ch:"requested_at" json:"requested_at" lst:"DateTime64(6, 'UTC')"`
	ReceivedAt                time.Time       `ch:"received_at" json:"received_at" lst:"DateTime64(6, 'UTC')"`
	AvailableAt               time.Time       `ch:"available_at" json:"available_at" lst:"DateTime64(6, 'UTC')"`
	SourcePayloadHash         string          `ch:"source_payload_hash" json:"source_payload_hash" lst:"FixedString(32)"`
	RowHash                   string          `ch:"row_hash" json:"row_hash" lst:"FixedString(32)"`
}

type Batch struct {
	Capture       Capture
	Protocols     []ProtocolState
	Quotes        []Quote
	Requests      []WithdrawalRequest
	Finalizations []WithdrawalFinalization
	Claims        []WithdrawalClaim
	Funding       []FundingSettlement
	// Local cleanup metadata, frozen in pending-batch.gob; never a database row.
	RawEvidenceHashes []string
}

type Store interface {
	WriteLST(context.Context, Batch) error
	WriteLSTRevision(context.Context, Capture) error
	LSTCaptures(context.Context, string) ([]Capture, error)
	LSTBatch(context.Context, Capture) (Batch, error)
}
