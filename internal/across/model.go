// Package across collects public Across stablecoin relay evidence.
// FixedString values are binary strings. Never JSON-encode these rows directly.
package across

import (
	"github.com/shopspring/decimal"
	"math/big"
	"time"
)

type Capture struct {
	ManifestHash      string    `ch:"manifest_hash" dbtype:"FixedString(32)"`
	CaptureId         string    `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId           uint64    `ch:"chain_id" dbtype:"UInt64"`
	CaptureKind       string    `ch:"capture_kind" dbtype:"LowCardinality(String)"`
	CaptureMode       string    `ch:"capture_mode" dbtype:"LowCardinality(String)"`
	FromBlock         *uint64   `ch:"from_block" dbtype:"Nullable(UInt64)"`
	ToBlock           *uint64   `ch:"to_block" dbtype:"Nullable(UInt64)"`
	FromHash          *string   `ch:"from_hash" dbtype:"Nullable(FixedString(32))"`
	ToHash            *string   `ch:"to_hash" dbtype:"Nullable(FixedString(32))"`
	StartedAt         time.Time `ch:"started_at" dbtype:"DateTime64(6, 'UTC')"`
	AvailableAt       time.Time `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	SourceId          string    `ch:"source_id" dbtype:"LowCardinality(String)"`
	Status            string    `ch:"status" dbtype:"LowCardinality(String)"`
	Reason            string    `ch:"reason" dbtype:"String"`
	ExpectedTasks     uint32    `ch:"expected_tasks" dbtype:"UInt32"`
	CompletedTasks    uint32    `ch:"completed_tasks" dbtype:"UInt32"`
	UnknownEventCount uint32    `ch:"unknown_event_count" dbtype:"UInt32"`
	TableIds          []uint8   `ch:"table_ids" dbtype:"Array(UInt8)"`
	RowCounts         []uint32  `ch:"row_counts" dbtype:"Array(UInt32)"`
	RowDigests        []string  `ch:"row_digests" dbtype:"Array(FixedString(32))"`
	EvidenceHash      string    `ch:"evidence_hash" dbtype:"FixedString(32)"`
	Canonical         bool      `ch:"canonical" dbtype:"Bool"`
	Finality          string    `ch:"finality" dbtype:"LowCardinality(String)"`
	Revision          uint64    `ch:"revision" dbtype:"UInt64"`
	Committed         bool      `ch:"committed" dbtype:"Bool"`
}
type Deposit struct {
	CaptureId           string     `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId             uint64     `ch:"chain_id" dbtype:"UInt64"`
	SpokePool           string     `ch:"spoke_pool" dbtype:"FixedString(20)"`
	BlockNumber         uint64     `ch:"block_number" dbtype:"UInt64"`
	BlockHash           string     `ch:"block_hash" dbtype:"FixedString(32)"`
	BlockTime           time.Time  `ch:"block_time" dbtype:"DateTime64(6, 'UTC')"`
	TxHash              string     `ch:"tx_hash" dbtype:"FixedString(32)"`
	TxIndex             uint32     `ch:"tx_index" dbtype:"UInt32"`
	LogIndex            uint32     `ch:"log_index" dbtype:"UInt32"`
	AbiRevision         string     `ch:"abi_revision" dbtype:"LowCardinality(String)"`
	DestinationChainId  uint64     `ch:"destination_chain_id" dbtype:"UInt64"`
	DepositId           *big.Int   `ch:"deposit_id" dbtype:"UInt256"`
	Depositor           string     `ch:"depositor" dbtype:"FixedString(32)"`
	Recipient           string     `ch:"recipient" dbtype:"FixedString(32)"`
	ExclusiveRelayer    string     `ch:"exclusive_relayer" dbtype:"FixedString(32)"`
	InputToken          string     `ch:"input_token" dbtype:"FixedString(32)"`
	OutputToken         string     `ch:"output_token" dbtype:"FixedString(32)"`
	InputAmountRaw      *big.Int   `ch:"input_amount_raw" dbtype:"UInt256"`
	OutputAmountRaw     *big.Int   `ch:"output_amount_raw" dbtype:"UInt256"`
	QuoteTimestamp      uint32     `ch:"quote_timestamp" dbtype:"UInt32"`
	FillDeadline        uint32     `ch:"fill_deadline" dbtype:"UInt32"`
	ExclusivityDeadline uint32     `ch:"exclusivity_deadline" dbtype:"UInt32"`
	Message             string     `ch:"message" dbtype:"String"`
	MessageHash         string     `ch:"message_hash" dbtype:"FixedString(32)"`
	RelayHash           string     `ch:"relay_hash" dbtype:"FixedString(32)"`
	LiveReceivedAt      *time.Time `ch:"live_received_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	AvailableAt         time.Time  `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	PayloadHash         string     `ch:"payload_hash" dbtype:"FixedString(32)"`
}
type DepositUpdate struct {
	CaptureId              string     `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId                uint64     `ch:"chain_id" dbtype:"UInt64"`
	SpokePool              string     `ch:"spoke_pool" dbtype:"FixedString(20)"`
	BlockNumber            uint64     `ch:"block_number" dbtype:"UInt64"`
	BlockHash              string     `ch:"block_hash" dbtype:"FixedString(32)"`
	BlockTime              time.Time  `ch:"block_time" dbtype:"DateTime64(6, 'UTC')"`
	TxHash                 string     `ch:"tx_hash" dbtype:"FixedString(32)"`
	TxIndex                uint32     `ch:"tx_index" dbtype:"UInt32"`
	LogIndex               uint32     `ch:"log_index" dbtype:"UInt32"`
	AbiRevision            string     `ch:"abi_revision" dbtype:"LowCardinality(String)"`
	DepositId              *big.Int   `ch:"deposit_id" dbtype:"UInt256"`
	Depositor              string     `ch:"depositor" dbtype:"FixedString(32)"`
	UpdatedOutputAmountRaw *big.Int   `ch:"updated_output_amount_raw" dbtype:"UInt256"`
	UpdatedRecipient       string     `ch:"updated_recipient" dbtype:"FixedString(32)"`
	UpdatedMessage         string     `ch:"updated_message" dbtype:"String"`
	UpdatedMessageHash     string     `ch:"updated_message_hash" dbtype:"FixedString(32)"`
	DepositorSignature     string     `ch:"depositor_signature" dbtype:"String"`
	LiveReceivedAt         *time.Time `ch:"live_received_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	AvailableAt            time.Time  `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	PayloadHash            string     `ch:"payload_hash" dbtype:"FixedString(32)"`
}
type Fill struct {
	CaptureId              string     `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId                uint64     `ch:"chain_id" dbtype:"UInt64"`
	SpokePool              string     `ch:"spoke_pool" dbtype:"FixedString(20)"`
	BlockNumber            uint64     `ch:"block_number" dbtype:"UInt64"`
	BlockHash              string     `ch:"block_hash" dbtype:"FixedString(32)"`
	BlockTime              time.Time  `ch:"block_time" dbtype:"DateTime64(6, 'UTC')"`
	TxHash                 string     `ch:"tx_hash" dbtype:"FixedString(32)"`
	TxIndex                uint32     `ch:"tx_index" dbtype:"UInt32"`
	LogIndex               uint32     `ch:"log_index" dbtype:"UInt32"`
	AbiRevision            string     `ch:"abi_revision" dbtype:"LowCardinality(String)"`
	OriginChainId          uint64     `ch:"origin_chain_id" dbtype:"UInt64"`
	DepositId              *big.Int   `ch:"deposit_id" dbtype:"UInt256"`
	InputToken             string     `ch:"input_token" dbtype:"FixedString(32)"`
	OutputToken            string     `ch:"output_token" dbtype:"FixedString(32)"`
	InputAmountRaw         *big.Int   `ch:"input_amount_raw" dbtype:"UInt256"`
	OutputAmountRaw        *big.Int   `ch:"output_amount_raw" dbtype:"UInt256"`
	Depositor              string     `ch:"depositor" dbtype:"FixedString(32)"`
	Recipient              string     `ch:"recipient" dbtype:"FixedString(32)"`
	ExclusiveRelayer       string     `ch:"exclusive_relayer" dbtype:"FixedString(32)"`
	FillDeadline           uint32     `ch:"fill_deadline" dbtype:"UInt32"`
	ExclusivityDeadline    uint32     `ch:"exclusivity_deadline" dbtype:"UInt32"`
	MessageHash            string     `ch:"message_hash" dbtype:"FixedString(32)"`
	RepaymentChainId       uint64     `ch:"repayment_chain_id" dbtype:"UInt64"`
	RepaymentAddress       string     `ch:"repayment_address" dbtype:"FixedString(32)"`
	UpdatedRecipient       string     `ch:"updated_recipient" dbtype:"FixedString(32)"`
	UpdatedMessageHash     string     `ch:"updated_message_hash" dbtype:"FixedString(32)"`
	UpdatedOutputAmountRaw *big.Int   `ch:"updated_output_amount_raw" dbtype:"UInt256"`
	FillType               uint8      `ch:"fill_type" dbtype:"UInt8"`
	LiveReceivedAt         *time.Time `ch:"live_received_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	AvailableAt            time.Time  `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	PayloadHash            string     `ch:"payload_hash" dbtype:"FixedString(32)"`
}
type Refund struct {
	CaptureId         string     `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId           uint64     `ch:"chain_id" dbtype:"UInt64"`
	SpokePool         string     `ch:"spoke_pool" dbtype:"FixedString(20)"`
	BlockNumber       uint64     `ch:"block_number" dbtype:"UInt64"`
	BlockHash         string     `ch:"block_hash" dbtype:"FixedString(32)"`
	BlockTime         time.Time  `ch:"block_time" dbtype:"DateTime64(6, 'UTC')"`
	TxHash            string     `ch:"tx_hash" dbtype:"FixedString(32)"`
	TxIndex           uint32     `ch:"tx_index" dbtype:"UInt32"`
	LogIndex          uint32     `ch:"log_index" dbtype:"UInt32"`
	AbiRevision       string     `ch:"abi_revision" dbtype:"LowCardinality(String)"`
	EventKind         string     `ch:"event_kind" dbtype:"LowCardinality(String)"`
	Token             string     `ch:"token" dbtype:"FixedString(20)"`
	RootBundleId      *uint32    `ch:"root_bundle_id" dbtype:"Nullable(UInt32)"`
	LeafId            *uint32    `ch:"leaf_id" dbtype:"Nullable(UInt32)"`
	AmountToReturnRaw *big.Int   `ch:"amount_to_return_raw" dbtype:"Nullable(UInt256)"`
	RefundAddresses   []string   `ch:"refund_addresses" dbtype:"Array(FixedString(20))"`
	RefundAmountsRaw  []*big.Int `ch:"refund_amounts_raw" dbtype:"Array(UInt256)"`
	DeferredRefunds   *bool      `ch:"deferred_refunds" dbtype:"Nullable(Bool)"`
	Caller            string     `ch:"caller" dbtype:"FixedString(20)"`
	AvailableAt       time.Time  `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	PayloadHash       string     `ch:"payload_hash" dbtype:"FixedString(32)"`
}
type TxReceipt struct {
	CaptureId              string    `ch:"capture_id" dbtype:"FixedString(32)"`
	ChainId                uint64    `ch:"chain_id" dbtype:"UInt64"`
	BlockNumber            uint64    `ch:"block_number" dbtype:"UInt64"`
	BlockHash              string    `ch:"block_hash" dbtype:"FixedString(32)"`
	BlockTime              time.Time `ch:"block_time" dbtype:"DateTime64(6, 'UTC')"`
	TxHash                 string    `ch:"tx_hash" dbtype:"FixedString(32)"`
	TxIndex                uint32    `ch:"tx_index" dbtype:"UInt32"`
	Sender                 string    `ch:"sender" dbtype:"FixedString(20)"`
	Recipient              *string   `ch:"recipient" dbtype:"Nullable(FixedString(20))"`
	TxType                 uint8     `ch:"tx_type" dbtype:"UInt8"`
	InputSelector          *string   `ch:"input_selector" dbtype:"Nullable(FixedString(4))"`
	CalldataHash           string    `ch:"calldata_hash" dbtype:"FixedString(32)"`
	TxValueWei             *big.Int  `ch:"tx_value_wei" dbtype:"UInt256"`
	Success                bool      `ch:"success" dbtype:"Bool"`
	GasUsed                uint64    `ch:"gas_used" dbtype:"UInt64"`
	EffectiveGasPriceWei   *big.Int  `ch:"effective_gas_price_wei" dbtype:"UInt256"`
	ExecutionFeeWei        *big.Int  `ch:"execution_fee_wei" dbtype:"UInt256"`
	L1DataFeeWei           *big.Int  `ch:"l1_data_fee_wei" dbtype:"Nullable(UInt256)"`
	OperatorFeeWei         *big.Int  `ch:"operator_fee_wei" dbtype:"Nullable(UInt256)"`
	GasUsedForL1           *uint64   `ch:"gas_used_for_l1" dbtype:"Nullable(UInt64)"`
	TotalFeeWei            *big.Int  `ch:"total_fee_wei" dbtype:"Nullable(UInt256)"`
	FeeRule                string    `ch:"fee_rule" dbtype:"LowCardinality(String)"`
	FeeComplete            bool      `ch:"fee_complete" dbtype:"Bool"`
	Reason                 string    `ch:"reason" dbtype:"String"`
	RequestedAt            time.Time `ch:"requested_at" dbtype:"DateTime64(6, 'UTC')"`
	AvailableAt            time.Time `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	TransactionPayloadHash string    `ch:"transaction_payload_hash" dbtype:"FixedString(32)"`
	ReceiptPayloadHash     string    `ch:"receipt_payload_hash" dbtype:"FixedString(32)"`
}
type OrderProbe struct {
	CaptureId            string           `ch:"capture_id" dbtype:"FixedString(32)"`
	ProbeId              string           `ch:"probe_id" dbtype:"FixedString(32)"`
	ChainId              uint64           `ch:"chain_id" dbtype:"UInt64"`
	OriginChainId        uint64           `ch:"origin_chain_id" dbtype:"UInt64"`
	OriginSpokePool      string           `ch:"origin_spoke_pool" dbtype:"FixedString(20)"`
	OriginBlockHash      string           `ch:"origin_block_hash" dbtype:"FixedString(32)"`
	DepositId            *big.Int         `ch:"deposit_id" dbtype:"UInt256"`
	RelayHash            string           `ch:"relay_hash" dbtype:"FixedString(32)"`
	BlockNumber          *uint64          `ch:"block_number" dbtype:"Nullable(UInt64)"`
	BlockHash            *string          `ch:"block_hash" dbtype:"Nullable(FixedString(32))"`
	BlockTime            *time.Time       `ch:"block_time" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	ContractTime         *uint64          `ch:"contract_time" dbtype:"Nullable(UInt64)"`
	DestinationSpokePool string           `ch:"destination_spoke_pool" dbtype:"FixedString(20)"`
	PlannedForAt         time.Time        `ch:"planned_for_at" dbtype:"DateTime64(6, 'UTC')"`
	TargetDelayMs        *uint32          `ch:"target_delay_ms" dbtype:"Nullable(UInt32)"`
	RequestedAt          time.Time        `ch:"requested_at" dbtype:"DateTime64(6, 'UTC')"`
	AvailableAt          time.Time        `ch:"available_at" dbtype:"DateTime64(6, 'UTC')"`
	ObservationOrigin    string           `ch:"observation_origin" dbtype:"LowCardinality(String)"`
	ProbeStatus          string           `ch:"probe_status" dbtype:"LowCardinality(String)"`
	FillStatus           *uint8           `ch:"fill_status" dbtype:"Nullable(UInt8)"`
	PausedFills          *bool            `ch:"paused_fills" dbtype:"Nullable(Bool)"`
	Reason               string           `ch:"reason" dbtype:"String"`
	EthUsdtAsk           *decimal.Decimal `ch:"eth_usdt_ask" dbtype:"Nullable(Decimal(38,18))"`
	EthUsdtAskQty        *decimal.Decimal `ch:"eth_usdt_ask_qty" dbtype:"Nullable(Decimal(38,18))"`
	EthPriceRequestedAt  *time.Time       `ch:"eth_price_requested_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	EthPriceAvailableAt  *time.Time       `ch:"eth_price_available_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	EthPricePayloadHash  *string          `ch:"eth_price_payload_hash" dbtype:"Nullable(FixedString(32))"`
	UsdcUsdtBid          *decimal.Decimal `ch:"usdc_usdt_bid" dbtype:"Nullable(Decimal(38,18))"`
	UsdcUsdtAsk          *decimal.Decimal `ch:"usdc_usdt_ask" dbtype:"Nullable(Decimal(38,18))"`
	UsdcUsdtBidQty       *decimal.Decimal `ch:"usdc_usdt_bid_qty" dbtype:"Nullable(Decimal(38,18))"`
	UsdcUsdtAskQty       *decimal.Decimal `ch:"usdc_usdt_ask_qty" dbtype:"Nullable(Decimal(38,18))"`
	UsdcPriceRequestedAt *time.Time       `ch:"usdc_price_requested_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	UsdcPriceAvailableAt *time.Time       `ch:"usdc_price_available_at" dbtype:"Nullable(DateTime64(6, 'UTC'))"`
	UsdcPricePayloadHash *string          `ch:"usdc_price_payload_hash" dbtype:"Nullable(FixedString(32))"`
	PayloadHash          string           `ch:"payload_hash" dbtype:"FixedString(32)"`
}
type Batch struct {
	Capture  Capture
	Deposits []Deposit
	Updates  []DepositUpdate
	Fills    []Fill
	Refunds  []Refund
	Receipts []TxReceipt
	Probes   []OrderProbe
}
