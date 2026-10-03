// Package justlendkeeper collects public TRON energy-rental evidence.
// FixedString values are binary strings: freeze batches with gob, never JSON.
package justlendkeeper

import (
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"math/big"
	"time"
)

type NativeTransfer struct {
	InternalIndex uint32  `ch:"internal_index"`
	ValueIndex    uint32  `ch:"value_index"`
	Sender        string  `ch:"sender"`
	Recipient     string  `ch:"recipient"`
	AmountSun     big.Int `ch:"amount_sun"`
	Rejected      bool    `ch:"rejected"`
}
type Capture struct {
	CaptureId            uuid.UUID  `ch:"capture_id"`
	CaptureStartedAt     time.Time  `ch:"capture_started_at"`
	ConfigHash           string     `ch:"config_hash"`
	Network              string     `ch:"network"`
	ContractAddress      string     `ch:"contract_address"`
	CaptureKind          string     `ch:"capture_kind"`
	CaptureMode          string     `ch:"capture_mode"`
	SourceId             string     `ch:"source_id"`
	RequestedFrom        *time.Time `ch:"requested_from"`
	RequestedTo          *time.Time `ch:"requested_to"`
	EventKind            string     `ch:"event_kind"`
	CoverageScope        string     `ch:"coverage_scope"`
	SolidHeight          *uint64    `ch:"solid_height"`
	SolidHash            *string    `ch:"solid_hash"`
	AvailableAt          time.Time  `ch:"available_at"`
	Status               string     `ch:"status"`
	Reason               string     `ch:"reason"`
	ExpectedTasks        uint32     `ch:"expected_tasks"`
	CompletedTasks       uint32     `ch:"completed_tasks"`
	PageCount            uint32     `ch:"page_count"`
	PaginationExhausted  bool       `ch:"pagination_exhausted"`
	DiscoveredCandidates uint32     `ch:"discovered_candidates"`
	SelectedCandidates   uint32     `ch:"selected_candidates"`
	SkippedCandidates    uint32     `ch:"skipped_candidates"`
	EventRows            uint32     `ch:"event_rows"`
	EventDigest          string     `ch:"event_digest"`
	ReceiptRows          uint32     `ch:"receipt_rows"`
	ReceiptDigest        string     `ch:"receipt_digest"`
	ProbeRows            uint32     `ch:"probe_rows"`
	ProbeDigest          string     `ch:"probe_digest"`
	CostRows             uint32     `ch:"cost_rows"`
	CostDigest           string     `ch:"cost_digest"`
	EvidenceManifestHash string     `ch:"evidence_manifest_hash"`
	Committed            bool       `ch:"committed"`
}
type RentalEvent struct {
	CaptureId          uuid.UUID `ch:"capture_id"`
	CaptureStartedAt   time.Time `ch:"capture_started_at"`
	ContractAddress    string    `ch:"contract_address"`
	BlockNumber        uint64    `ch:"block_number"`
	BlockHash          string    `ch:"block_hash"`
	BlockTime          time.Time `ch:"block_time"`
	Finality           string    `ch:"finality"`
	TxId               string    `ch:"tx_id"`
	TransactionIndex   *uint32   `ch:"transaction_index"`
	ProviderEventIndex uint32    `ch:"provider_event_index"`
	ReceiptLogIndex    *uint32   `ch:"receipt_log_index"`
	PositionStatus     string    `ch:"position_status"`
	AbiRevision        string    `ch:"abi_revision"`
	EventKind          string    `ch:"event_kind"`
	Renter             string    `ch:"renter"`
	Receiver           string    `ch:"receiver"`
	ResourceType       uint8     `ch:"resource_type"`
	Liquidator         *string   `ch:"liquidator"`
	AmountSun          *big.Int  `ch:"amount_sun"`
	AddedAmountSun     *big.Int  `ch:"added_amount_sun"`
	AddedDepositSun    *big.Int  `ch:"added_deposit_sun"`
	ReturnedAmountSun  *big.Int  `ch:"returned_amount_sun"`
	ReturnedDepositSun *big.Int  `ch:"returned_deposit_sun"`
	UsageRentalSun     *big.Int  `ch:"usage_rental_sun"`
	RewardSun          *big.Int  `ch:"reward_sun"`
	SendBackSun        *big.Int  `ch:"send_back_sun"`
	SecurityDepositSun *big.Int  `ch:"security_deposit_sun"`
	RentIndex          *big.Int  `ch:"rent_index"`
	RequestStartedAt   time.Time `ch:"request_started_at"`
	AvailableAt        time.Time `ch:"available_at"`
	PayloadHash        string    `ch:"payload_hash"`
}
type TxReceipt struct {
	CaptureId                 uuid.UUID        `ch:"capture_id"`
	CaptureStartedAt          time.Time        `ch:"capture_started_at"`
	BlockNumber               uint64           `ch:"block_number"`
	BlockHash                 string           `ch:"block_hash"`
	BlockTime                 time.Time        `ch:"block_time"`
	Finality                  string           `ch:"finality"`
	TxId                      string           `ch:"tx_id"`
	Sender                    *string          `ch:"sender"`
	OuterTarget               *string          `ch:"outer_target"`
	OuterSelector             *string          `ch:"outer_selector"`
	OuterCallValueSun         *big.Int         `ch:"outer_call_value_sun"`
	ExecutionResult           string           `ch:"execution_result"`
	BodyComplete              bool             `ch:"body_complete"`
	ReceiptComplete           bool             `ch:"receipt_complete"`
	FeeSun                    *uint64          `ch:"fee_sun"`
	EnergyUsageTotal          *uint64          `ch:"energy_usage_total"`
	EnergyUsage               *uint64          `ch:"energy_usage"`
	OriginEnergyUsage         *uint64          `ch:"origin_energy_usage"`
	EnergyFeeSun              *uint64          `ch:"energy_fee_sun"`
	EnergyPenaltyTotal        *uint64          `ch:"energy_penalty_total"`
	NetUsage                  *uint64          `ch:"net_usage"`
	NetFeeSun                 *uint64          `ch:"net_fee_sun"`
	MultisignFeeSun           *uint64          `ch:"multisign_fee_sun"`
	MemoFeeSun                *uint64          `ch:"memo_fee_sun"`
	NativeTransferStatus      string           `ch:"native_transfer_status"`
	NativeTransfers           []NativeTransfer `ch:"native_transfers"`
	RentalLiquidationLogCount uint32           `ch:"rental_liquidation_log_count"`
	OtherLogCount             uint32           `ch:"other_log_count"`
	CallClass                 string           `ch:"call_class"`
	RequestStartedAt          time.Time        `ch:"request_started_at"`
	AvailableAt               time.Time        `ch:"available_at"`
	BodyPayloadHash           *string          `ch:"body_payload_hash"`
	ReceiptPayloadHash        string           `ch:"receipt_payload_hash"`
}
type Probe struct {
	CaptureId                 uuid.UUID  `ch:"capture_id"`
	CaptureStartedAt          time.Time  `ch:"capture_started_at"`
	ProbeIndex                uint32     `ch:"probe_index"`
	ContractAddress           string     `ch:"contract_address"`
	Renter                    string     `ch:"renter"`
	Receiver                  string     `ch:"receiver"`
	ResourceType              uint8      `ch:"resource_type"`
	CandidateOriginTx         *string    `ch:"candidate_origin_tx"`
	CandidateOriginEventIndex *uint32    `ch:"candidate_origin_event_index"`
	CohortId                  uuid.UUID  `ch:"cohort_id"`
	CallerAddress             string     `ch:"caller_address"`
	ScheduledAt               time.Time  `ch:"scheduled_at"`
	RequestStartedAt          *time.Time `ch:"request_started_at"`
	ResponseReceivedAt        *time.Time `ch:"response_received_at"`
	AvailableAt               time.Time  `ch:"available_at"`
	EndpointView              string     `ch:"endpoint_view"`
	StateBinding              string     `ch:"state_binding"`
	HeadBeforeNumber          *uint64    `ch:"head_before_number"`
	HeadBeforeHash            *string    `ch:"head_before_hash"`
	HeadBeforeTime            *time.Time `ch:"head_before_time"`
	HeadAfterNumber           *uint64    `ch:"head_after_number"`
	HeadAfterHash             *string    `ch:"head_after_hash"`
	HeadAfterTime             *time.Time `ch:"head_after_time"`
	ImplementationAddress     *string    `ch:"implementation_address"`
	ImplementationCodeHash    *string    `ch:"implementation_code_hash"`
	ImplementationCheckedAt   *time.Time `ch:"implementation_checked_at"`
	IdentityStatus            string     `ch:"identity_status"`
	Status                    string     `ch:"status"`
	ApiSuccess                *bool      `ch:"api_success"`
	TvmResult                 string     `ch:"tvm_result"`
	RewardReturnSun           *big.Int   `ch:"reward_return_sun"`
	RewardLogSun              *big.Int   `ch:"reward_log_sun"`
	RewardTransferSun         *big.Int   `ch:"reward_transfer_sun"`
	EnergyUsed                *uint64    `ch:"energy_used"`
	EnergyPenalty             *uint64    `ch:"energy_penalty"`
	RewardConsistency         string     `ch:"reward_consistency"`
	ErrorCode                 string     `ch:"error_code"`
	ErrorMessage              string     `ch:"error_message"`
	RequestPayloadHash        *string    `ch:"request_payload_hash"`
	ResponsePayloadHash       *string    `ch:"response_payload_hash"`
}
type CostObservation struct {
	CaptureId                   uuid.UUID        `ch:"capture_id"`
	CaptureStartedAt            time.Time        `ch:"capture_started_at"`
	ObservationIndex            uint32           `ch:"observation_index"`
	ObservationKind             string           `ch:"observation_kind"`
	SourceId                    string           `ch:"source_id"`
	RequestStartedAt            time.Time        `ch:"request_started_at"`
	ReceivedAt                  *time.Time       `ch:"received_at"`
	AvailableAt                 time.Time        `ch:"available_at"`
	SourceTime                  *time.Time       `ch:"source_time"`
	SourceTimeKind              string           `ch:"source_time_kind"`
	HeadBeforeNumber            *uint64          `ch:"head_before_number"`
	HeadBeforeHash              *string          `ch:"head_before_hash"`
	HeadAfterNumber             *uint64          `ch:"head_after_number"`
	HeadAfterHash               *string          `ch:"head_after_hash"`
	StateBinding                string           `ch:"state_binding"`
	EnergyFeeSunPerUnit         *uint64          `ch:"energy_fee_sun_per_unit"`
	BandwidthFeeSunPerByte      *uint64          `ch:"bandwidth_fee_sun_per_byte"`
	ContractUserResourcePercent *uint8           `ch:"contract_user_resource_percent"`
	ContractOriginEnergyLimit   *uint64          `ch:"contract_origin_energy_limit"`
	ImplementationAddress       *string          `ch:"implementation_address"`
	ImplementationCodeHash      *string          `ch:"implementation_code_hash"`
	Symbol                      string           `ch:"symbol"`
	BidPriceUsdt                *decimal.Decimal `ch:"bid_price_usdt"`
	BidQtyTrx                   *decimal.Decimal `ch:"bid_qty_trx"`
	AskPriceUsdt                *decimal.Decimal `ch:"ask_price_usdt"`
	AskQtyTrx                   *decimal.Decimal `ch:"ask_qty_trx"`
	Status                      string           `ch:"status"`
	Reason                      string           `ch:"reason"`
	PayloadHash                 *string          `ch:"payload_hash"`
}
type Batch struct {
	Capture  Capture
	Events   []RentalEvent
	Receipts []TxReceipt
	Probes   []Probe
	Costs    []CostObservation
}
