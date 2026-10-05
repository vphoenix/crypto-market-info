package justlendkeeper

import (
	"errors"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// IndexedEvent is an indexer's observation, never a verified chain fact.
// Ordinal preserves separate source rows, including repeated provider indices.
type IndexedEvent struct {
	CaptureId          uuid.UUID `ch:"capture_id"`
	CaptureStartedAt   time.Time `ch:"capture_started_at"`
	Ordinal            uint32    `ch:"row_ordinal"`
	ContractAddress    string    `ch:"contract_address"`
	BlockNumber        uint64    `ch:"block_number"`
	BlockHash          *string   `ch:"block_hash"`
	BlockTime          time.Time `ch:"block_time"`
	Finality           string    `ch:"finality"`
	TxId               string    `ch:"tx_id"`
	ProviderEventIndex uint32    `ch:"provider_event_index"`
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

func indexedRow(raw RawEvent, cap Capture, ordinal uint32) (IndexedEvent, error) {
	r, err := EventRow(raw)
	if err != nil {
		return IndexedEvent{}, err
	}
	return IndexedEvent{CaptureId: cap.CaptureId, CaptureStartedAt: cap.CaptureStartedAt,
		Ordinal: ordinal, ContractAddress: r.ContractAddress, BlockNumber: r.BlockNumber,
		BlockTime: r.BlockTime, Finality: "provider_claimed_confirmed", TxId: r.TxId,
		ProviderEventIndex: r.ProviderEventIndex, PositionStatus: "indexed_only",
		AbiRevision: r.AbiRevision, EventKind: r.EventKind, Renter: r.Renter, Receiver: r.Receiver,
		ResourceType: r.ResourceType, Liquidator: r.Liquidator, AmountSun: r.AmountSun,
		AddedAmountSun: r.AddedAmountSun, AddedDepositSun: r.AddedDepositSun,
		ReturnedAmountSun: r.ReturnedAmountSun, ReturnedDepositSun: r.ReturnedDepositSun,
		UsageRentalSun: r.UsageRentalSun, RewardSun: r.RewardSun, SendBackSun: r.SendBackSun,
		SecurityDepositSun: r.SecurityDepositSun, RentIndex: r.RentIndex,
		RequestStartedAt: r.RequestStartedAt, AvailableAt: r.AvailableAt, PayloadHash: r.PayloadHash}, nil
}

func validateIndexed(b Batch) error {
	c := b.Capture
	if c.CaptureKind == "enrichment" {
		if c.ParentCaptureId == nil || *c.ParentCaptureId == uuid.Nil {
			return errors.New("missing_enrichment_parent")
		}
	} else if c.ParentCaptureId != nil {
		return errors.New("unexpected_enrichment_parent")
	}
	if c.CaptureKind == "event_index" {
		if c.RequestedFrom == nil || c.RequestedTo == nil || !c.RequestedFrom.Before(*c.RequestedTo) || c.SourceId != "trongrid" || (c.EventKind != "RentResource" && c.EventKind != "ReturnResource" && c.EventKind != "Liquidate") || c.CoverageScope != "indexed_energy_events" || c.SolidHash != nil || c.SolidHeight != nil {
			return errors.New("invalid_indexed_capture_window")
		}
		if c.Status == "complete" && (c.PageCount != 1 || c.DiscoveredCandidates != uint32(len(b.IndexedEvents))) {
			return errors.New("invalid_indexed_capture_counts")
		}
		if c.IndexedDigest == nil || *c.IndexedDigest != digest(b.IndexedEvents) || c.IndexedRows != uint32(len(b.IndexedEvents)) || c.EventRows+c.ReceiptRows+c.ProbeRows+c.CostRows != 0 {
			return errors.New("indexed_capture_members_mismatch")
		}
	} else if c.IndexedDigest != nil || c.IndexedRows != 0 || len(b.IndexedEvents) != 0 {
		return errors.New("unexpected_indexed_members")
	}
	seen := map[uint32]bool{}
	if b.IndexPage != nil {
		if err := validateIndexPage(c, *b.IndexPage); err != nil {
			return err
		}
	}
	for _, r := range b.IndexedEvents {
		if r.ContractAddress != c.ContractAddress || c.RequestedFrom == nil || c.RequestedTo == nil || r.BlockTime.Before(*c.RequestedFrom) || !r.BlockTime.Before(*c.RequestedTo) || r.RequestStartedAt.IsZero() || r.AvailableAt.Before(r.RequestStartedAt) {
			return errors.New("indexed_event_filter_mismatch")
		}
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || seen[r.Ordinal] || r.Ordinal >= 200 || r.BlockNumber == 0 || r.BlockHash != nil || r.BlockTime.IsZero() || r.Finality != "provider_claimed_confirmed" || r.PositionStatus != "indexed_only" || len(r.ContractAddress) != 21 || len(r.TxId) != 32 || len(r.PayloadHash) != 32 || len(r.Renter) != 21 || len(r.Receiver) != 21 || r.ResourceType != 1 || r.AmountSun == nil || r.AmountSun.Sign() < 0 || r.AmountSun.BitLen() > 256 {
			return errors.New("invalid_indexed_event")
		}
		seen[r.Ordinal] = true
		if _, err := rawFromIndexed(r); err != nil {
			return err
		}
		if b.IndexPage != nil && (r.PayloadHash != b.IndexPage.PayloadHash || !r.RequestStartedAt.Equal(b.IndexPage.RequestStartedAt) || !r.AvailableAt.Equal(b.IndexPage.AvailableAt)) {
			return errors.New("indexed_page_source_mismatch")
		}
		for _, v := range []*big.Int{r.AddedAmountSun, r.AddedDepositSun, r.ReturnedAmountSun, r.ReturnedDepositSun, r.UsageRentalSun, r.RewardSun, r.SendBackSun, r.SecurityDepositSun, r.RentIndex} {
			if v != nil && (v.Sign() < 0 || v.BitLen() > 256) {
				return errors.New("invalid_indexed_uint256")
			}
		}
		if (c.EventKind == "RentResource" && r.EventKind != "rent") || (c.EventKind == "ReturnResource" && r.EventKind != "return") || (c.EventKind == "Liquidate" && r.EventKind != "liquidate") {
			return errors.New("indexed_event_kind_mismatch")
		}
	}
	return nil
}

// Reuse the existing receipt matcher with values read from typed database rows.
// These JSON scalar values are transient; no source response is read or stored.
func rawFromIndexed(r IndexedEvent) (RawEvent, error) {
	if len(r.Renter) != 21 || len(r.Receiver) != 21 || r.AmountSun == nil || (r.SecurityDepositSun == nil) != (r.RentIndex == nil) || (r.AbiRevision == seedRevision && r.SecurityDepositSun == nil) {
		return RawEvent{}, errors.New("incomplete_indexed_event")
	}
	switch r.EventKind {
	case "rent":
		if r.AddedAmountSun == nil || r.AddedDepositSun == nil {
			return RawEvent{}, errors.New("incomplete_indexed_rent")
		}
	case "return":
		if r.ReturnedAmountSun == nil || r.ReturnedDepositSun == nil || r.UsageRentalSun == nil {
			return RawEvent{}, errors.New("incomplete_indexed_return")
		}
	case "liquidate":
		if r.Liquidator == nil || len(*r.Liquidator) != 21 || r.UsageRentalSun == nil || r.RewardSun == nil || r.SendBackSun == nil {
			return RawEvent{}, errors.New("incomplete_indexed_liquidation")
		}
	default:
		return RawEvent{}, errors.New("unknown_indexed_event")
	}
	v := RentalEvent{ContractAddress: r.ContractAddress, BlockNumber: r.BlockNumber, BlockTime: r.BlockTime,
		TxId: r.TxId, ProviderEventIndex: r.ProviderEventIndex, AbiRevision: r.AbiRevision,
		EventKind: r.EventKind, Renter: r.Renter, Receiver: r.Receiver, ResourceType: r.ResourceType,
		Liquidator: r.Liquidator, AmountSun: r.AmountSun, AddedAmountSun: r.AddedAmountSun,
		AddedDepositSun: r.AddedDepositSun, ReturnedAmountSun: r.ReturnedAmountSun,
		ReturnedDepositSun: r.ReturnedDepositSun, UsageRentalSun: r.UsageRentalSun,
		RewardSun: r.RewardSun, SendBackSun: r.SendBackSun, SecurityDepositSun: r.SecurityDepositSun,
		RentIndex: r.RentIndex, RequestStartedAt: r.RequestStartedAt, AvailableAt: r.AvailableAt, PayloadHash: r.PayloadHash}
	raw := RawFromRow(v)
	raw.Ordinal = r.Ordinal
	back, err := EventRow(raw)
	if err != nil || back.AbiRevision != r.AbiRevision || !EventEqual(v, back) {
		return RawEvent{}, errors.New("indexed_event_roundtrip_mismatch")
	}
	return raw, nil
}

func captureEqual(a, b Capture) bool {
	x, err := FactBytes(a)
	y, err2 := FactBytes(b)
	return err == nil && err2 == nil && string(x) == string(y)
}

func ValidateEnrichmentParent(child, p Capture) error {
	if child.ParentCaptureId == nil || *child.ParentCaptureId != p.CaptureId || !p.Committed || p.CaptureKind != "event_index" || p.Status != "complete" || p.ConfigHash != child.ConfigHash || p.EventKind != child.EventKind || p.ContractAddress != child.ContractAddress || p.RequestedFrom == nil || child.RequestedFrom == nil || p.RequestedTo == nil || child.RequestedTo == nil || !p.RequestedFrom.Equal(*child.RequestedFrom) || !p.RequestedTo.Equal(*child.RequestedTo) || child.DiscoveredCandidates != p.IndexedRows {
		return errors.New("enrichment_parent_mismatch")
	}
	return nil
}
