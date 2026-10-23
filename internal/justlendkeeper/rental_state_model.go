package justlendkeeper

import (
	"errors"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// RentalStateObservation is a getRentInfo result, not a liquidation reward.
// Its before/after heads bracket node latest state; they do not pin execution.
type RentalStateObservation struct {
	CaptureId                 uuid.UUID  `ch:"capture_id"`
	CaptureStartedAt          time.Time  `ch:"capture_started_at"`
	ObservationIndex          uint32     `ch:"observation_index"`
	ContractAddress           string     `ch:"contract_address"`
	Renter                    string     `ch:"renter"`
	Receiver                  string     `ch:"receiver"`
	ResourceType              uint8      `ch:"resource_type"`
	CandidateOriginTx         *string    `ch:"candidate_origin_tx"`
	CandidateOriginEventIndex *uint32    `ch:"candidate_origin_event_index"`
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
	SecurityDepositSun        *big.Int   `ch:"security_deposit_sun"`
	RentIndex                 *big.Int   `ch:"rent_index"`
	Liquidatable              *bool      `ch:"liquidatable"`
	EnergyUsed                *uint64    `ch:"energy_used"`
	EnergyPenalty             *uint64    `ch:"energy_penalty"`
	ErrorCode                 string     `ch:"error_code"`
	ErrorMessage              string     `ch:"error_message"`
	RequestPayloadHash        *string    `ch:"request_payload_hash"`
	ResponsePayloadHash       *string    `ch:"response_payload_hash"`
}

// RentalStateBatchProof extends membership without changing historical Capture
// columns, migration hashes, or the canonical bytes of existing member types.
type RentalStateBatchProof struct {
	CaptureId        uuid.UUID `ch:"capture_id"`
	CaptureStartedAt time.Time `ch:"capture_started_at"`
	RowCount         uint32    `ch:"row_count"`
	Digest           string    `ch:"digest"`
}

func validateRentalStates(b Batch) error {
	c, p := b.Capture, b.RentalStateProof
	if c.CaptureKind != "rental_state" {
		if p != nil || len(b.RentalStates) != 0 {
			return errors.New("unexpected_rental_state_members")
		}
		return nil
	}
	if p == nil || p.CaptureId != c.CaptureId || !p.CaptureStartedAt.Equal(c.CaptureStartedAt) || p.RowCount != uint32(len(b.RentalStates)) || p.Digest != digest(b.RentalStates) {
		return errors.New("rental_state_members_mismatch")
	}
	if len(b.Events)+len(b.Receipts)+len(b.Costs)+len(b.IndexedEvents) != 0 || b.IndexPage != nil {
		return errors.New("rental_state_mixed_members")
	}
	seen := map[uint32]bool{}
	for _, r := range b.RentalStates {
		if seen[r.ObservationIndex] {
			return errors.New("duplicate_rental_state_index")
		}
		seen[r.ObservationIndex] = true
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.ContractAddress != c.ContractAddress || len(r.Renter) != 21 || len(r.Receiver) != 21 || len(r.CallerAddress) != 21 || r.ResourceType != 1 || r.ScheduledAt.IsZero() || r.AvailableAt.IsZero() || r.StateBinding != "node_latest_unpinned" || r.EndpointView != "wallet_latest" || r.Status == "" {
			return errors.New("invalid_rental_state")
		}
		if (r.CandidateOriginTx == nil) != (r.CandidateOriginEventIndex == nil) || (r.CandidateOriginTx != nil && len(*r.CandidateOriginTx) != 32) {
			return errors.New("invalid_rental_state_origin")
		}
		for _, n := range []*big.Int{r.SecurityDepositSun, r.RentIndex} {
			if n != nil && (n.Sign() < 0 || n.BitLen() > 256) {
				return errors.New("invalid_rental_state_uint256")
			}
		}
		for _, h := range []*string{r.HeadBeforeHash, r.HeadAfterHash, r.ImplementationCodeHash, r.RequestPayloadHash, r.ResponsePayloadHash} {
			if h != nil && len(*h) != 32 {
				return errors.New("invalid_rental_state_hash")
			}
		}
		if r.ImplementationAddress != nil && len(*r.ImplementationAddress) != 21 {
			return errors.New("invalid_rental_state_implementation")
		}
		if r.Status == "ok" {
			if r.ApiSuccess == nil || !*r.ApiSuccess || r.TvmResult != "SUCCESS" || !rentalStateIdentityVerified(r) || r.SecurityDepositSun == nil || r.RentIndex == nil || r.Liquidatable == nil {
				return errors.New("rental_state_success_unverified")
			}
			if r.HeadBeforeNumber == nil || r.HeadBeforeHash == nil || r.HeadBeforeTime == nil || r.HeadBeforeTime.IsZero() || r.HeadAfterNumber == nil || r.HeadAfterHash == nil || r.HeadAfterTime == nil || r.HeadAfterTime.IsZero() || *r.HeadAfterNumber < *r.HeadBeforeNumber || r.HeadAfterTime.Before(*r.HeadBeforeTime) || (*r.HeadAfterNumber == *r.HeadBeforeNumber && *r.HeadAfterHash != *r.HeadBeforeHash) {
				return errors.New("rental_state_missing_or_inconsistent_heads")
			}
			if r.RequestStartedAt == nil || r.RequestStartedAt.IsZero() || r.ResponseReceivedAt == nil || r.ResponseReceivedAt.IsZero() || r.RequestPayloadHash == nil || r.ResponsePayloadHash == nil || r.ResponseReceivedAt.Before(*r.RequestStartedAt) || r.AvailableAt.Before(*r.ResponseReceivedAt) {
				return errors.New("rental_state_missing_evidence")
			}
		}
	}
	for _, probe := range b.Probes {
		matched := false
		// A failed after-head downgrades status without erasing the parsed tuple.
		// Preserve the already performed probe; positive reward has its own stricter
		// persisted-evidence validation below, including both heads.
		for _, r := range b.RentalStates {
			if r.Liquidatable != nil && *r.Liquidatable && r.SecurityDepositSun != nil && r.RentIndex != nil && r.ApiSuccess != nil && *r.ApiSuccess && r.TvmResult == "SUCCESS" && rentalStateIdentityVerified(r) && r.RequestPayloadHash != nil && r.ResponsePayloadHash != nil && r.Renter == probe.Renter && r.Receiver == probe.Receiver && r.CallerAddress == probe.CallerAddress && r.ContractAddress == probe.ContractAddress && r.ResourceType == probe.ResourceType {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("rental_state_probe_without_eligible_state")
		}
	}
	return nil
}

func rentalStateIdentityVerified(r RentalStateObservation) bool {
	if r.RequestStartedAt == nil || r.RequestStartedAt.IsZero() || r.ResponseReceivedAt == nil || r.ResponseReceivedAt.IsZero() || r.ResponseReceivedAt.Before(*r.RequestStartedAt) || r.AvailableAt.Before(*r.ResponseReceivedAt) {
		return false
	}
	// Identity freshness belongs to the source observation. A delayed trailing
	// head or storage retry must not erase a tuple that was trusted when read.
	return probeIdentityVerified(Probe{IdentityStatus: r.IdentityStatus, ImplementationAddress: r.ImplementationAddress, ImplementationCodeHash: r.ImplementationCodeHash, ImplementationCheckedAt: r.ImplementationCheckedAt, RequestStartedAt: r.RequestStartedAt, AvailableAt: *r.ResponseReceivedAt})
}

// RentalStateProofEqual protects retries: Capture intentionally has no new
// member digest, so writers must compare this extension proof separately.
func RentalStateProofEqual(a, b *RentalStateBatchProof) bool {
	x, e := FactBytes(a)
	y, f := FactBytes(b)
	return e == nil && f == nil && string(x) == string(y)
}
