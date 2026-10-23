package justlendkeeper

import (
	"errors"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

const DynamicPolicy = "dynamic-40-10-v1"

// SelectionMember records the inputs used at admission, not a later re-score.
type SelectionMember struct {
	CaptureId            uuid.UUID  `ch:"capture_id"`
	CaptureStartedAt     time.Time  `ch:"capture_started_at"`
	RoundId              uuid.UUID  `ch:"round_id"`
	SlotNo               uint8      `ch:"slot_no"`
	Renter               string     `ch:"renter" cht:"FixedString(21)"`
	Receiver             string     `ch:"receiver" cht:"FixedString(21)"`
	Generation           string     `ch:"generation" cht:"FixedString(32)"`
	RequestedLane        string     `ch:"requested_lane"`
	ActualLane           string     `ch:"actual_lane"`
	SelectedAt           time.Time  `ch:"selected_at"`
	Reason               string     `ch:"reason"`
	EstimatorRevision    string     `ch:"estimator_revision"`
	AmountSun            *big.Int   `ch:"amount_sun"`
	DepositSun           *big.Int   `ch:"deposit_sun"`
	Rate                 *big.Int   `ch:"rate"`
	BaseTime             *time.Time `ch:"base_time"`
	OriginTx             string     `ch:"origin_tx" cht:"FixedString(32)"`
	OriginEventIndex     uint32     `ch:"origin_event_index"`
	InputDigest          string     `ch:"input_digest" cht:"FixedString(32)"`
	RateCaptureId        *uuid.UUID `ch:"rate_capture_id"`
	StateCaptureId       *uuid.UUID `ch:"state_capture_id"`
	CoverageTime         time.Time  `ch:"coverage_time"`
	Outcome              string     `ch:"outcome"`
	SeedCaptureId        *uuid.UUID `ch:"seed_capture_id"`
	StateResultCaptureId *uuid.UUID `ch:"state_result_capture_id"`
	RPCAttempts          uint32     `ch:"rpc_attempts"`
}

type SelectionRound struct {
	CaptureId                 uuid.UUID `ch:"capture_id"`
	CaptureStartedAt          time.Time `ch:"capture_started_at"`
	RoundId                   uuid.UUID `ch:"round_id"`
	Policy                    string    `ch:"policy"`
	StartedAt                 time.Time `ch:"started_at"`
	FinishedAt                time.Time `ch:"finished_at"`
	FocusTarget               uint8     `ch:"focus_target"`
	ExploreTarget             uint8     `ch:"explore_target"`
	FocusCount                uint8     `ch:"focus_count"`
	ExploreCount              uint8     `ch:"explore_count"`
	Reason                    string    `ch:"reason"`
	Population                uint64    `ch:"population"`
	MaxUnattemptedAgeSeconds  uint64    `ch:"max_unattempted_age_seconds"`
	MaxUnsuccessfulAgeSeconds uint64    `ch:"max_unsuccessful_age_seconds"`
	Quarantined               uint64    `ch:"quarantined"`
	CoverageTime              time.Time `ch:"coverage_time"`
	CoverageScope             string    `ch:"coverage_scope"`
	MemberCount               uint32    `ch:"member_count"`
	MemberDigest              string    `ch:"member_digest" cht:"FixedString(32)"`
}

// RentalRateObservation uses the same source evidence as a state getter, with a
// dedicated numeric rate field. It never masquerades as a deposit or yield APR.
type RentalRateObservation struct {
	CaptureId               uuid.UUID  `ch:"capture_id"`
	CaptureStartedAt        time.Time  `ch:"capture_started_at"`
	ObservationIndex        uint32     `ch:"observation_index"`
	ContractAddress         string     `ch:"contract_address" cht:"FixedString(21)"`
	ResourceType            uint8      `ch:"resource_type"`
	RequestStartedAt        *time.Time `ch:"request_started_at"`
	ResponseReceivedAt      *time.Time `ch:"response_received_at"`
	AvailableAt             time.Time  `ch:"available_at"`
	HeadBeforeNumber        *uint64    `ch:"head_before_number"`
	HeadBeforeHash          *string    `ch:"head_before_hash" cht:"Nullable(FixedString(32))"`
	HeadBeforeTime          *time.Time `ch:"head_before_time"`
	HeadAfterNumber         *uint64    `ch:"head_after_number"`
	HeadAfterHash           *string    `ch:"head_after_hash" cht:"Nullable(FixedString(32))"`
	HeadAfterTime           *time.Time `ch:"head_after_time"`
	ImplementationAddress   *string    `ch:"implementation_address" cht:"Nullable(FixedString(21))"`
	ImplementationCodeHash  *string    `ch:"implementation_code_hash" cht:"Nullable(FixedString(32))"`
	ImplementationCheckedAt *time.Time `ch:"implementation_checked_at"`
	IdentityStatus          string     `ch:"identity_status"`
	Status                  string     `ch:"status"`
	ApiSuccess              *bool      `ch:"api_success"`
	TvmResult               string     `ch:"tvm_result"`
	Rate                    *big.Int   `ch:"rate"`
	Scale                   uint64     `ch:"scale"`
	StateBinding            string     `ch:"state_binding"`
	ErrorCode               string     `ch:"error_code"`
	RequestPayloadHash      *string    `ch:"request_payload_hash" cht:"Nullable(FixedString(32))"`
	ResponsePayloadHash     *string    `ch:"response_payload_hash" cht:"Nullable(FixedString(32))"`
}

type RentalRateBatchProof struct {
	CaptureId        uuid.UUID `ch:"capture_id"`
	CaptureStartedAt time.Time `ch:"capture_started_at"`
	RowCount         uint32    `ch:"row_count"`
	Digest           string    `ch:"digest" cht:"FixedString(32)"`
}

type DynamicTable struct {
	Name  string
	Key   []string
	Model any
}
type DynamicColumn struct{ Name, Type string }

func DynamicTables() []DynamicTable {
	return []DynamicTable{
		{"jl_keeper_rental_rate", []string{"capture_id", "observation_index"}, RentalRateObservation{}},
		{"jl_keeper_rental_rate_batch", []string{"capture_id"}, RentalRateBatchProof{}},
		{"jl_keeper_selection_member", []string{"capture_id", "slot_no"}, SelectionMember{}},
		{"jl_keeper_selection_round", []string{"capture_id"}, SelectionRound{}},
	}
}

// Exact typed extension catalog; never part of the frozen 125-table migration.
func DynamicColumns(model any) []DynamicColumn {
	t := reflect.TypeOf(model)
	out := make([]DynamicColumn, t.NumField())
	for i := range out {
		f := t.Field(i)
		kind := f.Tag.Get("cht")
		if kind == "" {
			kind = dynamicType(f.Type)
		}
		out[i] = DynamicColumn{f.Tag.Get("ch"), kind}
	}
	return out
}
func dynamicType(t reflect.Type) string {
	if t == reflect.TypeOf(time.Time{}) {
		return "DateTime64(6, 'UTC')"
	}
	if t == reflect.TypeOf(uuid.UUID{}) {
		return "UUID"
	}
	if t == reflect.TypeOf(big.Int{}) {
		return "UInt256"
	}
	if t.Kind() == reflect.Pointer {
		return "Nullable(" + dynamicType(t.Elem()) + ")"
	}
	switch t.Kind() {
	case reflect.String:
		return "String"
	case reflect.Bool:
		return "Bool"
	case reflect.Uint8:
		return "UInt8"
	case reflect.Uint32:
		return "UInt32"
	case reflect.Uint64:
		return "UInt64"
	default:
		panic("unsupported keeper dynamic model type: " + t.String())
	}
}

func DynamicProofEqual(a, b Batch) bool {
	x, e := FactBytes(a.RateProof)
	y, f := FactBytes(b.RateProof)
	u, g := FactBytes(a.SelectionRound)
	v, h := FactBytes(b.SelectionRound)
	return e == nil && f == nil && g == nil && h == nil && string(x) == string(y) && string(u) == string(v)
}

func validateDynamic(b Batch) error {
	c := b.Capture
	if c.CaptureKind != "rental_rate" && (len(b.Rates) > 0 || b.RateProof != nil) {
		return errors.New("unexpected_rental_rate_members")
	}
	if c.CaptureKind != "selection_round" && (len(b.Selections) > 0 || b.SelectionRound != nil) {
		return errors.New("unexpected_selection_members")
	}
	if c.CaptureKind == "rental_rate" {
		p := b.RateProof
		if p == nil || p.CaptureId != c.CaptureId || !p.CaptureStartedAt.Equal(c.CaptureStartedAt) || p.RowCount != uint32(len(b.Rates)) || p.Digest != digest(b.Rates) || len(b.Rates) > 1 {
			return errors.New("rental_rate_members_mismatch")
		}
		for _, r := range b.Rates {
			if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.ContractAddress != c.ContractAddress || r.ResourceType != 1 || r.Scale != 1000000000000000000 || r.Status == "" || r.StateBinding != "node_latest_unpinned" || r.AvailableAt.IsZero() || r.Rate != nil && (r.Rate.Sign() < 0 || r.Rate.BitLen() > 256) {
				return errors.New("invalid_rental_rate")
			}
			for _, v := range []*string{r.RequestPayloadHash, r.ResponsePayloadHash, r.HeadBeforeHash, r.HeadAfterHash, r.ImplementationCodeHash} {
				if v != nil && len(*v) != 32 {
					return errors.New("invalid_rate_hash")
				}
			}
			if r.Status == "ok" && !validRentalRate(r) {
				return errors.New("unverified_rental_rate")
			}
		}
	}
	if c.CaptureKind == "selection_round" {
		p := b.SelectionRound
		if p == nil || p.CaptureId != c.CaptureId || !p.CaptureStartedAt.Equal(c.CaptureStartedAt) || p.RoundId == uuid.Nil || p.Policy != DynamicPolicy || p.FocusTarget != 40 || p.ExploreTarget != 10 || p.MemberCount != uint32(len(b.Selections)) || p.MemberDigest != digest(b.Selections) || len(b.Selections) > 50 || p.FinishedAt.Before(p.StartedAt) || p.Reason == "" {
			return errors.New("selection_members_mismatch")
		}
		seen := map[string]bool{}
		focus, explore := uint8(0), uint8(0)
		for i, r := range b.Selections {
			pair := r.Renter + r.Receiver
			if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.RoundId != p.RoundId || int(r.SlotNo) != i+1 || len(r.Renter) != 21 || len(r.Receiver) != 21 || len(r.Generation) != 32 || len(r.OriginTx) != 32 || len(r.InputDigest) != 32 || r.InputDigest != SelectionInputDigest(r) || r.SelectedAt.Before(p.StartedAt) || seen[pair] || r.Outcome == "" {
				return errors.New("invalid_selection_member")
			}
			seen[pair] = true
			want := "focus"
			if r.SlotNo%5 == 0 {
				want = "explore"
			}
			if r.RequestedLane != want {
				return errors.New("selection_lane_sequence")
			}
			if r.ActualLane == "focus" {
				focus++
			} else if r.ActualLane == "explore" {
				explore++
			} else {
				return errors.New("invalid_selection_lane")
			}
			for _, v := range []*big.Int{r.AmountSun, r.DepositSun, r.Rate} {
				if v != nil && (v.Sign() < 0 || v.BitLen() > 256) {
					return errors.New("invalid_selection_uint256")
				}
			}
		}
		if p.FocusCount != focus || p.ExploreCount != explore {
			return errors.New("selection_counts_mismatch")
		}
	}
	if c.CaptureKind == "selection_round" || c.CaptureKind == "rental_rate" {
		if len(b.Events)+len(b.Receipts)+len(b.Probes)+len(b.Costs)+len(b.IndexedEvents)+len(b.RentalStates) > 0 || b.IndexPage != nil || b.RentalStateProof != nil {
			return errors.New("dynamic_mixed_members")
		}
	}
	return nil
}

func validRentalRate(r RentalRateObservation) bool {
	return r.Rate != nil && r.Rate.Sign() >= 0 && r.ApiSuccess != nil && *r.ApiSuccess && r.TvmResult == "SUCCESS" && r.RequestStartedAt != nil && r.ResponseReceivedAt != nil && !r.ResponseReceivedAt.Before(*r.RequestStartedAt) && !r.AvailableAt.Before(*r.ResponseReceivedAt) && r.RequestPayloadHash != nil && r.ResponsePayloadHash != nil && r.HeadBeforeNumber != nil && r.HeadAfterNumber != nil && *r.HeadAfterNumber >= *r.HeadBeforeNumber && r.HeadBeforeTime != nil && r.HeadAfterTime != nil && !r.HeadAfterTime.Before(*r.HeadBeforeTime) && r.HeadBeforeHash != nil && r.HeadAfterHash != nil && (*r.HeadBeforeNumber != *r.HeadAfterNumber || *r.HeadBeforeHash == *r.HeadAfterHash) && probeIdentityVerified(Probe{IdentityStatus: r.IdentityStatus, ImplementationAddress: r.ImplementationAddress, ImplementationCodeHash: r.ImplementationCodeHash, ImplementationCheckedAt: r.ImplementationCheckedAt, RequestStartedAt: r.RequestStartedAt, AvailableAt: *r.ResponseReceivedAt})
}
