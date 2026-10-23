package justlendkeeper

import (
	"encoding/hex"
	"math/big"
	"time"
)

func ParseRentalState(body []byte, ev Evidence, can Candidate, caller string, scheduled time.Time, c *Collector) (RentalStateObservation, error) {
	p, s, ready, err := parseConstantExecution(body, ev, can, caller, scheduled, c)
	r := RentalStateObservation{
		ContractAddress: p.ContractAddress, Renter: p.Renter, Receiver: p.Receiver, ResourceType: p.ResourceType,
		CandidateOriginTx: p.CandidateOriginTx, CandidateOriginEventIndex: p.CandidateOriginEventIndex, CallerAddress: p.CallerAddress,
		ScheduledAt: p.ScheduledAt, RequestStartedAt: p.RequestStartedAt, ResponseReceivedAt: p.ResponseReceivedAt, AvailableAt: p.AvailableAt,
		EndpointView: p.EndpointView, StateBinding: p.StateBinding, ImplementationAddress: p.ImplementationAddress,
		ImplementationCodeHash: p.ImplementationCodeHash, ImplementationCheckedAt: p.ImplementationCheckedAt, IdentityStatus: p.IdentityStatus,
		Status: p.Status, ApiSuccess: p.ApiSuccess, TvmResult: p.TvmResult, EnergyUsed: p.EnergyUsed, EnergyPenalty: p.EnergyPenalty,
		ErrorCode: p.ErrorCode, ErrorMessage: p.ErrorMessage, RequestPayloadHash: p.RequestPayloadHash, ResponsePayloadHash: p.ResponsePayloadHash,
	}
	if err != nil || !ready {
		return r, err
	}
	r.TvmResult = "SUCCESS"
	if len(s.Constant) != 1 || len(s.Constant[0]) != 192 {
		r.ErrorCode = "invalid_rental_state_abi"
		return r, nil
	}
	raw, err := hex.DecodeString(s.Constant[0])
	if err != nil {
		r.ErrorCode = "invalid_rental_state_hex"
		return r, nil
	}
	flag := new(big.Int).SetBytes(raw[64:96])
	if flag.Cmp(big.NewInt(1)) > 0 {
		r.ErrorCode = "invalid_rental_state_bool"
		return r, nil
	}
	r.SecurityDepositSun = new(big.Int).SetBytes(raw[:32])
	r.RentIndex = new(big.Int).SetBytes(raw[32:64])
	r.Liquidatable = Ptr(flag.Sign() == 1)
	if !probeIdentityVerified(p) {
		r.ErrorCode = "implementation_identity_unverified"
		return r, nil
	}
	r.Status = "ok"
	r.ErrorCode = ""
	return r, nil
}
