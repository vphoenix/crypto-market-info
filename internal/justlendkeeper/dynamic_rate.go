package justlendkeeper

import (
	"encoding/hex"
	"math/big"
	"strings"
	"time"
)

func (c *Collector) addRentalRate(now time.Time) {
	w := c.State.Dynamic
	o := c.NewOperation("rental_rate", "live", "publicnode")
	o.Scheduled = UTC(now)
	o.Caller = c.State.CallersValid[0]
	o.Requests = []Request{headRequest(false, "rental_state_before", false), Post("publicnode", "/wallet/triggerconstantcontract", "rental_rate", "", map[string]any{"owner_address": Hex(o.Caller), "contract_address": ContractHex, "function_selector": "_rentalRate(uint256,uint256)", "parameter": strings.Repeat("0", 64) + strings.Repeat("0", 63) + "1", "call_value": 0}, false), headRequest(false, "rental_state_after", false)}
	for i := range o.Requests {
		o.Requests[i].Research = true
	}
	w.NextRate = UTC(now.Add(5 * time.Minute))
	c.State.Ops = append(c.State.Ops, o)
	c.scheduler().InFlightCapture = o.Batch.Capture.CaptureId.String()
}
func (c *Collector) processRentalRate(o *Operation, ev Evidence, body []byte) error {
	p, s, ready, err := parseConstantExecution(body, ev, Candidate{}, o.Caller, o.Scheduled, c)
	r := RentalRateObservation{CaptureId: o.Batch.Capture.CaptureId, CaptureStartedAt: o.Batch.Capture.CaptureStartedAt, ObservationIndex: uint32(len(o.Batch.Rates)), ContractAddress: p.ContractAddress, ResourceType: 1, RequestStartedAt: p.RequestStartedAt, ResponseReceivedAt: p.ResponseReceivedAt, AvailableAt: p.AvailableAt, ImplementationAddress: p.ImplementationAddress, ImplementationCodeHash: p.ImplementationCodeHash, ImplementationCheckedAt: p.ImplementationCheckedAt, IdentityStatus: p.IdentityStatus, Status: "unknown", ApiSuccess: p.ApiSuccess, TvmResult: p.TvmResult, Scale: 1000000000000000000, StateBinding: "node_latest_unpinned", ErrorCode: p.ErrorCode, RequestPayloadHash: p.RequestPayloadHash, ResponsePayloadHash: p.ResponsePayloadHash}
	if err == nil && ready && !probeIdentityVerified(p) {
		r.ErrorCode = "implementation_identity_unverified"
	}
	if err == nil && ready && probeIdentityVerified(p) {
		if len(s.Constant) == 1 && len(s.Constant[0]) == 64 {
			v, e := hex.DecodeString(s.Constant[0])
			if e == nil {
				r.Rate = new(big.Int).SetBytes(v)
				r.Status = "ok"
				r.TvmResult = "SUCCESS"
				r.ErrorCode = ""
			} else {
				r.ErrorCode = "invalid_rate_hex"
			}
		} else {
			r.ErrorCode = "invalid_rate_abi"
		}
	}
	if err != nil {
		r.ErrorCode = err.Error()
	}
	// A retried request replaces the observation; its individual attempts remain
	// in the operation evidence and durable limiter accounting.
	r.ObservationIndex = 0
	o.Batch.Rates = []RentalRateObservation{r}
	return err
}
func (c *Collector) finalizeRentalRates(o *Operation, now time.Time) {
	for i := range o.Batch.Rates {
		r := &o.Batch.Rates[i]
		r.AvailableAt = now
		if h := o.ResearchBefore; h != nil {
			r.HeadBeforeNumber = Ptr(h.Number)
			r.HeadBeforeHash = Ptr(h.Hash)
			r.HeadBeforeTime = Ptr(h.Time)
		}
		if h := o.ResearchAfter; h != nil {
			r.HeadAfterNumber = Ptr(h.Number)
			r.HeadAfterHash = Ptr(h.Hash)
			r.HeadAfterTime = Ptr(h.Time)
		}
		if r.Status == "ok" && (o.Failed || !validRentalRate(*r)) {
			r.Status = "unknown"
			r.ErrorCode = "missing_or_inconsistent_rate_evidence"
		}
	}
}
func (c *Collector) completeRentalRate(o *Operation) {
	s := c.scheduler()
	if s.InFlightCapture == o.Batch.Capture.CaptureId.String() {
		s.InFlightCapture = ""
	}
	s.NextCheck = UTC(c.Now().Add(30 * time.Second))
	for _, r := range o.Batch.Rates {
		if !o.Failed && r.Status == "ok" && validRentalRate(r) {
			row := r
			c.State.Dynamic.Rate = &row
		}
	}
}

func (c *Collector) errorRentalRate(o *Operation, ev Evidence) {
	_ = c.processRentalRate(o, ev, []byte(`{"result":{"result":false}}`))
	for i := range o.Batch.Rates {
		r := &o.Batch.Rates[i]
		r.ApiSuccess = nil
		r.Status = "rpc_error"
		if ev.Error == "timeout" {
			r.Status = "timeout"
		}
		r.ErrorCode = ev.Error
	}
}
