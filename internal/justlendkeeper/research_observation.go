package justlendkeeper

import "time"

func (c *Collector) processRentalState(o *Operation, ev Evidence, body []byte) error {
	r, err := ParseRentalState(body, ev, *o.Candidate, o.Caller, o.Scheduled, c)
	r.CaptureId, r.CaptureStartedAt, r.ObservationIndex = o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt, uint32(len(o.Batch.RentalStates))
	if err != nil {
		r.Status = "unknown"
		r.ErrorCode = err.Error()
	}
	o.Batch.RentalStates = append(o.Batch.RentalStates, r)
	if err == nil && r.Status == "ok" && r.Liquidatable != nil && *r.Liquidatable && c.researchGenerationCurrent(o) {
		probe := Post("publicnode", "/wallet/triggerconstantcontract", "probe", "", map[string]any{"owner_address": Hex(o.Caller), "contract_address": ContractHex, "function_selector": "liquidate(address,address,uint256)", "parameter": rentalParameter(*o.Candidate), "call_value": 0}, false)
		probe.Research = true
		o.Requests = append([]Request{probe}, o.Requests...)
	}
	return err
}

func (c *Collector) errorRentalState(o *Operation, ev Evidence) {
	r, _ := ParseRentalState([]byte(`{"result":{"result":false}}`), ev, *o.Candidate, o.Caller, o.Scheduled, c)
	r.ApiSuccess = nil
	r.Status = "rpc_error"
	r.ErrorCode = ev.Error
	if ev.Error == "timeout" {
		r.Status = "timeout"
	}
	r.CaptureId, r.CaptureStartedAt, r.ObservationIndex = o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt, uint32(len(o.Batch.RentalStates))
	o.Batch.RentalStates = append(o.Batch.RentalStates, r)
}

func researchTrailingHead(requests []Request) []Request {
	for _, r := range requests {
		if r.Role == "rental_state_after" || r.Role == "probe_after" {
			return []Request{r}
		}
	}
	return nil
}

func (c *Collector) finalizeRentalStates(o *Operation, completed time.Time) {
	for i := range o.Batch.RentalStates {
		r := &o.Batch.RentalStates[i]
		r.AvailableAt = completed
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
		if r.Status != "ok" {
			continue
		}
		before, after := o.ResearchBefore, o.ResearchAfter
		if before == nil || after == nil || after.Number < before.Number || after.Time.Before(before.Time) || (after.Number == before.Number && (after.Hash != before.Hash || !after.Time.Equal(before.Time))) {
			r.Status = "unknown"
			r.ErrorCode = "missing_or_inconsistent_head_range"
		}
	}
}

// Pin one request chain even when a pre-upgrade checkpoint contains several
// legacy operations. Frozen SQL retries are resolved separately and send no RPC.
func (c *Collector) researchTurn(o *Operation, now time.Time) bool {
	if !c.Opportunities || (o.Kind != "seed" && o.Kind != "probe" && o.Kind != "rental_state" && o.Kind != "rental_rate") {
		return true
	}
	s := c.scheduler()
	if s.InFlightCapture != "" {
		exists := false
		for _, pending := range c.State.Ops {
			if pending.Batch.Capture.CaptureId.String() == s.InFlightCapture && !pending.RuntimeApplied {
				exists = true
				break
			}
		}
		if !exists {
			s.InFlightCapture = ""
		}
	}
	if s.InFlightCapture == "" {
		if now.Before(s.NextCheck) {
			return false
		}
		s.InFlightCapture = o.Batch.Capture.CaptureId.String()
	}
	return s.InFlightCapture == o.Batch.Capture.CaptureId.String()
}
