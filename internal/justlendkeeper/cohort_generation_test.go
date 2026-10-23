package justlendkeeper

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestReviewCohortOnlyPartialReturnRemainsSchedulable(t *testing.T) {
	c, now := opportunityCollector(t)
	can := opportunityCandidate(99, *now, true)
	can.Lifecycle = "original-rental"
	c.State.Cohort = []Candidate{can}
	c.State.Candidates = nil // bounded discovery cache has evicted this fixed member
	c.scheduleResearch(*now)
	finishOpportunity(c, now, true)
	returned := can
	returned.Origin = RawEvent{BlockNumber: can.Origin.BlockNumber + 1, BlockTimestamp: now.Add(-time.Minute).UnixMilli(), Name: "ReturnResource", Transaction: fmt.Sprintf("%064x", 8800), Index: 3, Result: map[string]json.RawMessage{
		"renter": json.RawMessage(fmt.Sprintf("%q", Hex(can.Renter))), "receiver": json.RawMessage(fmt.Sprintf("%q", Hex(can.Receiver))), "resourceType": json.RawMessage("1"), "amount": json.RawMessage("50"), "subedAmount": json.RawMessage("50"), "subedSecurityDeposit": json.RawMessage("10"), "usageRental": json.RawMessage("1"),
	}}
	row := verifiedOpportunityRow(t, returned)
	c.observeResearchEvents([]RentalEvent{row})
	c.ApplyEvents([]RentalEvent{row})
	if c.State.Cohort[0].Origin.Transaction != returned.Origin.Transaction || c.State.Cohort[0].Lifecycle != can.Lifecycle {
		t.Fatal("partial return lost origin or changed rental lifecycle")
	}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 {
		t.Fatal("authenticated open partial return permanently removed fixed research member from scheduling", c.State.Scheduler.Ledger)
	}
}
