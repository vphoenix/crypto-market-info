package justlendkeeper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// A stalled Liquidate feed must prevent opportunity work even when rent/return
// are fresh: otherwise closed leases can remain eligible indefinitely.
func TestOpportunityFreshnessRequiresLiquidations(t *testing.T) {
	c, _, now := testCollector(t)
	c.Opportunities = true
	c.State.EventCursors["RentResource"] = now.Add(-2 * time.Minute)
	c.State.EventCursors["ReturnResource"] = now.Add(-2 * time.Minute)
	c.State.EventCursors["Liquidate"] = now.Add(-6 * time.Minute)
	if c.lifecycleCurrent(*now) {
		t.Fatal("opportunity admission ignored stalled liquidation feed")
	}
}

func opportunityCollector(t *testing.T) (*Collector, *time.Time) {
	c, _, now := testCollector(t)
	c.Opportunities = true
	c.State.BootstrapReady = true
	c.State.IdentityStatus = "verified_manifest"
	c.State.CallersValid = []string{"A" + "01234567890123456789"}
	c.API.Limiter.Started = now.Add(-time.Hour)
	opportunityFresh(c, *now)
	return c, now
}
func opportunityFresh(c *Collector, now time.Time) {
	for _, kind := range []string{"RentResource", "ReturnResource", "Liquidate"} {
		c.State.EventCursors[kind] = now.Add(-2 * time.Minute)
	}
}
func opportunityCandidate(i int, now time.Time, verified bool) Candidate {
	address := "A" + fmt.Sprintf("%020d", i)
	raw := RawEvent{BlockNumber: uint64(100 + i), BlockTimestamp: now.Add(-time.Hour).UnixMilli(), Name: "RentResource", Transaction: fmt.Sprintf("%064x", i+1), Index: 3, Result: map[string]json.RawMessage{
		"renter": json.RawMessage(fmt.Sprintf("%q", Hex(address))), "receiver": json.RawMessage(fmt.Sprintf("%q", Hex(address))), "resourceType": json.RawMessage("1"), "amount": json.RawMessage("100"), "addedAmount": json.RawMessage("100"), "addedSecurityDeposit": json.RawMessage("20"),
	}}
	return Candidate{Renter: address, Receiver: address, Origin: raw, LastActivity: now.Add(-time.Hour), Verified: verified}
}
func finishOpportunity(c *Collector, now *time.Time, failed bool) Operation {
	o := c.State.Ops[0]
	o.Failed = failed
	o.Requests = nil
	o.Batch.Capture.Committed = true
	c.completeResearch(&o)
	c.State.Ops = nil
	*now = now.Add(31 * time.Second)
	opportunityFresh(c, *now)
	return o
}

func TestOpportunitySkipsExhaustedCohortWithoutEmptyCapture(t *testing.T) {
	c, now := opportunityCollector(t)
	for i := 0; i < 16; i++ {
		can := opportunityCandidate(i, *now, false)
		can.SeedAttempts = 3
		can.SeedRevision = seedRevision
		c.State.Cohort = append(c.State.Cohort, can)
	}
	original := append([]Candidate(nil), c.State.Cohort...)
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 0 || !reflect.DeepEqual(original, c.State.Cohort) {
		t.Fatal("invalid cohort generated work or changed its audit")
	}
	c.State.Cohort = append(c.State.Cohort, opportunityCandidate(17, *now, true))
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Candidate.Renter != c.State.Cohort[16].Renter || c.State.Ops[0].ResearchLane != "research" {
		t.Fatal("valid research member blocked behind invalid slots")
	}
	for _, r := range c.State.Ops[0].Requests {
		if !r.Research {
			t.Fatal("research request bypasses shared gate")
		}
	}
}

func TestOpportunityFairRotationVisitsThreeHundredCandidates(t *testing.T) {
	c, now := opportunityCollector(t)
	for i := 0; i < 300; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
	}
	cold := append([]Candidate(nil), c.State.Candidates...)
	seen := map[string]bool{}
	for i := 0; i < 300; i++ {
		c.scheduleResearch(*now)
		if len(c.State.Ops) != 1 {
			t.Fatalf("no admission at round %d", i)
		}
		o := c.State.Ops[0]
		if o.Kind != "seed" || len(o.Requests) != 4 {
			t.Fatal("unverified candidate bypassed four-request seed", o.Kind, len(o.Requests))
		}
		seen[o.Candidate.Renter] = true
		finishOpportunity(c, now, true)
	}
	if len(seen) != 300 {
		t.Fatalf("fair cursor visited %d of 300 candidates", len(seen))
	}
	if !reflect.DeepEqual(cold, c.State.Candidates) {
		t.Fatal("scheduler rewrote cold candidates")
	}
	if len(c.State.Scheduler.Ledger) > 300 {
		t.Fatal("ledger grew beyond candidate generations")
	}
}

func TestOpportunitySeedRetriesRemainBoundedAcrossRestart(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Candidates = []Candidate{opportunityCandidate(1, *now, false)}
	for attempt := 0; attempt < 3; attempt++ {
		c.scheduleResearch(*now)
		if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "seed" {
			t.Fatalf("missing retry %d", attempt)
		}
		finishOpportunity(c, now, true)
		*now = now.Add(31 * time.Minute)
		opportunityFresh(c, *now)
	}
	raw, err := runtimecheckpoint.MarshalTyped(*c.State.Scheduler)
	if err != nil {
		t.Fatal(err)
	}
	var restored SchedulerState
	if err = runtimecheckpoint.UnmarshalTyped(raw, &restored); err != nil {
		t.Fatal(err)
	}
	c.State.Scheduler = &restored
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 0 {
		t.Fatal("restart reset exhausted retry allowance")
	}
	c.State.Candidates[0].Origin.Transaction = fmt.Sprintf("%064x", 5000)
	c.State.Candidates[0].Origin.BlockNumber += 1000
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "seed" {
		t.Fatal("new origin inherited exhausted old retries")
	}
}

func TestOpportunityLateSeedCannotVerifyNewGeneration(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Candidates = []Candidate{opportunityCandidate(1, *now, false)}
	c.scheduleResearch(*now)
	old := c.State.Ops[0]
	row, err := EventRow(old.Candidate.Origin)
	if err != nil {
		t.Fatal(err)
	}
	row.PositionStatus = "receipt_verified"
	row.TransactionIndex = Ptr(uint32(1))
	row.ReceiptLogIndex = Ptr(uint32(3))
	old.Batch.Events = []RentalEvent{row}
	c.State.Candidates[0].Origin.Transaction = fmt.Sprintf("%064x", 5001)
	c.State.Candidates[0].Origin.BlockNumber += 1000
	if c.researchGenerationCurrent(&old) {
		t.Fatal("old seed still matches replaced generation")
	}
	old.Batch.Capture.Committed = true
	c.completeResearch(&old)
	c.State.Ops = nil
	*now = now.Add(31 * time.Second)
	opportunityFresh(c, *now)
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "seed" {
		t.Fatal("late seed authenticated the new origin")
	}
}

func TestOpportunityCompletedChainEnforcesPauseAndOneInflight(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Candidates = []Candidate{opportunityCandidate(1, *now, false)}
	c.scheduleResearch(*now)
	c.scheduleResearch(now.Add(time.Hour))
	if len(c.State.Ops) != 1 {
		t.Fatal("parallel research chain admitted")
	}
	o := c.State.Ops[0]
	o.Failed = true
	o.Batch.Capture.Committed = true
	c.completeResearch(&o)
	c.State.Ops = nil
	c.State.Candidates = append(c.State.Candidates, opportunityCandidate(2, *now, false))
	c.scheduleResearch(now.Add(29 * time.Second))
	if len(c.State.Ops) != 0 {
		t.Fatal("new chain bypassed completion pause")
	}
	c.scheduleResearch(now.Add(30 * time.Second))
	if len(c.State.Ops) != 1 {
		t.Fatal("eligible chain not admitted at pause boundary")
	}
}

func verifiedOpportunityRow(t *testing.T, can Candidate) RentalEvent {
	t.Helper()
	r, err := EventRow(can.Origin)
	if err != nil {
		t.Fatal(err)
	}
	r.PositionStatus = "receipt_verified"
	r.TransactionIndex = Ptr(uint32(1))
	r.ReceiptLogIndex = Ptr(uint32(3))
	return r
}

func TestOpportunityClosedGenerationIsNotReopenedByLateSeed(t *testing.T) {
	c, now := opportunityCollector(t)
	can := opportunityCandidate(1, *now, false)
	c.State.Candidates = []Candidate{can}
	c.scheduleResearch(*now)
	rent := verifiedOpportunityRow(t, can)
	c.observeResearchEvents([]RentalEvent{rent})
	closed := rent
	closed.EventKind = "liquidate"
	closed.BlockNumber++
	closed.TxId = "x" + closed.TxId[1:]
	c.observeResearchEvents([]RentalEvent{closed})
	if c.researchGenerationCurrent(&c.State.Ops[0]) {
		t.Fatal("known liquidation did not invalidate pending origin")
	}
	c.observeResearchEvents([]RentalEvent{rent})
	if c.researchGenerationCurrent(&c.State.Ops[0]) {
		t.Fatal("late older rent reopened a closed generation")
	}
	can.Origin.Transaction = fmt.Sprintf("%064x", 9000)
	can.Origin.BlockNumber = closed.BlockNumber + 1
	c.State.Candidates[0] = can
	oldOperation := c.State.Ops[0]
	oldOperation.Failed = true
	c.completeResearch(&oldOperation)
	c.State.Ops = nil
	*now = now.Add(31 * time.Second)
	opportunityFresh(c, *now)
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "seed" {
		t.Fatal("new rent could not reopen the same address pair")
	}
}

func TestOpportunityResearchAdmissionQuotaIncludesSeed(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Cohort = []Candidate{opportunityCandidate(99, *now, true)}
	for i := 0; i < 8; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
	}
	lanes := []string{}
	for i := 0; i < 8; i++ {
		c.scheduleResearch(*now)
		if len(c.State.Ops) != 1 {
			t.Fatal("eligible lanes stopped")
		}
		lanes = append(lanes, c.State.Ops[0].ResearchLane)
		finishOpportunity(c, now, true)
	}
	want := []string{"research", "opportunity", "opportunity", "opportunity", "research", "opportunity", "opportunity", "opportunity"}
	if !reflect.DeepEqual(lanes, want) {
		t.Fatal("seed bypassed 1:3 admission budget", lanes)
	}
}

func TestOpportunityPriorityCannotStarveFairPass(t *testing.T) {
	c, now := opportunityCollector(t)
	for i := 0; i < 20; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, *now, false))
	}
	c.observeResearchEvents([]RentalEvent{verifiedOpportunityRow(t, c.State.Candidates[0])})
	key := researchGeneration(c.State.Candidates[0])
	entry := c.State.Scheduler.Ledger[key]
	entry.Liquidatable = true
	c.State.Scheduler.Ledger[key] = entry
	seen := map[string]bool{}
	for i := 0; i < 80; i++ {
		c.scheduleResearch(*now)
		if len(c.State.Ops) != 1 {
			t.Fatalf("lost opportunity slot %d", i)
		}
		seen[c.State.Ops[0].Candidate.Renter] = true
		o := finishOpportunity(c, now, true)
		// A continuously eligible priority candidate models a busy market; the
		// fair quota must still rotate every other retained pool member through.
		if o.ResearchGeneration == key {
			entry = c.State.Scheduler.Ledger[key]
			entry.Liquidatable = true
			c.State.Scheduler.Ledger[key] = entry
		}
	}
	if len(seen) != 20 {
		t.Fatalf("priority candidate starved %d retained candidates", 20-len(seen))
	}
}

func TestOpportunityResearchMemberFollowsVerifiedPartialReturn(t *testing.T) {
	c, now := opportunityCollector(t)
	rent := opportunityCandidate(1, *now, true)
	returned := rent
	returned.Origin = RawEvent{BlockNumber: rent.Origin.BlockNumber + 1, BlockTimestamp: now.Add(-time.Minute).UnixMilli(), Name: "ReturnResource", Transaction: fmt.Sprintf("%064x", 8800), Index: 3, Result: map[string]json.RawMessage{
		"renter": json.RawMessage(fmt.Sprintf("%q", Hex(rent.Renter))), "receiver": json.RawMessage(fmt.Sprintf("%q", Hex(rent.Receiver))), "resourceType": json.RawMessage("1"), "amount": json.RawMessage("50"), "subedAmount": json.RawMessage("50"), "subedSecurityDeposit": json.RawMessage("10"), "usageRental": json.RawMessage("1"),
	}}
	returned.LastActivity = now.Add(-time.Minute)
	c.State.Cohort = []Candidate{rent}
	c.State.Candidates = []Candidate{returned}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].ResearchLane != "research" || c.State.Ops[0].Candidate.Origin.Transaction != returned.Origin.Transaction {
		t.Fatal("verified partial return removed an otherwise valid research member")
	}
}

func TestOpportunitySameBlockCloseCannotCloseLaterRent(t *testing.T) {
	c, now := opportunityCollector(t)
	can := opportunityCandidate(1, *now, false)
	c.State.Candidates = []Candidate{can}
	rent := verifiedOpportunityRow(t, can)
	rent.TransactionIndex = Ptr(uint32(2))
	closed := rent
	closed.EventKind = "liquidate"
	closed.TransactionIndex = Ptr(uint32(1))
	closed.TxId = "z" + closed.TxId[1:]
	// Input order is intentionally reversed; transaction/log position wins.
	c.observeResearchEvents([]RentalEvent{rent, closed})
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "rental_state" {
		t.Fatal("earlier same-block close suppressed later verified rent")
	}
	closed.TransactionIndex = Ptr(uint32(3))
	c.observeResearchEvents([]RentalEvent{closed})
	if c.researchGenerationCurrent(&c.State.Ops[0]) {
		t.Fatal("later same-block close left current generation eligible")
	}
}

func TestOpportunityLegacySeedPathsCannotBypassSingleChain(t *testing.T) {
	c, now := opportunityCollector(t)
	old := opportunityCandidate(1, *now, true)
	old.Closed = true
	c.State.Cohort = []Candidate{old}
	c.State.Candidates = []Candidate{opportunityCandidate(2, *now, false)}
	c.AddSeed(c.State.Candidates[0])
	c.RetrySeed()
	c.RefillCohort()
	if len(c.State.Ops) != 0 || !reflect.DeepEqual(c.State.Cohort, []Candidate{old}) {
		t.Fatal("legacy seed/refill bypassed shared scheduler or changed fixed cohort")
	}
	c.State.Cohort = nil
	c.SelectCohort()
	if len(c.State.Cohort) != 1 || len(c.State.Ops) != 0 {
		t.Fatal("bootstrap selection fan-out bypassed shared scheduler")
	}
}

func BenchmarkOpportunitySelectPool1500(b *testing.B) {
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	c := &Collector{State: &State{Scheduler: &SchedulerState{Version: 1, Ledger: map[string]ResearchEntry{}}, CallersValid: []string{"A01234567890123456789"}}}
	for i := 0; i < 1500; i++ {
		c.State.Candidates = append(c.State.Candidates, opportunityCandidate(i, now, false))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.selectOpportunity(now); !ok {
			b.Fatal("no eligible candidate")
		}
	}
}

func TestOpportunityAdmissionOwnsChainUntilMatchingCompletion(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Candidates = []Candidate{opportunityCandidate(1, *now, false)}
	c.scheduleResearch(*now)
	o := c.State.Ops[0]
	if c.State.Scheduler.InFlightCapture != o.Batch.Capture.CaptureId.String() {
		t.Fatal("admission did not persist chain ownership")
	}
	other := c.NewOperation("seed", "catchup", "publicnode")
	c.completeResearch(&other)
	if c.State.Scheduler.InFlightCapture != o.Batch.Capture.CaptureId.String() {
		t.Fatal("unrelated completion released live chain")
	}
	c.completeResearch(&o)
	if c.State.Scheduler.InFlightCapture != "" {
		t.Fatal("matching completion retained chain ownership")
	}
}

func TestOpportunityDisabledAdmissionStillCompletesRestoredChain(t *testing.T) {
	c, now := opportunityCollector(t)
	c.State.Candidates = []Candidate{opportunityCandidate(1, *now, false)}
	c.scheduleResearch(*now)
	o := c.State.Ops[0]
	c.Opportunities = false
	c.completeResearch(&o)
	if c.State.Scheduler.InFlightCapture != "" {
		t.Fatal("disabling new admission stranded restored chain owner")
	}
}
