package justlendkeeper

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SchedulerState is checkpointed separately from the cold candidate payload.
// The cursor is the queue: a second active queue would duplicate origins and
// prevent long-lived rentals from yielding their slots to unseen candidates.
type SchedulerState struct {
	Version           uint32
	NextCheck         time.Time
	Phase             uint8
	ResearchCursor    uint32
	OpportunityCursor string
	FairPhase         uint8
	Ledger            map[string]ResearchEntry
	InFlightCapture   string
}

// Entries contain only scheduling/proof metadata, never raw events. Exhausted
// attempts remain while their generation is in the pool; LRU eviction must not
// silently grant an unchanged origin three more verification attempts.
type ResearchEntry struct {
	Renter, Receiver             string
	OriginNumber                 uint64
	Verified, Closed, Superseded bool
	Attempts                     uint8
	Revision                     string
	NextSeed, NextCheck          time.Time
	Liquidatable                 bool
	LastNumber                   uint64
	LastTx, LastLog              uint32
}

type researchDecision struct {
	Candidate                Candidate
	Generation, Lane, Action string
	ResearchCursor           uint32
	Fair                     bool
}

func researchGeneration(can Candidate) string {
	return Hex(Hash([]byte(fmt.Sprintf("energy-research-v1/%s/%s/%d/%s/%d/%s", Hex(can.Renter), Hex(can.Receiver), can.Origin.BlockNumber, strings.ToLower(can.Origin.Transaction), can.Origin.Index, can.Origin.Name))))
}
func sameResearchPair(a, b Candidate) bool { return a.Renter == b.Renter && a.Receiver == b.Receiver }
func newerResearchCandidate(a, b Candidate) bool {
	if !a.LastActivity.Equal(b.LastActivity) {
		return a.LastActivity.After(b.LastActivity)
	}
	if a.Origin.BlockNumber != b.Origin.BlockNumber {
		return a.Origin.BlockNumber > b.Origin.BlockNumber
	}
	if a.LastNumber != b.LastNumber {
		return a.LastNumber > b.LastNumber
	}
	if a.LastTx != b.LastTx {
		return a.LastTx > b.LastTx
	}
	if a.LastLog != b.LastLog {
		return a.LastLog > b.LastLog
	}
	return a.Verified && !b.Verified
}
func (c *Collector) currentResearchCandidate(can Candidate) (Candidate, bool) {
	if c.DynamicSelection && c.State.Dynamic != nil {
		o, ok := c.State.Dynamic.getOrder(dynamicPair(can))
		return o.Candidate, ok
	}
	var latest Candidate
	found := false
	for _, pool := range [][]Candidate{c.State.Candidates, c.State.Cohort} {
		for _, member := range pool {
			if sameResearchPair(member, can) && (!found || newerResearchCandidate(member, latest)) {
				latest, found = member, true
			}
		}
	}
	return latest, found
}
func researchCaller(c *Collector, can Candidate) string {
	for _, caller := range c.State.CallersValid {
		if len(caller) == 21 && caller != can.Renter && caller != can.Receiver {
			return caller
		}
	}
	return ""
}
func eligibleResearchOrigin(can Candidate) bool {
	if can.Closed || can.Ambiguous || len(can.Renter) != 21 || len(can.Receiver) != 21 {
		return false
	}
	row, err := EventRow(can.Origin)
	return err == nil && row.Renter == can.Renter && row.Receiver == can.Receiver && (row.EventKind == "rent" || (row.EventKind == "return" && row.AmountSun.Sign() > 0))
}
func (c *Collector) scheduler() *SchedulerState {
	if c.State.Scheduler == nil {
		c.State.Scheduler = &SchedulerState{Version: 1, Ledger: map[string]ResearchEntry{}}
	}
	if c.State.Scheduler.Ledger == nil {
		c.State.Scheduler.Ledger = map[string]ResearchEntry{}
	}
	return c.State.Scheduler
}
func (c *Collector) researchEntry(can Candidate) ResearchEntry {
	key := researchGeneration(can)
	if e, ok := c.State.Scheduler.Ledger[key]; ok {
		if e.Revision != seedRevision {
			e.Attempts = 0
			e.NextSeed = time.Time{}
			e.Revision = seedRevision
		}
		return e
	}
	e := ResearchEntry{Renter: can.Renter, Receiver: can.Receiver, OriginNumber: can.Origin.BlockNumber, Revision: seedRevision}
	// Preserve existing research retry caps when the same origin also appears in
	// the discovery cache; a new lane is not a new retry allowance.
	for _, member := range c.State.Cohort {
		if researchGeneration(member) == key && member.SeedRevision == seedRevision {
			e.Attempts = member.SeedAttempts
			e.NextSeed = member.SeedNext
		}
	}
	return e
}
func (c *Collector) pruneResearchLedger() {
	keep := map[string]bool{}
	for _, pool := range [][]Candidate{c.State.Candidates, c.State.Cohort} {
		for _, can := range pool {
			keep[researchGeneration(can)] = true
		}
	}
	for _, o := range c.State.Ops {
		if o.ResearchGeneration != "" {
			keep[o.ResearchGeneration] = true
		}
	}
	for key := range c.State.Scheduler.Ledger {
		if !keep[key] {
			delete(c.State.Scheduler.Ledger, key)
		}
	}
}
func (c *Collector) selectResearchMember(now time.Time) (researchDecision, bool) {
	n := len(c.State.Cohort)
	for step := 0; step < n; step++ {
		index := (int(c.State.Scheduler.ResearchCursor)%n + step) % n
		can := c.State.Cohort[index]
		current, ok := c.currentResearchCandidate(can)
		if !ok || !can.Verified || !current.Verified || !eligibleResearchOrigin(current) || researchCaller(c, current) == "" {
			continue
		}
		// A cohort identifies the address pair. A verified partial return can
		// update its origin in the discovery cache without replacing the member.
		can = current
		e := c.researchEntry(can)
		if e.Closed || e.Superseded || now.Before(e.NextCheck) {
			continue
		}
		return researchDecision{Candidate: can, Generation: researchGeneration(can), Lane: "research", Action: "probe", ResearchCursor: uint32((index + 1) % n)}, true
	}
	return researchDecision{}, false
}
func (c *Collector) selectOpportunity(now time.Time) (researchDecision, bool) {
	// Each retained pool member is considered once per fair pass. Sorting a
	// temporary slice never changes the cold cache or its immutable reference.
	candidates := map[string]Candidate{}
	latest := map[string]Candidate{}
	for _, pool := range [][]Candidate{c.State.Candidates, c.State.Cohort} {
		for _, can := range pool {
			pair := can.Renter + can.Receiver
			old, exists := latest[pair]
			if !exists || newerResearchCandidate(can, old) {
				latest[pair] = can
			}
		}
	}
	for _, can := range c.State.Candidates {
		current := latest[can.Renter+can.Receiver]
		candidates[researchGeneration(current)] = current
	}
	keys := make([]string, 0, len(candidates))
	for key := range candidates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	eligible := func(key string) (researchDecision, bool) {
		can := candidates[key]
		e := c.researchEntry(can)
		if e.Closed || e.Superseded || now.Before(e.NextCheck) {
			return researchDecision{}, false
		}
		action := "rental_state"
		if !e.Verified {
			if e.Attempts >= 3 || now.Before(e.NextSeed) {
				return researchDecision{}, false
			}
			action = "seed"
		}
		if !eligibleResearchOrigin(can) || researchCaller(c, can) == "" {
			return researchDecision{}, false
		}
		return researchDecision{Candidate: can, Generation: key, Lane: "opportunity", Action: action}, true
	}
	if c.State.Scheduler.FairPhase%4 != 3 {
		var best researchDecision
		var bestAt time.Time
		found := false
		for _, key := range keys {
			e, exists := c.State.Scheduler.Ledger[key]
			if !exists || !e.Verified || !e.Liquidatable {
				continue
			}
			d, ok := eligible(key)
			if ok && e.Verified && e.Liquidatable && (!found || e.NextCheck.Before(bestAt)) {
				best, bestAt, found = d, e.NextCheck, true
			}
		}
		if found {
			return best, true
		}
	}
	start := sort.SearchStrings(keys, c.State.Scheduler.OpportunityCursor)
	for start < len(keys) && keys[start] <= c.State.Scheduler.OpportunityCursor {
		start++
	}
	for step := 0; step < len(keys); step++ {
		key := keys[(start+step)%len(keys)]
		if d, ok := eligible(key); ok {
			d.Fair = true
			return d, true
		}
	}
	return researchDecision{}, false
}

func (c *Collector) scheduleResearch(now time.Time) {
	if c.DynamicSelection {
		c.scheduleDynamic(now)
		return
	}
	if !c.Opportunities || !c.State.BootstrapReady || c.Active("seed") || c.Active("probe") || c.Active("rental_state") || !c.lifecycleCurrent(now) || now.Before(c.API.Limiter.Started.Add(5*time.Minute)) {
		return
	}
	s := c.scheduler()
	if s.Version != 1 || s.InFlightCapture != "" || now.Before(s.NextCheck) {
		return
	}
	c.pruneResearchLedger()
	var d researchDecision
	var ok bool
	if s.Phase%4 == 0 {
		d, ok = c.selectResearchMember(now)
		if !ok {
			d, ok = c.selectOpportunity(now)
		}
	} else {
		d, ok = c.selectOpportunity(now)
		if !ok {
			d, ok = c.selectResearchMember(now)
		}
	}
	if !ok {
		return
	}
	e := c.researchEntry(d.Candidate)
	switch d.Action {
	case "seed":
		o := c.NewOperation("seed", "catchup", "publicnode")
		o.Batch.Capture.CaptureKind = "events"
		o.Batch.Capture.CoverageScope = "sampled_rental_events"
		can := d.Candidate
		o.Candidate = &can
		o.RawEvents = []RawEvent{can.Origin}
		o.Scheduled = UTC(now)
		o.Requests = []Request{headRequest(true, "solid", true)}
		o.AddHydration()
		c.State.Ops = append(c.State.Ops, o)
		e.Attempts++
		e.NextSeed = UTC(now.Add(30 * time.Minute))
	case "probe":
		c.AddProbe(d.Candidate, now)
	case "rental_state":
		c.addRentalState(d.Candidate, d.Lane, now)
	}
	o := &c.State.Ops[len(c.State.Ops)-1]
	o.ResearchLane = d.Lane
	o.ResearchGeneration = d.Generation
	s.InFlightCapture = o.Batch.Capture.CaptureId.String()
	for i := range o.Requests {
		o.Requests[i].Research = true
	}
	s.Ledger[d.Generation] = e
	s.Phase = (s.Phase + 1) % 4
	if d.Lane == "research" {
		s.ResearchCursor = d.ResearchCursor
	} else {
		s.FairPhase = (s.FairPhase + 1) % 4
		if d.Fair {
			s.OpportunityCursor = d.Generation
		}
	}
}

func rentalParameter(can Candidate) string {
	return "000000000000000000000000" + Hex(can.Renter[1:]) + "000000000000000000000000" + Hex(can.Receiver[1:]) + "0000000000000000000000000000000000000000000000000000000000000001"
}
func (c *Collector) addRentalState(can Candidate, lane string, scheduled time.Time) {
	o := c.NewOperation("rental_state", "live", "publicnode")
	o.Candidate = &can
	o.Caller = researchCaller(c, can)
	o.Scheduled = UTC(scheduled)
	o.ResearchLane = lane
	o.ResearchGeneration = researchGeneration(can)
	o.Requests = []Request{
		headRequest(false, "rental_state_before", false),
		Post("publicnode", "/wallet/triggerconstantcontract", "rental_state", "", map[string]any{"owner_address": Hex(o.Caller), "contract_address": ContractHex, "function_selector": "getRentInfo(address,address,uint256)", "parameter": rentalParameter(can), "call_value": 0}, false),
		headRequest(false, "rental_state_after", false),
	}
	for i := range o.Requests {
		o.Requests[i].Research = true
	}
	c.State.Ops = append(c.State.Ops, o)
}

func (c *Collector) researchGenerationCurrent(o *Operation) bool {
	if o.Candidate == nil {
		return false
	}
	can, ok := c.currentResearchCandidate(*o.Candidate)
	if o.DynamicSlot != 0 && c.State.Dynamic != nil {
		current, exists := c.State.Dynamic.getOrder(dynamicPair(*o.Candidate))
		can, ok = current.Candidate, exists
	}
	if !ok || (!c.DynamicSelection && !eligibleResearchOrigin(can)) || c.DynamicSelection && can.Closed {
		return false
	}
	if (c.DynamicSelection || o.DynamicSlot != 0) && o.Kind != "seed" {
		current, exists := c.State.Dynamic.getOrder(dynamicPair(can))
		if !exists || current.Candidate.Ambiguous || current.ClosurePending {
			return false
		}
	}
	generation := o.ResearchGeneration
	if generation == "" {
		generation = researchGeneration(*o.Candidate)
	}
	if researchGeneration(can) != generation {
		return false
	}
	if c.State.Scheduler != nil {
		if e, ok := c.State.Scheduler.Ledger[generation]; ok && (e.Closed || e.Superseded) {
			return false
		}
	}
	return true
}

func verifiedResearchEvent(r RentalEvent) bool {
	return r.ResourceType == 1 && r.PositionStatus == "receipt_verified" && r.Finality == "solid" && r.TransactionIndex != nil && r.ReceiptLogIndex != nil
}
func eventMatchesResearchOrigin(r RentalEvent, can Candidate) bool {
	return strings.EqualFold(Hex(r.TxId), can.Origin.Transaction) && r.ProviderEventIndex == can.Origin.Index && r.BlockNumber == can.Origin.BlockNumber && r.Renter == can.Renter && r.Receiver == can.Receiver
}
func researchPositionAfter(r RentalEvent, e ResearchEntry) bool {
	return r.BlockNumber > e.LastNumber || (r.BlockNumber == e.LastNumber && (*r.TransactionIndex > e.LastTx || (*r.TransactionIndex == e.LastTx && *r.ReceiptLogIndex > e.LastLog)))
}
func (c *Collector) observeResearchEvents(events []RentalEvent) {
	if c.DynamicSelection {
		return
	}
	if !c.Opportunities || len(events) == 0 {
		return
	}
	s := c.scheduler()
	ordered := append([]RentalEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.BlockNumber != b.BlockNumber {
			return a.BlockNumber < b.BlockNumber
		}
		if a.TransactionIndex == nil || b.TransactionIndex == nil {
			return a.TransactionIndex == nil && b.TransactionIndex != nil
		}
		if *a.TransactionIndex != *b.TransactionIndex {
			return *a.TransactionIndex < *b.TransactionIndex
		}
		return a.ReceiptLogIndex != nil && b.ReceiptLogIndex != nil && *a.ReceiptLogIndex < *b.ReceiptLogIndex
	})
	for _, r := range ordered {
		if !verifiedResearchEvent(r) {
			continue
		}
		// Already authenticated enrichment can certify its exact current origin;
		// unrelated historic evidence never authenticates a replacement generation.
		for _, pool := range [][]Candidate{c.State.Candidates, c.State.Cohort} {
			for _, can := range pool {
				if eventMatchesResearchOrigin(r, can) {
					key := researchGeneration(can)
					e := c.researchEntry(can)
					if e.Superseded || (e.LastNumber != 0 && !researchPositionAfter(r, e)) {
						continue
					}
					e.Verified = true
					e.LastNumber = r.BlockNumber
					e.LastTx = *r.TransactionIndex
					e.LastLog = *r.ReceiptLogIndex
					e.Closed = r.EventKind == "liquidate" || (r.EventKind == "return" && r.AmountSun != nil && r.AmountSun.Sign() == 0)
					s.Ledger[key] = e
				}
			}
		}
		for key, e := range s.Ledger {
			if e.Renter != r.Renter || e.Receiver != r.Receiver || r.BlockNumber < e.OriginNumber || !researchPositionAfter(r, e) {
				continue
			}
			// Same-block events need the origin's verified transaction position first.
			if e.LastNumber == 0 && r.BlockNumber == e.OriginNumber {
				continue
			}
			e.LastNumber = r.BlockNumber
			e.LastTx = *r.TransactionIndex
			e.LastLog = *r.ReceiptLogIndex
			e.Liquidatable = false
			if r.EventKind == "liquidate" || (r.EventKind == "return" && r.AmountSun != nil && r.AmountSun.Sign() == 0) {
				e.Closed = true
			} else {
				e.Superseded = true
			}
			s.Ledger[key] = e
		}
	}
}

func (c *Collector) applyResearchSeed(o *Operation) {
	if o.Kind != "seed" || o.ResearchGeneration == "" || o.Candidate == nil || !c.researchGenerationCurrent(o) {
		return
	}
	s := c.scheduler()
	e, ok := s.Ledger[o.ResearchGeneration]
	if !ok {
		return
	}
	if !o.Failed {
		for _, r := range o.Batch.Events {
			if verifiedResearchEvent(r) && eventMatchesResearchOrigin(r, *o.Candidate) {
				if e.Superseded || (e.LastNumber != 0 && !researchPositionAfter(r, e)) {
					continue
				}
				e.Verified = true
				e.LastNumber = r.BlockNumber
				e.LastTx = *r.TransactionIndex
				e.LastLog = *r.ReceiptLogIndex
				e.Closed = r.EventKind == "return" && r.AmountSun != nil && r.AmountSun.Sign() == 0
			}
		}
	}
	e.NextSeed = UTC(c.Now().Add(30 * time.Minute))
	s.Ledger[o.ResearchGeneration] = e
}
func (c *Collector) completeResearch(o *Operation) {
	if c.State.Dynamic != nil && (o.DynamicSlot != 0 || o.Kind == "rental_rate") {
		c.completeDynamic(o)
		return
	}
	if (!c.Opportunities && c.State.Scheduler == nil) || (o.Kind != "seed" && o.Kind != "probe" && o.Kind != "rental_state") {
		return
	}
	s := c.scheduler()
	if s.InFlightCapture != "" && s.InFlightCapture != o.Batch.Capture.CaptureId.String() {
		return
	}
	if s.InFlightCapture == o.Batch.Capture.CaptureId.String() {
		s.InFlightCapture = ""
	}
	next := UTC(c.Now().Add(30 * time.Second))
	if s.NextCheck.Before(next) {
		s.NextCheck = next
	}
	c.applyResearchSeed(o)
	if o.ResearchGeneration == "" || !c.researchGenerationCurrent(o) {
		return
	}
	e, ok := s.Ledger[o.ResearchGeneration]
	if !ok {
		return
	}
	e.NextCheck = next
	if o.Kind == "rental_state" {
		e.Liquidatable = false
		for _, r := range o.Batch.RentalStates {
			if !o.Failed && r.Status == "ok" && r.IdentityStatus == "verified_manifest" && r.Liquidatable != nil && r.Renter == o.Candidate.Renter && r.Receiver == o.Candidate.Receiver {
				e.Liquidatable = *r.Liquidatable
			}
		}
	}
	s.Ledger[o.ResearchGeneration] = e
}
