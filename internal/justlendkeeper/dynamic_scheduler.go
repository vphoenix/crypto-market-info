package justlendkeeper

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

type pressure struct {
	numerator, denominator *big.Int
	amount, deposit, rate  *big.Int
	at                     time.Time
}
type dynamicChoice struct {
	order             dynamicOrder
	check             dynamicCheck
	key, lane, reason string
	class             uint8
	pressure          *pressure
}

func (c *Collector) pressureFor(o dynamicOrder, e dynamicCheck, now time.Time) *pressure {
	w := c.State.Dynamic
	if w.Rate == nil || w.Rate.Status != "ok" || !validRentalRate(*w.Rate) || w.Rate.Rate.Sign() <= 0 || now.Before(w.Rate.AvailableAt) || now.Sub(w.Rate.AvailableAt) > 5*time.Minute || w.Rate.ImplementationAddress == nil || *w.Rate.ImplementationAddress != c.State.Implementation || w.Rate.ImplementationCodeHash == nil || *w.Rate.ImplementationCodeHash != c.State.CodeHash {
		return nil
	}
	// Empty closed orders can return (0,index,false). This does not prove
	// closure, but also cannot support a zero-time risk prediction using an old
	// event amount. Keep them discoverable through exploration.
	if e.Deposit != nil && e.Deposit.Sign() == 0 && e.Liquidatable != nil && !*e.Liquidatable {
		return nil
	}
	a, d, at := o.Amount, o.Deposit, o.BaseAt
	if e.Deposit != nil && !e.ObservedAt.IsZero() {
		d = e.Deposit
		at = e.ObservedAt
	}
	if a == nil || a.Sign() <= 0 || d == nil || d.Sign() < 0 || at.IsZero() || now.Before(at) || now.Sub(at) > 24*time.Hour {
		return nil
	}
	rate := w.Rate.Rate
	den := new(big.Int).Mul(a, rate)
	num := new(big.Int).Mul(d, big.NewInt(1000000000000000000))
	charge := new(big.Int).Mul(den, big.NewInt(int64(now.Sub(at)/time.Second)))
	num.Sub(num, charge)
	if num.Sign() < 0 {
		num.SetInt64(0)
	}
	return &pressure{num, den, copyInt(a), copyInt(d), copyInt(rate), at}
}
func lessPressure(a, b *pressure) bool {
	return new(big.Int).Mul(a.numerator, b.denominator).Cmp(new(big.Int).Mul(b.numerator, a.denominator)) < 0
}
func equalPressure(a, b *pressure) bool {
	return new(big.Int).Mul(a.numerator, b.denominator).Cmp(new(big.Int).Mul(b.numerator, a.denominator)) == 0
}

func (c *Collector) chooseDynamic(lane string, now time.Time) (dynamicChoice, bool) {
	w := c.State.Dynamic
	var best dynamicChoice
	found := false
	selected := map[string]bool{}
	if w.Round != nil {
		for _, k := range w.Round.Selected {
			selected[k] = true
		}
	}
	w.each(func(key string, o dynamicOrder) bool {
		can := o.Candidate
		e := w.getCheck(key, o)
		if len(o.Conflicts) > 32 || selected[key] || can.Closed || now.Before(e.NextCheck) || !e.Verified && (e.Attempts >= 3 || now.Before(e.NextSeed)) || researchCaller(c, can) == "" {
			return true
		}
		row, err := EventRow(can.Origin)
		if err != nil || row.ResourceType != 1 {
			return true
		}
		ch := dynamicChoice{order: o, check: e, key: key, lane: lane, reason: "oldest_waiting", class: 3}
		if lane == "focus" {
			if can.Ambiguous || o.ClosurePending {
				return true
			}
			if e.LatestStatus == "ok" && e.Liquidatable != nil && *e.Liquidatable && !e.ObservedAt.IsZero() && !now.Before(e.ObservedAt) && now.Sub(e.ObservedAt) <= 5*time.Minute && e.Verified {
				ch.class = 0
				ch.reason = "fresh_liquidatable"
			} else {
				ch.pressure = c.pressureFor(o, e, now)
				if ch.pressure == nil {
					return true
				}
				ch.class = 2
				ch.reason = "pressure_indexed"
				if e.Verified {
					ch.class = 1
					ch.reason = "pressure_verified"
				}
			}
		}
		less := !found
		if found {
			if lane == "explore" {
				less = e.FairSince.Before(best.check.FairSince) || e.FairSince.Equal(best.check.FairSince) && key < best.key
			} else if ch.class != best.class {
				less = ch.class < best.class
			} else if ch.pressure != nil && best.pressure != nil && !equalPressure(ch.pressure, best.pressure) {
				less = lessPressure(ch.pressure, best.pressure)
			} else {
				less = e.LastAttempt.Before(best.check.LastAttempt) || e.LastAttempt.Equal(best.check.LastAttempt) && key < best.key
			}
		}
		if less {
			best = ch
			found = true
		}
		return true
	})
	return best, found && w.err == nil
}

func SelectionInputDigest(r SelectionMember) string {
	r.CaptureId = uuid.Nil
	r.CaptureStartedAt = time.Time{}
	r.InputDigest = ""
	r.Outcome = ""
	r.SeedCaptureId = nil
	r.StateResultCaptureId = nil
	r.RPCAttempts = 0
	b, err := FactBytes(r)
	if err != nil {
		panic(err)
	}
	return Hash(b)
}
func (c *Collector) scheduleDynamic(now time.Time) {
	w := c.State.Dynamic
	if w == nil || !w.Ready || w.err != nil || !c.Opportunities {
		return
	}
	s := c.scheduler()
	if s.InFlightCapture != "" || c.Active("seed") || c.Active("probe") || c.Active("rental_state") || c.Active("rental_rate") || c.Active("selection_round") {
		return
	}
	if now.Before(s.NextCheck) || now.Before(w.NextRound) || now.Before(c.API.Limiter.Started.Add(5*time.Minute)) {
		return
	}
	if w.Round != nil && w.Round.Active == nil && (w.Round.Slots >= 50 || now.Sub(w.Round.Started) >= 2*time.Hour || w.Round.Closing != "") {
		reason := w.Round.Closing
		if reason == "" {
			reason = "round_limit"
			if w.Round.Slots < 50 {
				reason = "round_timeout"
			}
		}
		c.closeDynamicRound(reason)
		return
	}
	coverage := c.dynamicCoverage()
	if coverage.IsZero() || now.Sub(coverage) > 5*time.Minute || now.Before(coverage) || c.State.IdentityStatus != "verified_manifest" || len(c.State.CallersValid) == 0 || c.State.Sources["publicnode"].Blocked || c.State.Sources["trongrid"].Blocked {
		if w.Round != nil {
			if w.Round.Active != nil && w.Round.Continue {
				w.Round.Active.Outcome = "source_not_current"
				c.finishDynamicMember()
			}
			if w.Round.Active == nil {
				c.closeDynamicRound("source_not_current")
			}
		}
		return
	}
	if c.State.Day == now.UTC().Format("2006-01-02") && c.State.Used >= c.Config.DailyBudget {
		if w.Round != nil {
			if w.Round.Active != nil && w.Round.Continue {
				w.Round.Active.Outcome = "daily_budget"
				c.finishDynamicMember()
			}
			if w.Round.Active == nil {
				c.closeDynamicRound("daily_budget")
			}
		}
		return
	}
	if w.Round != nil && w.Round.Active != nil {
		if w.Round.Continue {
			c.continueDynamicSlot(now)
		}
		return
	}
	if !now.Before(w.NextRate) {
		c.addRentalRate(now)
		return
	}
	lane := "focus"
	slot := uint8(1)
	if w.Round != nil {
		slot = w.Round.Slots + 1
	}
	if slot%5 == 0 {
		lane = "explore"
	}
	choice, ok := c.chooseDynamic(lane, now)
	if !ok {
		other := "explore"
		if lane == "explore" {
			other = "focus"
		}
		choice, ok = c.chooseDynamic(other, now)
		if ok {
			choice.reason = lane + "_empty_" + choice.reason
		}
	}
	if !ok {
		if w.Round != nil {
			c.closeDynamicRound("no_eligible_orders")
		}
		w.NextRound = UTC(now.Add(30 * time.Second))
		return
	}
	if w.Round == nil {
		w.Round = &dynamicRound{ID: uuid.New(), Started: UTC(now)}
	}
	round := w.Round
	round.Slots = slot
	round.Selected = append(round.Selected, choice.key)
	can := choice.order.Candidate
	m := SelectionMember{RoundId: round.ID, SlotNo: slot, Renter: can.Renter, Receiver: can.Receiver, Generation: generationBytes(can), RequestedLane: lane, ActualLane: choice.lane, SelectedAt: UTC(now), Reason: choice.reason, EstimatorRevision: "deposit-pressure-v1", OriginEventIndex: can.Origin.Index, CoverageTime: coverage, StateCaptureId: choice.check.StateCapture}
	tx, _ := hex.DecodeString(can.Origin.Transaction)
	m.OriginTx = string(tx)
	if choice.pressure != nil {
		p := choice.pressure
		m.AmountSun = p.amount
		m.DepositSun = p.deposit
		m.Rate = p.rate
		m.BaseTime = Ptr(p.at)
	}
	if w.Rate != nil {
		m.RateCaptureId = Ptr(w.Rate.CaptureId)
	}
	m.InputDigest = SelectionInputDigest(m)
	round.Active = &m
	e := choice.check
	e.LastAttempt = UTC(now)
	e.FairSince = UTC(now)
	w.setCheck(choice.key, e)
	c.startDynamicOperation(choice.order, e, now)
	c.Log(fmt.Sprintf("keeper_dynamic_admit policy=%s round=%s slot=%d requested=%s actual=%s reason=%s population=%d", DynamicPolicy, round.ID, slot, lane, m.ActualLane, m.Reason, w.population()))
}
func (c *Collector) startDynamicOperation(o dynamicOrder, e dynamicCheck, now time.Time) {
	w := c.State.Dynamic
	can := o.Candidate
	var op Operation
	if !e.Verified || can.Ambiguous || o.ClosurePending {
		op = c.NewOperation("seed", "catchup", "publicnode")
		op.Batch.Capture.CaptureKind = "events"
		op.Batch.Capture.CoverageScope = "dynamic_selected_origin"
		op.Candidate = &can
		op.RawEvents = append([]RawEvent(nil), o.Conflicts...)
		if len(op.RawEvents) == 0 {
			op.RawEvents = []RawEvent{can.Origin}
		}
		op.Scheduled = UTC(now)
		op.Requests = []Request{headRequest(true, "solid", true)}
		op.AddHydration()
		e.Attempts++
		e.NextSeed = UTC(now.Add(30 * time.Minute))
		w.setCheck(dynamicPair(can), e)
		c.State.Ops = append(c.State.Ops, op)
	} else {
		c.addRentalState(can, w.Round.Active.ActualLane, now)
	}
	pending := &c.State.Ops[len(c.State.Ops)-1]
	pending.DynamicSlot = w.Round.Active.SlotNo
	pending.DynamicRound = w.Round.ID
	pending.ResearchGeneration = researchGeneration(can)
	pending.ResearchLane = w.Round.Active.ActualLane
	for i := range pending.Requests {
		pending.Requests[i].Research = true
	}
	c.scheduler().InFlightCapture = pending.Batch.Capture.CaptureId.String()
}
func (c *Collector) continueDynamicSlot(now time.Time) {
	w := c.State.Dynamic
	m := w.Round.Active
	can := Candidate{Renter: m.Renter, Receiver: m.Receiver}
	o, ok := w.getOrder(dynamicPair(can))
	if !ok || o.Candidate.Closed || o.Candidate.Ambiguous || o.ClosurePending || generationBytes(o.Candidate) != m.Generation {
		m.Outcome = "superseded_or_closed"
		c.finishDynamicMember()
		return
	}
	e := w.getCheck(dynamicPair(can), o)
	if !e.Verified {
		m.Outcome = "seed_unverified"
		c.finishDynamicMember()
		return
	}
	w.Round.Continue = false
	c.startDynamicOperation(o, e, now)
}
func (c *Collector) finishDynamicMember() {
	w := c.State.Dynamic
	r := w.Round
	if r == nil || r.Active == nil {
		return
	}
	if w.files == nil {
		r.members = append(r.members, *r.Active)
	} else {
		b, err := runtimeCompress(*r.Active)
		if err != nil {
			w.err = err
			return
		}
		ref, err := w.files.PutObject(b)
		if err != nil {
			w.err = err
			return
		}
		r.Members = append(r.Members, ref)
	}
	r.Active = nil
	r.Continue = false
}
func (c *Collector) closeDynamicRound(reason string) {
	w := c.State.Dynamic
	r := w.Round
	if r == nil || r.Active != nil {
		return
	}
	members := append([]SelectionMember(nil), r.members...)
	for _, ref := range r.Members {
		raw, err := w.files.ReadObject(ref)
		if err != nil {
			w.err = err
			return
		}
		var m SelectionMember
		if err = runtimeExpand(raw, &m); err != nil {
			w.err = err
			return
		}
		members = append(members, m)
	}
	o := c.NewOperation("selection_round", "live", "local")
	cap := o.Batch.Capture
	proof := SelectionRound{CaptureId: cap.CaptureId, CaptureStartedAt: cap.CaptureStartedAt, RoundId: r.ID, Policy: DynamicPolicy, StartedAt: r.Started, FinishedAt: UTC(c.Now()), FocusTarget: 40, ExploreTarget: 10, Reason: reason, Population: w.population(), CoverageTime: c.dynamicCoverage(), CoverageScope: w.CoverageScope}
	for i := range members {
		members[i].CaptureId = cap.CaptureId
		members[i].CaptureStartedAt = cap.CaptureStartedAt
		if members[i].ActualLane == "focus" {
			proof.FocusCount++
		} else {
			proof.ExploreCount++
		}
	}
	w.each(func(k string, v dynamicOrder) bool {
		if !v.Candidate.Closed {
			e := w.getCheck(k, v)
			attempt := e.LastAttempt
			if attempt.IsZero() {
				attempt = v.FirstSeen
			}
			success := e.LastSuccess
			if success.IsZero() {
				success = v.FirstSeen
			}
			if !attempt.IsZero() && c.Now().After(attempt) {
				proof.MaxUnattemptedAgeSeconds = max(proof.MaxUnattemptedAgeSeconds, uint64(c.Now().Sub(attempt)/time.Second))
			}
			if !success.IsZero() && c.Now().After(success) {
				proof.MaxUnsuccessfulAgeSeconds = max(proof.MaxUnsuccessfulAgeSeconds, uint64(c.Now().Sub(success)/time.Second))
			}
		}

		if !v.Candidate.Closed && ((!w.getCheck(k, v).Verified && w.getCheck(k, v).Attempts >= 3) || len(v.Conflicts) > 32) {
			proof.Quarantined++
		}
		return true
	})
	o.Batch.Selections = members
	o.Batch.SelectionRound = &proof
	if reason != "round_limit" {
		o.Batch.Capture.Status = "partial"
		o.Batch.Capture.Reason = reason
	}
	c.State.Ops = append(c.State.Ops, o)
	w.LastRound = r.ID
	w.LastRoundAt = UTC(c.Now())
	w.Round = nil
	w.NextRound = UTC(c.Now().Add(30 * time.Second))
	c.Log(fmt.Sprintf("keeper_dynamic_round round=%s members=%d focus=%d explore=%d reason=%s", r.ID, len(members), proof.FocusCount, proof.ExploreCount, reason))
}

func (c *Collector) completeDynamic(o *Operation) {
	w := c.State.Dynamic
	if w == nil {
		return
	}
	if o.Kind == "rental_rate" {
		c.completeRentalRate(o)
		return
	}
	if o.DynamicSlot == 0 || w.Round == nil || w.Round.Active == nil || o.DynamicRound != w.Round.ID || o.DynamicSlot != w.Round.Active.SlotNo {
		return
	}
	s := c.scheduler()
	if s.InFlightCapture == o.Batch.Capture.CaptureId.String() {
		s.InFlightCapture = ""
	}
	s.NextCheck = UTC(c.Now().Add(30 * time.Second))
	m := w.Round.Active
	m.RPCAttempts += max(o.DynamicRPCAttempts, uint32(len(o.Evidence)))
	key := dynamicPair(*o.Candidate)
	current, exists := w.getOrder(key)
	if o.Kind == "seed" {
		m.SeedCaptureId = Ptr(o.Batch.Capture.CaptureId)
	} else {
		m.StateResultCaptureId = Ptr(o.Batch.Capture.CaptureId)
	}
	if !exists || generationBytes(current.Candidate) != m.Generation || current.Candidate.Closed {
		m.Outcome = "superseded_or_closed"
		c.finishDynamicMember()
		return
	}
	e := w.getCheck(key, current)
	if o.Kind == "seed" {
		if !o.Failed && current.Candidate.Verified && !current.Candidate.Ambiguous && !current.ClosurePending {
			e.Verified = true
			w.setCheck(key, e)
			w.Round.Continue = true
			return
		}
		e.LatestStatus = "seed_failed"
		e.NextCheck = UTC(c.Now().Add(30 * time.Minute))
		w.setCheck(key, e)
		m.Outcome = "seed_failed"
		c.finishDynamicMember()
		return
	}
	e.LatestStatus = "unknown"
	e.Failures++
	e.NextCheck = UTC(c.Now().Add(dynamicFailureDelay(e.Failures)))
	for _, r := range o.Batch.RentalStates {
		if !o.Failed && r.Status == "ok" && r.Liquidatable != nil && rentalStateIdentityVerified(r) {
			e.LatestStatus = "ok"
			e.LastSuccess = r.AvailableAt
			e.Deposit = copyInt(r.SecurityDepositSun)
			e.Liquidatable = Ptr(*r.Liquidatable)
			e.ObservedAt = *r.HeadAfterTime
			e.StateCapture = Ptr(r.CaptureId)
			if !*r.Liquidatable {
				e.Failures = 0
				e.NextCheck = UTC(c.Now().Add(30 * time.Minute))
				p := c.pressureFor(current, e, c.Now())
				if p != nil && p.numerator.Cmp(new(big.Int).Mul(p.denominator, big.NewInt(1800))) <= 0 {
					e.NextCheck = UTC(c.Now().Add(5 * time.Minute))
				}
			} else {
				certified := false
				for _, p := range o.Batch.Probes {
					if p.Status == "success_reward" && validatePositiveProbe(p) == nil {
						certified = true
					}
				}
				if certified {
					e.Failures = 0
					e.NextCheck = UTC(c.Now().Add(time.Minute))
				} else {
					e.LatestStatus = "simulation_unconfirmed"
				}
			}
		}
	}
	w.setCheck(key, e)
	m.Outcome = e.LatestStatus
	c.finishDynamicMember()
}
func dynamicFailureDelay(n uint8) time.Duration {
	if n > 4 {
		n = 4
	}
	return min(5*time.Minute*time.Duration(1<<max(0, int(n)-1)), 30*time.Minute)
}
