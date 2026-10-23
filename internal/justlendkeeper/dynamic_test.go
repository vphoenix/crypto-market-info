package justlendkeeper

import (
	"context"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func dynamicFixture(t *testing.T) (*Collector, *time.Time) {
	t.Helper()
	c, now := opportunityCollector(t)
	c.DynamicSelection = true
	c.State.Implementation, _ = HexAddress(ImplementationHex)
	c.State.CodeHash = mustHex(ImplementationSHA)
	c.State.IdentityAt = now.Add(-time.Minute)
	c.State.Dynamic = &dynamicWork{Version: 1, Ready: true, CoverageScope: "all_discovered_orders_history_incomplete", NextRate: now.Add(24 * time.Hour)}
	c.State.Dynamic.Rate = dynamicRateFixture(c, *now)
	return c, now
}
func dynamicRateFixture(c *Collector, now time.Time) *RentalRateObservation {
	contract, _ := HexAddress(ContractHex)
	return &RentalRateObservation{CaptureId: uuid.New(), CaptureStartedAt: now, ContractAddress: contract, ResourceType: 1, RequestStartedAt: Ptr(now), ResponseReceivedAt: Ptr(now), AvailableAt: now, HeadBeforeNumber: Ptr(uint64(100)), HeadAfterNumber: Ptr(uint64(100)), HeadBeforeHash: Ptr(Hash([]byte("head"))), HeadAfterHash: Ptr(Hash([]byte("head"))), HeadBeforeTime: Ptr(now), HeadAfterTime: Ptr(now), ImplementationAddress: Ptr(c.State.Implementation), ImplementationCodeHash: Ptr(c.State.CodeHash), ImplementationCheckedAt: Ptr(c.State.IdentityAt), IdentityStatus: "verified_manifest", Status: "ok", ApiSuccess: Ptr(true), TvmResult: "SUCCESS", Rate: big.NewInt(1000000000000), Scale: 1000000000000000000, StateBinding: "node_latest_unpinned", RequestPayloadHash: Ptr(Hash([]byte("request"))), ResponsePayloadHash: Ptr(Hash([]byte("response")))}
}
func addDynamicFixture(c *Collector, i int, now time.Time, verified bool) dynamicOrder {
	can := opportunityCandidate(i, now, verified)
	o := dynamicOrder{Candidate: can, Amount: big.NewInt(100), Deposit: big.NewInt(int64(i + 1)), BaseAt: now.Add(-time.Minute), FirstSeen: now.Add(-time.Duration(i+1) * time.Second)}
	key := dynamicPair(can)
	w := c.State.Dynamic
	w.setOrder(key, o)
	e := w.getCheck(key, o)
	e.FairSince = o.FirstSeen
	w.setCheck(key, e)
	return o
}
func finishDynamicFixture(c *Collector, now *time.Time) {
	o := c.State.Ops[0]
	o.Evidence = make([]Evidence, 3)
	r := dynamicRateFixture(c, *now)
	o.Batch.RentalStates = []RentalStateObservation{{CaptureId: o.Batch.Capture.CaptureId, Renter: o.Candidate.Renter, Receiver: o.Candidate.Receiver, Status: "ok", Liquidatable: Ptr(false), SecurityDepositSun: big.NewInt(100), HeadAfterTime: Ptr(*now), IdentityStatus: r.IdentityStatus, ImplementationAddress: r.ImplementationAddress, ImplementationCodeHash: r.ImplementationCodeHash, ImplementationCheckedAt: r.ImplementationCheckedAt, RequestStartedAt: r.RequestStartedAt, ResponseReceivedAt: r.ResponseReceivedAt, AvailableAt: *now}}
	c.completeDynamic(&o)
	c.State.Ops = nil
	*now = now.Add(41 * time.Second)
	opportunityFresh(c, *now)
	c.State.IdentityAt = now.Add(-time.Minute)
	c.State.Dynamic.Rate = dynamicRateFixture(c, *now)
}

func TestDynamicFiftyAdmissionsFortyTenAndAudit(t *testing.T) {
	c, now := dynamicFixture(t)
	for i := 0; i < 120; i++ {
		addDynamicFixture(c, i, *now, true)
	}
	seen := map[string]bool{}
	focus, explore := 0, 0
	for i := 0; i < 50; i++ {
		c.scheduleResearch(*now)
		if len(c.State.Ops) != 1 {
			t.Fatalf("slot %d: no operation", i+1)
		}
		o := c.State.Ops[0]
		if o.Kind != "rental_state" {
			t.Fatal("unexpected operation", o.Kind)
		}
		key := dynamicPair(*o.Candidate)
		if seen[key] {
			t.Fatal("duplicate identity in round")
		}
		seen[key] = true
		m := c.State.Dynamic.Round.Active
		want := "focus"
		if (i+1)%5 == 0 {
			want = "explore"
		}
		if m.RequestedLane != want || m.ActualLane != want {
			t.Fatalf("slot %d lane %s/%s", i+1, m.RequestedLane, m.ActualLane)
		}
		if want == "focus" {
			focus++
		} else {
			explore++
		}
		finishDynamicFixture(c, now)
	}
	if focus != 40 || explore != 10 {
		t.Fatal(focus, explore)
	}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "selection_round" {
		t.Fatal("round did not close")
	}
	b := c.State.Ops[0].Batch
	b.Capture.EvidenceManifestHash = Hash([]byte("summary"))
	b.Capture.AvailableAt = *now
	Seal(&b)
	if err := Validate(b); err != nil {
		t.Fatal(err)
	}
	if b.SelectionRound.FocusCount != 40 || b.SelectionRound.ExploreCount != 10 || len(b.Selections) != 50 {
		t.Fatal("audit counts")
	}
	b.Selections[0].DepositSun = big.NewInt(999)
	Seal(&b)
	if err := Validate(b); err == nil {
		t.Fatal("changed admission input accepted")
	}
}
func TestDynamicExplorationUsesWaitingNotNextScore(t *testing.T) {
	c, now := dynamicFixture(t)
	for i := 0; i < 60; i++ {
		addDynamicFixture(c, i, *now, true)
	}
	w := c.State.Dynamic
	w.Round = &dynamicRound{ID: uuid.New(), Started: *now, Slots: 4}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 {
		t.Fatal("no explorer")
	}
	if c.State.Ops[0].Candidate.Renter != opportunityCandidate(59, *now, true).Renter {
		t.Fatal("exploration selected next score instead of oldest waiting")
	}
}
func TestDynamicPressureExactUnknownAndLargeIntegers(t *testing.T) {
	c, now := dynamicFixture(t)
	o := addDynamicFixture(c, 1, *now, true)
	e := c.State.Dynamic.getCheck(dynamicPair(o.Candidate), o)
	p := c.pressureFor(o, e, *now)
	if p == nil {
		t.Fatal("no score")
	}
	want := new(big.Int).Sub(new(big.Int).Mul(o.Deposit, big.NewInt(1000000000000000000)), new(big.Int).Mul(new(big.Int).Mul(o.Amount, c.State.Dynamic.Rate.Rate), big.NewInt(60)))
	if p.numerator.Cmp(want) != 0 {
		t.Fatal("decimal arithmetic changed")
	}
	o.Amount = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	o.Deposit = copyInt(o.Amount)
	c.State.Dynamic.Rate.Rate = copyInt(o.Amount)
	if p = c.pressureFor(o, e, *now); p == nil || p.denominator.BitLen() <= 256 {
		t.Fatal("intermediate multiplication truncated")
	}
	for _, kind := range []string{"zero_rate", "old_base", "future_base", "old_rate", "missing_amount"} {
		t.Run(kind, func(t *testing.T) {
			c, now := dynamicFixture(t)
			o := addDynamicFixture(c, 1, *now, true)
			e := c.State.Dynamic.getCheck(dynamicPair(o.Candidate), o)
			switch kind {
			case "zero_rate":
				c.State.Dynamic.Rate.Rate = big.NewInt(0)
			case "old_base":
				o.BaseAt = now.Add(-25 * time.Hour)
			case "future_base":
				o.BaseAt = now.Add(time.Second)
			case "old_rate":
				c.State.Dynamic.Rate.AvailableAt = now.Add(-6 * time.Minute)
			case "missing_amount":
				o.Amount = nil
			}
			if c.pressureFor(o, e, *now) != nil {
				t.Fatal("unknown became a numeric score")
			}
		})
	}
}
func TestDynamicUnknownRetainsLastGoodAndLosesPositivePriority(t *testing.T) {
	c, now := dynamicFixture(t)
	o := addDynamicFixture(c, 2, *now, true)
	key := dynamicPair(o.Candidate)
	e := c.State.Dynamic.getCheck(key, o)
	e.LatestStatus = "ok"
	e.Liquidatable = Ptr(true)
	e.Deposit = big.NewInt(10)
	e.ObservedAt = *now
	e.LastSuccess = *now
	c.State.Dynamic.setCheck(key, e)
	c.scheduleResearch(*now)
	op := c.State.Ops[0]
	op.Failed = true
	c.completeDynamic(&op)
	e = c.State.Dynamic.getCheck(key, o)
	if e.LatestStatus != "unknown" || e.Deposit == nil || e.Deposit.Int64() != 10 || !e.LastSuccess.Equal(*now) {
		t.Fatal("unknown erased last good or became false")
	}
	*now = now.Add(time.Hour)
	opportunityFresh(c, *now)
	c.State.Ops = nil
	c.State.Dynamic.Round = nil
	c.State.Dynamic.Rate = dynamicRateFixture(c, *now)
	if ch, ok := c.chooseDynamic("focus", *now); ok && ch.class == 0 {
		t.Fatal("failed positive retained fresh-positive priority")
	}
}
func TestDynamicNoCapFairnessAndNoIdleCapture(t *testing.T) {
	c, now := dynamicFixture(t)
	for i := 0; i < 1700; i++ {
		addDynamicFixture(c, i, *now, false)
	}
	if c.State.Dynamic.population() != 1700 {
		t.Fatal("candidate population capped")
	}
	seen := map[string]bool{}
	for i := 0; i < 1700; i++ {
		ch, ok := c.chooseDynamic("explore", *now)
		if !ok {
			t.Fatal("fair pass ended")
		}
		if seen[ch.key] {
			t.Fatal("starved candidate")
		}
		seen[ch.key] = true
		e := ch.check
		e.FairSince = *now
		c.State.Dynamic.setCheck(ch.key, e)
		*now = now.Add(time.Second)
	}
	c, now = dynamicFixture(t)
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 0 || c.State.Dynamic.Round != nil {
		t.Fatal("empty capture generated")
	}
}
func TestDynamicFiftiethSeedContinuesWithinSameSlot(t *testing.T) {
	c, now := dynamicFixture(t)
	o := addDynamicFixture(c, 1, *now, true)
	w := c.State.Dynamic
	m := SelectionMember{RoundId: uuid.New(), SlotNo: 50, Renter: o.Candidate.Renter, Receiver: o.Candidate.Receiver, Generation: generationBytes(o.Candidate)}
	w.Round = &dynamicRound{ID: m.RoundId, Started: *now, Slots: 50, Active: &m, Continue: true}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 || c.State.Ops[0].Kind != "rental_state" || c.State.Ops[0].DynamicSlot != 50 {
		t.Fatal("last slot blocked after seed")
	}
}
func TestDynamicNativeRecoveryAndTimeIndependentPageMetadata(t *testing.T) {
	c, now := dynamicFixture(t)
	w := c.State.Dynamic
	c.State.Dynamic = nil
	dir, binding := keeperRuntimeFixture(t, c)
	c.State.Dynamic = w
	if err := w.open(c.runtime.files); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1500; i++ {
		addDynamicFixture(c, i, *now, true)
	}
	c.scheduleResearch(*now)
	if len(c.State.Ops) != 1 {
		t.Fatal("no admission")
	}
	round := w.Round.ID
	slot := w.Round.Active.SlotNo
	key := dynamicPair(*c.State.Ops[0].Candidate)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(3 * time.Hour)
	if err := c.OpenRuntime(context.Background(), dir, binding); err != nil {
		t.Fatal(err)
	}
	w = c.State.Dynamic
	if w.Round.ID != round || w.Round.Active.SlotNo != slot || w.population() != 1500 {
		t.Fatal("recovery lost round or population")
	}
	o, ok := w.getOrder(key)
	if !ok {
		t.Fatal("order lost")
	}
	got := w.getCheck(key, o)
	if got.LastAttempt.IsZero() || got.Generation != generationBytes(o.Candidate) {
		t.Fatal("admission progress lost")
	}
	before := c.runtime.files.Metrics()
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	after := c.runtime.files.Metrics()
	if after.WrittenBytes != before.WrittenBytes {
		t.Fatal("idle save wrote data")
	}
}

// Compile-time guard: this model intentionally contains no float fields.
func TestDynamicModelTypesAndNullableProof(t *testing.T) {
	for _, tab := range DynamicTables() {
		typ := reflect.TypeOf(tab.Model)
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.Type.Kind() == reflect.Float64 || f.Type.Kind() == reflect.Float32 {
				t.Fatal("float model", f.Name)
			}
		}
	}
	c, now := dynamicFixture(t)
	o := c.NewOperation("rental_rate", "live", "publicnode")
	r := *dynamicRateFixture(c, *now)
	r.CaptureId = o.Batch.Capture.CaptureId
	r.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
	o.Batch.Rates = []RentalRateObservation{r}
	o.Batch.Capture.EvidenceManifestHash = Hash([]byte("summary"))
	Seal(&o.Batch)
	if err := Validate(o.Batch); err != nil {
		t.Fatal(err)
	}
	o.Batch.RateProof = nil
	if err := Validate(o.Batch); err == nil {
		t.Fatal("missing proof accepted")
	}
}
