package justlendkeeper

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDynamicSameBlockRequiresAllConflictingOrigins(t *testing.T) {
	c, now := dynamicFixture(t)
	a := opportunityCandidate(1, *now, false).Origin
	b := a
	b.Transaction = fmt.Sprintf("%064x", 999)
	c.dynamicApplyRaw(a, uuid.New(), 0, nil)
	c.dynamicApplyRaw(b, uuid.New(), 1, nil)
	r1, _ := EventRow(a)
	r2, _ := EventRow(b)
	for _, r := range []*RentalEvent{&r1, &r2} {
		r.BlockHash = Hash([]byte("block"))
		r.PositionStatus = "receipt_verified"
		r.Finality = "solid"
		r.ReceiptLogIndex = Ptr(uint32(1))
		r.CaptureId = uuid.New()
	}
	r1.TransactionIndex = Ptr(uint32(2))
	r2.TransactionIndex = Ptr(uint32(1))
	key := dynamicPair(Candidate{Renter: r1.Renter, Receiver: r1.Receiver})
	o, _ := c.State.Dynamic.getOrder(key)
	if !o.Candidate.Ambiguous || len(o.Conflicts) != 2 {
		t.Fatal("same-block source ordering guessed")
	}
	c.dynamicApplyBatch(Batch{Events: []RentalEvent{r1}})
	o, _ = c.State.Dynamic.getOrder(key)
	if !o.Candidate.Ambiguous {
		t.Fatal("one verified origin hid its competitor")
	}
	c.dynamicApplyBatch(Batch{Events: []RentalEvent{r2, r1}})
	o, _ = c.State.Dynamic.getOrder(key)
	if o.Candidate.Ambiguous || !o.Candidate.Verified || len(o.Conflicts) != 0 || o.Candidate.Origin.Transaction != a.Transaction {
		t.Fatal("canonical receipt order not respected")
	}
}
func TestDynamicRateStrictABIAndHeadFailure(t *testing.T) {
	c, _, caller, ev, now := rewardParseContext()
	c.State.Dynamic = &dynamicWork{Version: 1}
	c.State.CallersValid = []string{caller}
	c.Now = func() time.Time { return ev.Available }
	for _, tc := range []struct {
		word string
		ok   bool
	}{{fmt.Sprintf("%064x", 123), true}, {strings.Repeat("0", 64), true}, {"01", false}, {strings.Repeat("f", 192), false}, {strings.Repeat("z", 64), false}} {
		o := c.NewOperation("rental_rate", "live", "publicnode")
		o.Scheduled = now
		o.Caller = caller
		body, _ := json.Marshal(map[string]any{"result": map[string]any{"result": true}, "transaction": map[string]any{"ret": []any{map[string]any{}}}, "constant_result": []string{tc.word}})
		if err := c.processRentalRate(&o, ev, body); err != nil {
			t.Fatal(err)
		}
		if (o.Batch.Rates[0].Status == "ok") != tc.ok {
			t.Fatal("wrong ABI interpretation", tc.word)
		}
		c.finalizeRentalRates(&o, ev.Available)
		if o.Batch.Rates[0].Status == "ok" {
			t.Fatal("rate certified without surrounding heads")
		}
	}
}
func TestDynamicRetryAttributionReservedBeforeSend(t *testing.T) {
	c, now := dynamicFixture(t)
	addDynamicFixture(c, 1, *now, true)
	c.scheduleResearch(*now)
	o := &c.State.Ops[0]
	id := o.Batch.Capture.CaptureId.String()
	before := c.State.Used
	r := Request{Source: "publicnode", Research: true, AttemptCapture: id}
	if err := c.API.Limiter.ReserveRequest(r); err != nil {
		t.Fatal(err)
	}
	if o.DynamicRPCAttempts != 1 || c.State.Used != before+1 {
		t.Fatal("attempt not durably attributed")
	}
	if err := c.API.Limiter.ReserveRequest(r); err == nil {
		t.Fatal("gate bypass")
	}
	if o.DynamicRPCAttempts != 1 {
		t.Fatal("rejected request counted")
	}
}

type dynamicRebuildStore struct {
	memoryStore
	BatchesByID map[uuid.UUID]Batch
	Caps        []Capture
}

func (s *dynamicRebuildStore) KeeperCapturePage(_ context.Context, _ string, after []byte, _ int) ([]Capture, []byte, error) {
	if len(after) != 0 {
		return nil, nil, nil
	}
	id := s.Caps[len(s.Caps)-1].CaptureId
	return s.Caps, append([]byte(nil), id[:]...), nil
}
func (s *dynamicRebuildStore) KeeperBatch(_ context.Context, c Capture) (Batch, error) {
	return s.BatchesByID[c.CaptureId], nil
}

func TestDynamicRebuildUsesCommittedRowsBeyondLegacyCache(t *testing.T) {
	c, _, now := testCollector(t)
	keeperRuntimeFixture(t, c)
	c.DynamicSelection = true
	b := keeperRuntimePage(c, now.Add(-2*time.Hour), *now, "", "", "live")
	for i := 0; i < 200; i++ {
		raw := opportunityCandidate(i, *now, false).Origin
		raw.Evidence.ResponseHash = Hex(Hash([]byte("source")))
		raw.Evidence.Started = *now
		raw.Evidence.Available = *now
		row, err := indexedRow(raw, b.Capture, uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		b.IndexedEvents = append(b.IndexedEvents, row)
	}
	b.Capture.DiscoveredCandidates = 200
	Seal(&b)
	store := &dynamicRebuildStore{BatchesByID: map[uuid.UUID]Batch{b.Capture.CaptureId: b}, Caps: []Capture{b.Capture}}
	c.Store = store
	c.State.Candidates = []Candidate{opportunityCandidate(0, *now, false)}
	before := c.State.Used
	if err := c.RebuildDynamic(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !c.State.Dynamic.Ready || c.State.Dynamic.population() != 200 || c.State.Used != before {
		t.Fatal("rebuild used bounded cache or consumed network budget")
	}
	if c.State.Dynamic.CoverageScope != "all_discovered_orders_history_incomplete" {
		t.Fatal("partial history claimed full market")
	}
}

func TestDynamicHistorySharesBudgetAndRotatesSlowly(t *testing.T) {
	c, now := dynamicFixture(t)
	w := c.State.Dynamic
	c.ensureDynamicHistory()
	if len(c.State.Scans) != 3 || w.HistoryTo.IsZero() {
		t.Fatal("history discovery not scheduled")
	}
	c.ensureDynamicHistory()
	if len(c.State.Scans) != 3 {
		t.Fatal("duplicate history scans")
	}
	if !c.allowDynamicHistory(c.State.Scans[0]) || c.allowDynamicHistory(c.State.Scans[1]) {
		t.Fatal("historical page burst")
	}
	*now = now.Add(time.Minute)
	opportunityFresh(c, *now)
	if !c.allowDynamicHistory(c.State.Scans[1]) {
		t.Fatal("history kind starved")
	}
	*now = now.Add(time.Minute)
	opportunityFresh(c, *now)
	if !c.allowDynamicHistory(c.State.Scans[2]) {
		t.Fatal("third history kind starved")
	}
	for i, sc := range c.State.Scans {
		sc.Done = true
		c.completeDynamicHistory(sc)
		if i < 2 && w.CoverageScope != "all_discovered_orders_history_incomplete" {
			t.Fatal("partial history declared complete")
		}
	}
	if w.CoverageScope != "all_available_contract_history_provider_indexed" {
		t.Fatal("provider completion not labeled")
	}
	*now = now.Add(10 * time.Minute)
	if c.allowDynamicHistory(c.State.Scans[0]) {
		t.Fatal("history continued with stale real-time coverage")
	}
}

func TestDynamicZeroFalseCannotMonopolizeFocus(t *testing.T) {
	c, now := dynamicFixture(t)
	o := addDynamicFixture(c, 1, *now, true)
	key := dynamicPair(o.Candidate)
	e := c.State.Dynamic.getCheck(key, o)
	e.Deposit = big.NewInt(0)
	e.Liquidatable = Ptr(false)
	e.LatestStatus = "ok"
	e.ObservedAt = *now
	c.State.Dynamic.setCheck(key, e)
	if _, ok := c.chooseDynamic("focus", *now); ok {
		t.Fatal("zero false tuple monopolized risk ranking")
	}
	if _, ok := c.chooseDynamic("explore", *now); !ok {
		t.Fatal("zero false incorrectly treated as closed")
	}
}

func TestDynamicOldSourceCorrectsDiscoveryWithoutReplacingGeneration(t *testing.T) {
	c, now := dynamicFixture(t)
	fresh := opportunityCandidate(2, *now, false).Origin
	fresh.Evidence.Available = *now
	old := fresh
	old.BlockNumber--
	old.Transaction = fmt.Sprintf("%064x", 1)
	old.Evidence.Available = now.Add(-24 * time.Hour)
	c.dynamicApplyRaw(fresh, uuid.New(), 0, nil)
	key := dynamicPair(Candidate{Renter: opportunityCandidate(2, *now, false).Renter, Receiver: opportunityCandidate(2, *now, false).Receiver})
	c.dynamicApplyRaw(old, uuid.New(), 0, nil)
	o, _ := c.State.Dynamic.getOrder(key)
	e := c.State.Dynamic.getCheck(key, o)
	if o.Candidate.Origin.Transaction != fresh.Transaction || !o.FirstSeen.Equal(old.Evidence.Available) || !e.FairSince.Equal(old.Evidence.Available) {
		t.Fatal("capture UUID order corrupted discovery age or generation")
	}
	e.LastAttempt = *now
	e.FairSince = *now
	c.State.Dynamic.setCheck(key, e)
	old.Evidence.Available = now.Add(-48 * time.Hour)
	c.dynamicApplyRaw(old, uuid.New(), 0, nil)
	o, _ = c.State.Dynamic.getOrder(key)
	e = c.State.Dynamic.getCheck(key, o)
	if !e.FairSince.Equal(*now) {
		t.Fatal("old history reset an already attempted member")
	}
}
