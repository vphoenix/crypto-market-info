package optionslive

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"testing"
	"time"
)

func catalogFixture(t *testing.T) (*engine, time.Time, *[]options.CatalogEnvelope, uuid.UUID) {
	old, at, _ := engineFixture(t)
	spec := old.specs[1]
	r := old.run
	r.ID = uuid.New()
	r.Selection = options.CatalogSelection
	r.Members = r.Members[:1]
	var out []options.CatalogEnvelope
	e, err := newCatalogEngine(r, []options.ContractSpec{spec}, at.Add(-time.Second), 20000, func(uint32) {}, func() {}, func(v options.CatalogEnvelope) error { out = append(out, v); return nil })
	if err != nil {
		t.Fatal(err)
	}
	p := options.CollectionPlan{ID: uuid.New(), SessionID: uuid.New(), Revision: 1, ConfigHash: options.PayloadHash([]byte("config")), CreatedAt: at.Add(-time.Second), EffectiveMinute: at, InstrumentIDs: []uint32{1}, Definitions: []string{spec.DefinitionHash()}, RunIDs: []uuid.UUID{r.ID}, ObservationIDs: []uuid.UUID{uuid.New()}}
	handleTest(t, e, event{At: at.Add(-time.Second), Plan: &p})
	epoch := uuid.New()
	s := catalogState{InstrumentID: 1, Known: true, Open: true, StateID: uuid.New(), RuleObservationID: uuid.New(), StateKind: 2, RuleID: options.PayloadHash([]byte("rule")), StateAt: at.Add(-time.Second), StateObservedAt: at.Add(-time.Second), RuleAt: at.Add(-time.Second), ObservedAt: at.Add(-time.Second), Epoch: epoch}
	handleTest(t, e, event{At: at.Add(-time.Second), State: &s})
	g := catalogGate{Epoch: epoch, Known: true, BaselineID: uuid.New(), LockMode: "false", ConfirmedAt: at.Add(-time.Second)}
	handleTest(t, e, event{At: at.Add(-time.Second), Gate: &g})
	bookSnapshot(t, e, at.Add(-time.Second), epoch)
	return e, at, &out, epoch
}
func bookSnapshot(t *testing.T, e *engine, at time.Time, epoch uuid.UUID) {
	spec := e.specs[1]
	ch := "book." + spec.Instrument.ExchangeSymbol + ".100ms"
	for _, kind := range []string{"connected", "ready"} {
		handleTest(t, e, event{At: at, InstrumentID: 1, Stream: deribit.StreamEvent{Kind: kind, Epoch: epoch, ReceivedAt: at}})
	}
	raw := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":%q,"data":{"type":"snapshot","instrument_name":%q,"timestamp":%d,"change_id":1,"bids":[["new",1,2]],"asks":[["new",2,3]]}}}`, ch, spec.Instrument.ExchangeSymbol, at.UnixMilli()))
	handleTest(t, e, event{At: at, InstrumentID: 1, Stream: deribit.StreamEvent{Kind: "message", Channel: ch, Epoch: epoch, Raw: raw, ReceivedAt: at}})
}
func catalogStep(t *testing.T, e *engine, at time.Time) {
	e.catalog.gate.ConfirmedAt = at.Add(-time.Microsecond)
	for id, c := range e.catalog.connections {
		c.confirmed = at.Add(-time.Microsecond)
		e.catalog.connections[id] = c
	}
	stepTest(t, e, at, true)
}
func TestCatalogUnpairedFullMinuteAndRecoveryFence(t *testing.T) {
	e, at, out, epoch := catalogFixture(t)
	for sec := 0; sec < 60; sec++ {
		now := at.Add(time.Duration(sec) * time.Second)
		if sec == 10 {
			s := e.catalog.states[1]
			s.Open = false
			handleTest(t, e, event{At: now.Add(-time.Millisecond), State: &s})
		}
		if sec == 11 {
			s := e.catalog.states[1]
			s.Open = true
			handleTest(t, e, event{At: now.Add(-time.Millisecond), State: &s})
		}
		if sec == 20 {
			bookSnapshot(t, e, now.Add(-time.Millisecond), uuid.New())
		}
		if sec == 30 {
			raw := []byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.bad.100ms","data":{}}}`)
			handleTest(t, e, event{At: now.Add(-time.Millisecond), InstrumentID: 1, Stream: deribit.StreamEvent{Kind: "message", Epoch: epoch, Raw: raw}})
		}
		catalogStep(t, e, now)
	}
	if len(*out) != 1 {
		t.Fatal("missing catalog minute")
	}
	b := (*out)[0].Live.Books[0]
	for sec := 0; sec < 60; sec++ {
		r, err := replay.ReplayDerivative(b, uint8(sec))
		if err != nil {
			t.Fatal(err)
		}
		want := sec < 10 || sec >= 20
		if r.Quality.ReplayValid != want {
			t.Fatalf("second %d valid=%v want=%v", sec, r.Quality.ReplayValid, want)
		}
	}
	if err := (*out)[0].Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestCatalogPlanSwitchDoesNotWaitForWriter(t *testing.T) {
	e, at, out, _ := catalogFixture(t)
	for sec := 0; sec < 60; sec++ {
		catalogStep(t, e, at.Add(time.Duration(sec)*time.Second))
	}
	old := (*out)[0]
	r := e.run
	r.ID = uuid.New()
	r.StartedAt = at.Add(30 * time.Second)
	p := e.catalog.plans[0]
	next := p
	next.ID = uuid.New()
	next.Revision++
	next.PreviousID = p.ID
	next.PreviousHash = p.Hash()
	next.CreatedAt = at.Add(30 * time.Second)
	next.EffectiveMinute = at.Add(time.Minute)
	next.RunIDs = []uuid.UUID{r.ID}
	next.RowHash = ""
	handleTest(t, e, event{At: at.Add(59900 * time.Millisecond), Plan: &next})
	handleTest(t, e, event{At: at.Add(59901 * time.Millisecond), Swap: &catalogSwap{At: next.EffectiveMinute, Run: r, Specs: []options.ContractSpec{e.specs[1]}}})
	for sec := 0; sec < 60; sec++ {
		catalogStep(t, e, next.EffectiveMinute.Add(time.Duration(sec)*time.Second))
	}
	if len(*out) != 2 || (*out)[1].Live.RunID != r.ID || old.Live.RunID == r.ID {
		t.Fatal("ownership did not switch at minute boundary")
	}
	if !(*out)[1].Live.Books[0].Quality[0].ReplayValid {
		t.Fatal("unchanged book lost minute anchor")
	}
}
func TestCatalogGateReopeningRequiresFreshSnapshot(t *testing.T) {
	e, at, _, _ := catalogFixture(t)
	catalogStep(t, e, at)
	g := e.catalog.gate
	g.Known = false
	handleTest(t, e, event{At: at.Add(10 * time.Millisecond), Gate: &g})
	g.Known = true
	handleTest(t, e, event{At: at.Add(20 * time.Millisecond), Gate: &g})
	catalogStep(t, e, at.Add(time.Second))
	_, q := e.books[1].Current()
	if q.StreamValid || q.Reason == model.DerivativeOK {
		t.Fatal("gate reopening reused old snapshot")
	}
}
