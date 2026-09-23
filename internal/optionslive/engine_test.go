package optionslive

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

func engineFixture(t *testing.T) (*engine, time.Time, *[]options.LiveEnvelope) {
	t.Helper()
	at := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	var specs []options.ContractSpec
	for _, kind := range []string{"option", "future"} {
		raw, err := os.ReadFile("../exchange/deribit/testdata/metadata-BTC-" + kind + ".json")
		if err != nil {
			t.Fatal(err)
		}
		items, _, err := deribit.DecodeInstruments(raw, "BTC", kind, at.Add(-time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range items {
			p.Spec.Instrument.ID = uint32(len(specs) + 1)
			specs = append(specs, p.Spec)
		}
	}
	r := options.LiveRun{ID: uuid.New(), StartedAt: at.Add(-time.Minute), RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2", Selection: "explicit", Indexes: []string{"btc_usd"}}
	for _, s := range specs {
		r.Members = append(r.Members, options.LiveMember{InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, DefinitionHash: s.DefinitionHash(), IndexID: s.IndexID})
	}
	var batches []options.LiveEnvelope
	e, err := newEngine(r, specs, at.Add(-time.Second), func(string) {}, func() {}, func(b options.LiveEnvelope) error { batches = append(batches, b); return nil })
	if err != nil {
		t.Fatal(err)
	}
	e.now = func() time.Time { return at.Add(time.Millisecond) }
	epoch := uuid.New()
	for _, group := range []string{"book", "index"} {
		handleTest(t, e, event{At: at.Add(-2 * time.Second), Group: group, Stream: deribit.StreamEvent{Kind: "connected", Epoch: epoch}})
		handleTest(t, e, event{At: at.Add(-time.Second), Group: group, Stream: deribit.StreamEvent{Kind: "ready", Epoch: epoch}})
	}
	for _, s := range specs {
		meta := options.MetadataObservation{RunID: r.ID, InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, DefinitionHash: s.DefinitionHash(), Status: "complete", State: "open", Active: true, ObservedAt: at.Add(-time.Second), TradingRuleID: options.PayloadHash([]byte(s.Instrument.ExchangeSymbol))}
		handleTest(t, e, event{At: at.Add(-time.Second), Metadata: &meta})
		channel := "book." + s.Instrument.ExchangeSymbol + ".100ms"
		raw := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":%q,"data":{"type":"snapshot","instrument_name":%q,"timestamp":%d,"change_id":1,"bids":[["new",1,2]],"asks":[["new",2,3]]}}}`, channel, s.Instrument.ExchangeSymbol, at.Add(-time.Second).UnixMilli()))
		handleTest(t, e, event{At: at.Add(-time.Second), Group: "book", Stream: deribit.StreamEvent{Kind: "message", Channel: channel, Epoch: epoch, Raw: raw}})
	}
	price := decimal.RequireFromString("12345.123456789012345678")
	e.indexes["btc_usd"] = options.IndexSample{Price: &price, SourceTime: at.Add(-2 * time.Second), ReceivedAt: at.Add(-time.Second), Epoch: epoch, State: options.IndexObserved}
	return e, at, &batches
}
func handleTest(t *testing.T, e *engine, v event) {
	t.Helper()
	if err := e.handle(v, v.At.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
}
func stepTest(t *testing.T, e *engine, at time.Time, confirm bool) {
	t.Helper()
	e.now = func() time.Time { return at.Add(time.Millisecond) }
	if confirm {
		for key, c := range e.connections {
			c.confirmed = at.Add(-time.Microsecond)
			e.connections[key] = c
		}
	}
	handleTest(t, e, event{At: at, Boundary: true})
}

func TestLiveEngineFullMinuteAndSourceTimes(t *testing.T) {
	e, at, batches := engineFixture(t)
	for sec := 0; sec < 60; sec++ {
		stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
	}
	if len(*batches) != 1 {
		t.Fatal("missing complete minute")
	}
	b := (*batches)[0]
	if err := b.ValidateRun(e.run); err != nil {
		t.Fatal(err)
	}
	for _, book := range b.Books {
		for sec := 0; sec < 60; sec++ {
			r, err := replay.ReplayDerivative(book, uint8(sec))
			if err != nil || !r.Quality.ReplayValid || !r.Snapshot.SourceTime.Equal(at.Add(-time.Second)) {
				t.Fatalf("bad replay %d: %+v %v", sec, r, err)
			}
		}
	}
	if b.Indexes[0].Samples[59].State != options.IndexHeld || !b.Indexes[0].Samples[59].SourceTime.Equal(at.Add(-2*time.Second)) {
		t.Fatal("held index refreshed source time")
	}
}
func TestLiveFreezeCompletionAndConnectionAge(t *testing.T) {
	t.Run("completion crosses deadline", func(t *testing.T) {
		e, at, _ := engineFixture(t)
		e.now = func() time.Time { return at.Add(251 * time.Millisecond) }
		handleTest(t, e, event{At: at, Boundary: true})
		for sec := 1; sec < 60; sec++ {
			stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
		}
		for _, buf := range e.buffers {
			b, err := buf.Complete()
			if err != nil {
				t.Fatal(err)
			}
			if b.Minute != nil || b.Quality[0].Reason != model.DerivativeSamplingLag {
				t.Fatal("late freeze became valid anchor")
			}
		}
	})
	t.Run("quiet books require healthy connection", func(t *testing.T) {
		e, at, _ := engineFixture(t)
		for sec := 0; sec < 60; sec++ {
			stepTest(t, e, at.Add(time.Duration(sec)*time.Second), false)
		}
		for _, buf := range e.buffers {
			b, err := buf.Complete()
			if err != nil {
				t.Fatal(err)
			}
			if !b.Quality[0].ReplayValid || b.Quality[31].ReplayValid {
				t.Fatal("expired connection held valid")
			}
		}
		if e.indexMinutes[0].Samples[31].Price != nil {
			t.Fatal("disconnected index retained value")
		}
	})
}
func TestLiveRulePublicationClosedAndPartialMinutes(t *testing.T) {
	e, at, batches := engineFixture(t)
	first := e.run.Members[0].InstrumentID
	before := e.metadata[first].observation.TradingRuleID
	stepTest(t, e, at, true)
	o := e.metadata[first].observation
	o.TradingRuleID = options.PayloadHash([]byte("new rule"))
	o.ObservedAt = at
	handleTest(t, e, event{At: at.Add(100 * time.Microsecond), Metadata: &o})
	for sec := 1; sec < 20; sec++ {
		stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
	}
	if len(*batches) != 0 {
		t.Fatal("partial minute published")
	}
	o.Active = false
	o.State = "closed"
	o.ObservedAt = at.Add(19500 * time.Millisecond)
	handleTest(t, e, event{At: o.ObservedAt, Metadata: &o})
	for sec := 20; sec < 60; sec++ {
		stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
	}
	b := (*batches)[0].Books[0]
	if b.Quality[0].TradingRuleID != before || b.Quality[1].TradingRuleID != o.TradingRuleID || b.Quality[20].ReplayValid || b.Quality[20].Reason != model.DerivativeMarketClosed {
		t.Fatal("rule/state cutoff broken")
	}
}
func TestLiveStartupAfterMinuteBoundary(t *testing.T) {
	e, at, _ := engineFixture(t)
	specs := []options.ContractSpec{}
	for _, m := range e.run.Members {
		specs = append(specs, e.specs[m.InstrumentID])
	}
	late, err := newEngine(e.run, specs, at.Add(time.Millisecond), func(string) {}, func() {}, func(options.LiveEnvelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !late.next.Equal(at.Add(time.Minute)) {
		t.Fatal("missed startup boundary retained")
	}
	stepTest(t, late, at.Add(time.Second), false)
}

func TestLiveGapRecoveryRejectsOldEpoch(t *testing.T) {
	e, at, batches := engineFixture(t)
	first := e.run.Members[0]
	oldEpoch := e.connections["book"].epoch
	newEpoch := uuid.New()
	resets := 0
	e.reset = func(group string) {
		if group == "book" {
			resets++
		}
	}
	channel := "book." + first.Symbol + ".100ms"
	message := func(when time.Time, epoch uuid.UUID, kind string, prev, change int) {
		raw := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":%q,"data":{"type":%q,"instrument_name":%q,"timestamp":%d,"prev_change_id":%d,"change_id":%d,"bids":[["new",1,2]],"asks":[["new",2,3]]}}}`, channel, kind, first.Symbol, when.UnixMilli(), prev, change))
		handleTest(t, e, event{At: when, Group: "book", Stream: deribit.StreamEvent{Kind: "message", Channel: channel, Epoch: epoch, Raw: raw}})
	}
	stepTest(t, e, at, true)
	message(at.Add(100*time.Millisecond), oldEpoch, "change", 99, 100)
	stepTest(t, e, at.Add(time.Second), true)
	if resets != 1 {
		t.Fatal("sequence gap did not request rebuild")
	}
	connectedAt := at.Add(1100 * time.Millisecond)
	handleTest(t, e, event{At: connectedAt, Group: "book", Stream: deribit.StreamEvent{Kind: "connected", Epoch: newEpoch}})
	handleTest(t, e, event{At: connectedAt, Group: "book", Stream: deribit.StreamEvent{Kind: "ready", Epoch: newEpoch}})
	message(at.Add(1200*time.Millisecond), oldEpoch, "snapshot", 0, 200)
	stepTest(t, e, at.Add(2*time.Second), true)
	message(at.Add(2100*time.Millisecond), newEpoch, "snapshot", 0, 300)
	stepTest(t, e, at.Add(3*time.Second), true)
	o := e.metadata[first.InstrumentID].observation
	o.ObservedAt = at.Add(3100 * time.Millisecond)
	handleTest(t, e, event{At: o.ObservedAt, Metadata: &o})
	for sec := 4; sec < 60; sec++ {
		stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
	}
	b := (*batches)[0].Books[0]
	if !b.Quality[0].ReplayValid || b.Quality[1].ReplayValid || b.Quality[2].ReplayValid || b.Quality[3].ReplayValid || !b.Quality[4].ReplayValid || b.Quality[4].Epoch != newEpoch {
		t.Fatal("gap/epoch/metadata recovery became falsely valid")
	}
	if _, err := replay.ReplayDerivative(b, 4); err != nil {
		t.Fatal(err)
	}
}

func TestLiveMetadataAndIndexFailureIsolation(t *testing.T) {
	e, at, batches := engineFixture(t)
	stepTest(t, e, at, true)
	o := e.metadata[e.run.Members[0].InstrumentID].observation
	o.Status = "request_error"
	o.ObservedAt = at.Add(100 * time.Millisecond)
	handleTest(t, e, event{At: o.ObservedAt, Metadata: &o})
	handleTest(t, e, event{At: at.Add(200 * time.Millisecond), Group: "index", Stream: deribit.StreamEvent{Kind: "disconnected", Epoch: e.connections["index"].epoch}})
	for sec := 1; sec < 60; sec++ {
		stepTest(t, e, at.Add(time.Duration(sec)*time.Second), true)
	}
	b := (*batches)[0]
	if b.Books[0].Quality[1].ReplayValid || b.Books[0].Quality[1].Reason != model.DerivativeMetadataUncertain {
		t.Fatal("failed metadata retained a current state")
	}
	for _, book := range b.Books[1:] {
		if !book.Quality[59].ReplayValid {
			t.Fatal("unrelated metadata/index failure invalidated healthy book")
		}
	}
	if b.Indexes[0].Samples[1].Price != nil || b.Indexes[0].Samples[1].State != options.IndexDisconnected {
		t.Fatal("index disconnect retained price")
	}
}
func TestIngressOrdersLateMessagesAndBoundsQueue(t *testing.T) {
	at := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	q := newIngress(at.Add(-time.Millisecond))
	if err := q.offerLocked(at.Add(-time.Nanosecond), event{Group: "before"}); err != nil {
		t.Fatal(err)
	}
	if err := q.offerLocked(at.Add(time.Nanosecond), event{Group: "after"}); err != nil {
		t.Fatal(err)
	}
	a, b, c := <-q.queue, <-q.queue, <-q.queue
	if a.Group != "before" || !b.Boundary || !b.At.Equal(at) || c.Group != "after" {
		t.Fatal("boundary ordering broken")
	}
	for n := 0; n <= 256; n++ {
		_ = q.offerLocked(at.Add(2*time.Nanosecond), event{Group: "fill"})
	}
	if !q.failed {
		t.Fatal("overflow not reported")
	}
}
