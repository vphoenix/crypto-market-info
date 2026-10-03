package justlendkeeper

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func liveFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, e := os.ReadFile("../../research/2026-10-03-keeper-implementation/" + name)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestCombinedResourceAvailabilityAfterAllComponents(t *testing.T) {
	c, store, now := testCollector(t)
	parameterAt := *now
	raw := liveFixture(t, "chain-parameters.raw")
	o := c.NewOperation("resources", "live", "publicnode")
	if e := c.Process(&o, Request{Role: "parameters"}, Evidence{ResponseHash: Hex(Hash(raw)), Started: parameterAt, Received: &parameterAt, Available: parameterAt}, raw); e != nil {
		t.Fatal(e)
	}
	o.Batch.Costs[0].ContractUserResourcePercent = Ptr(uint8(10))
	o.Batch.Costs[0].ContractOriginEnergyLimit = Ptr(uint64(90000))
	o.Headers = []Header{{Number: 1, Hash: Hash([]byte("before"))}, {Number: 2, Hash: Hash([]byte("after"))}}
	*now = now.Add(20 * time.Second)
	if e := c.Finish(context.Background(), &o); e != nil {
		t.Fatal(e)
	}
	r := store.Batches[0].Costs[0]
	if !r.AvailableAt.Equal(*now) || !r.AvailableAt.Equal(store.Batches[0].Capture.AvailableAt) || r.SourceTime == nil || !r.SourceTime.Equal(parameterAt) || r.ReceivedAt == nil || !r.ReceivedAt.Equal(parameterAt) {
		t.Fatal("combined context was available before completion", r)
	}
}

func TestResponseGapAbsorbsVariableStateWriteTime(t *testing.T) {
	c, _, now := testCollector(t)
	*now = now.Add(5 * time.Second)
	l := c.API.Limiter
	before := c.State.Used
	if e := l.Reserve("publicnode", true); e != nil {
		t.Fatal(e)
	}
	// A slow fsync plus wire latency occurs after the persisted reservation.
	*now = now.Add(350 * time.Millisecond)
	l.ExtendGap("publicnode", true, *now)
	if !l.Ready("publicnode", true).Equal(now.Add(5*time.Second)) || c.State.Used != before+1 {
		t.Fatal("response gap/budget failed")
	}
	if e := l.Status("publicnode", 200, 0); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(5*time.Second - time.Millisecond)
	if e := l.Reserve("publicnode", true); e == nil {
		t.Fatal("fsync shrank actual attempt spacing")
	}
}

func TestResponseGapKeepsWarmupReservationAcrossBoundary(t *testing.T) {
	c, _, now := testCollector(t)
	l := c.API.Limiter
	*now = now.Add(5*time.Minute - time.Millisecond)
	if e := l.Reserve("publicnode", false); e != nil {
		t.Fatal(e)
	}
	used := c.State.Used
	*now = now.Add(100 * time.Millisecond)
	l.ExtendGap("publicnode", false, *now)
	if !l.Ready("publicnode", false).Equal(now.Add(5 * time.Second)) {
		t.Fatal("warmup boundary shortened reserved gap")
	}
	if e := l.Status("publicnode", 429, 10*time.Minute); e != nil {
		t.Fatal(e)
	}
	oldCooldown := c.State.Sources["publicnode"].Cooldown
	l.ExtendGap("publicnode", false, *now)
	if c.State.Used != used || !c.State.Sources["publicnode"].Cooldown.Equal(oldCooldown) {
		t.Fatal("response extended budget or reset cooldown")
	}
	s, e := LoadState(c.Path, "test", c.Config)
	if e != nil || s.Used != used || !s.NextGlobal.Equal(now.Add(5*time.Second)) || !s.Sources["publicnode"].Cooldown.Equal(oldCooldown) {
		t.Fatal("persisted response gap differs", e)
	}
}

type failingTransport struct{ now *time.Time }

func (t failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	*t.now = t.now.Add(350 * time.Millisecond)
	return nil, errors.New("simulated transport failure")
}
func TestTransportFailurePersistsExtendedGapOnce(t *testing.T) {
	c, _, now := testCollector(t)
	c.API.HTTP.Transport = failingTransport{now}
	*now = now.Add(5 * time.Second)
	ev, _, e := c.API.Send(context.Background(), headRequest(false, "test", true))
	if e != nil || ev.Error != "transport_error" {
		t.Fatal(e, ev)
	}
	s, e := LoadState(c.Path, "test", c.Config)
	if e != nil || s.Used != 1 || !s.NextGlobal.Equal(ev.Available.Add(5*time.Second)) || !s.NextBackground.Equal(ev.Available.Add(5*time.Second)) {
		t.Fatal("failed attempt spacing/budget not persisted", e, s.Used)
	}
}

func TestChainParametersSignedValuesAndRequiredPrices(t *testing.T) {
	b := liveFixture(t, "chain-parameters.raw")
	now := time.Now().UTC()
	r, e := ParseParameters(b, Evidence{ResponseHash: Hex(Hash(b)), Available: now})
	if e != nil || r.EnergyFeeSunPerUnit == nil || *r.EnergyFeeSunPerUnit != 100 || r.BandwidthFeeSunPerByte == nil || *r.BandwidthFeeSunPerByte != 1000 {
		t.Fatal(e, r)
	}
	for _, raw := range []string{
		`{"chainParameter":[{"key":"getEnergyFee","value":-1},{"key":"getTransactionFee","value":1000}]}`,
		`{"chainParameter":[{"key":"getEnergyFee"},{"key":"getTransactionFee","value":1000}]}`,
		`{"chainParameter":[{"key":"getEnergyFee","value":100},{"key":"getTransactionFee","value":0}]}`,
		`{"chainParameter":[{"key":"getEnergyFee","value":100},{"key":"getTransactionFee","value":1000},{"key":"unrelated","value":1.1}]}`,
		`{"chainParameter":[{"key":"getEnergyFee","value":100},{"key":"getTransactionFee","value":1000},{"key":"unrelated","value":9223372036854775808}]}`,
	} {
		if _, e = ParseParameters([]byte(raw), Evidence{}); e == nil {
			t.Fatal("invalid parameter accepted", raw)
		}
	}
}

func TestQuoteFixedScaleAndEquivalentRepresentationDigest(t *testing.T) {
	c, _, now := testCollector(t)
	raw := liveFixture(t, "quote.raw")
	h, e := c.API.Archive.Put(raw)
	if e != nil {
		t.Fatal(e)
	}
	o := c.NewOperation("quote", "live", "binance")
	if e = c.Process(&o, Request{Role: "quote"}, Evidence{ResponseHash: Hex(h), Started: *now, Available: *now, Received: Ptr(*now)}, raw); e != nil {
		t.Fatal(e)
	}
	row := o.Batch.Costs[0]
	for _, d := range []*decimal.Decimal{row.BidPriceUsdt, row.BidQtyTrx, row.AskPriceUsdt, row.AskQtyTrx} {
		if d.Exponent() != -18 {
			t.Fatal("noncanonical output", d.Exponent())
		}
	}
	legacy := row
	for _, field := range []**decimal.Decimal{&legacy.BidPriceUsdt, &legacy.BidQtyTrx, &legacy.AskPriceUsdt, &legacy.AskQtyTrx} {
		v := decimal.NewFromBigInt((*field).Shift(8).BigInt(), -8)
		*field = &v
	}
	o.Batch.Costs = []CostObservation{legacy}
	o.Batch.Capture.AvailableAt = *now
	o.Batch.Capture.EvidenceManifestHash = Hash([]byte("manifest"))
	Seal(&o.Batch)
	originalDigest := o.Batch.Capture.CostDigest
	readBack := o.Batch
	readBack.Costs = []CostObservation{row}
	if e = Validate(readBack); e != nil {
		t.Fatal(e)
	}
	if readBack.Capture.CostDigest != originalDigest || digest(readBack.Costs) != originalDigest {
		t.Fatal("equivalent scale changed canonical digest")
	}
	bad := readBack
	bad.Costs = append([]CostObservation(nil), readBack.Costs...)
	v := bad.Costs[0].BidPriceUsdt.Add(decimal.New(1, -18))
	bad.Costs[0].BidPriceUsdt = &v
	if e = Validate(bad); e == nil {
		t.Fatal("changed amount accepted by compatibility")
	}
	bad.Costs = append([]CostObservation(nil), readBack.Costs...)
	bad.Costs[0].Symbol = "OTHER"
	if e = Validate(bad); e == nil {
		t.Fatal("changed nondecimal field accepted by compatibility")
	}
	bad = readBack
	bad.Capture.EventDigest = Hash([]byte("changed"))
	if e = Validate(bad); e == nil {
		t.Fatal("other table digest bypassed by compatibility")
	}
}
