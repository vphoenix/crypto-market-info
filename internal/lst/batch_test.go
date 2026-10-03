package lst

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func lstTestPtr[T any](v T) *T { return &v }
func lstBatchFixture() Batch {
	now := time.Date(2026, 10, 2, 1, 2, 3, 123000, time.UTC)
	h := string([]byte{0xff, 0x80}) + strings.Repeat("x", 30)
	c := Capture{CaptureId: uuid.New(), ManifestHash: h, CaptureKind: "market", CaptureMode: "live", SourceId: "fixture", StartedAt: now, AvailableAt: now, Finality: "head", Canonical: true, Status: "partial", ExpectedProtocolRows: 1, ExpectedQuoteRows: 1}
	p := ProtocolState{CaptureId: c.CaptureId, ObservedAt: now, AvailableAt: now, ChainId: 1, QueueAddress: strings.Repeat("q", 20), StateStatus: "unknown", Reason: "source_failed"}
	q := Quote{CaptureId: c.CaptureId, QuoteId: CanonicalHash("q"), ObservedAt: now, AvailableAt: now, QuoteRole: "entry", RouteId: "A", QuoteAssetAddress: strings.Repeat("u", 20), LstAddress: strings.Repeat("l", 20), PurchaseBudgetUsdtRaw: big.NewInt(100000000000), HedgeInstrumentId: 1, BuyStatus: "unknown", ConversionStatus: "unknown", ExitStatus: "unknown", HedgeStatus: "unknown", TimingStatus: "unknown"}
	return Batch{Capture: c, Protocols: []ProtocolState{p}, Quotes: []Quote{q}}
}
func TestLSTCanonicalHashPreservesBinaryAndNumericPrecision(t *testing.T) {
	// JSON replaces each byte with U+FFFD and aliases these distinct inputs.
	if CanonicalHash("\xff") == CanonicalHash("\xfe") {
		t.Fatal("invalid UTF-8 bytes collided")
	}
	if CanonicalHash([]string(nil)) != CanonicalHash([]string{}) {
		t.Fatal("ClickHouse empty-array roundtrip changed hash")
	}
	if CanonicalHash((*big.Int)(nil)) == CanonicalHash(big.NewInt(0)) {
		t.Fatal("unknown equals zero")
	}
	if CanonicalHash(decimal.RequireFromString("0.010000000000000000")) != CanonicalHash(decimal.RequireFromString("0.01")) {
		t.Fatal("Decimal database scale changed hash")
	}
	n := new(big.Int).Lsh(big.NewInt(1), 240)
	if CanonicalHash(n) == CanonicalHash(new(big.Int).Add(n, big.NewInt(1))) {
		t.Fatal("UInt256 precision lost")
	}
}
func TestLSTSealStableRetryAndFrozenMembership(t *testing.T) {
	b := lstBatchFixture()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	before := CanonicalHash(b)
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if before != CanonicalHash(b) {
		t.Fatal("sealing same batch changed identity")
	}
	b.Quotes[0].Reason = "tampered"
	if Validate(b) == nil {
		t.Fatal("tampered row accepted")
	}
	b = lstBatchFixture()
	b.Quotes = append(b.Quotes, b.Quotes[0])
	b.Capture.ExpectedQuoteRows++
	if b.Seal() == nil {
		t.Fatal("duplicate identity accepted")
	}
	b = lstBatchFixture()
	b.Capture.ExpectedQuoteRows = 2
	if b.Seal() == nil {
		t.Fatal("missing required member accepted")
	}
}
func TestLSTNullEvidenceAndRangeChecks(t *testing.T) {
	b := lstBatchFixture()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if b.Protocols[0].BlockNumber != nil || b.Quotes[0].ChainRequestedAt != nil {
		t.Fatal("failed source fabricated evidence")
	}
	b = lstBatchFixture()
	b.Quotes[0].PurchaseBudgetUsdtRaw = new(big.Int).Lsh(big.NewInt(1), 256)
	if b.Seal() == nil {
		t.Fatal("overflow accepted")
	}
	b = lstBatchFixture()
	b.Quotes[0].IndicatedFundingRate = lstTestPtr(decimal.RequireFromString("0.0000000000000000001"))
	if b.Seal() == nil {
		t.Fatal("decimal truncation accepted")
	}
	b = lstBatchFixture()
	b.Quotes[0].ObservedAt = b.Quotes[0].ObservedAt.Add(time.Nanosecond)
	if b.Seal() == nil {
		t.Fatal("submicrosecond time accepted")
	}
	b = lstBatchFixture()
	b.Quotes[0].ChainPayloadHashes = []string{"short"}
	if b.Seal() == nil {
		t.Fatal("short binary hash accepted")
	}
	b = lstBatchFixture()
	b.Protocols[0].StateStatus = "ok"
	if b.Seal() == nil {
		t.Fatal("success without chain evidence accepted")
	}
}
func TestLSTMembershipDigestIndependentOfQueryOrder(t *testing.T) {
	b := lstBatchFixture()
	q := b.Quotes[0]
	q.QuoteId = CanonicalHash("q2")
	b.Quotes = append(b.Quotes, q)
	b.Capture.ExpectedQuoteRows = 2
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	b.Quotes[0], b.Quotes[1] = b.Quotes[1], b.Quotes[0]
	if e := Validate(b); e != nil {
		t.Fatal(e)
	}
}
func TestLSTFundingNegativeAndZeroActualRate(t *testing.T) {
	b := lstBatchFixture()
	b.Protocols = nil
	b.Quotes = nil
	c := &b.Capture
	c.CaptureKind = "funding"
	c.Finality = "not_applicable"
	c.ExpectedProtocolRows = 0
	c.ExpectedQuoteRows = 0
	c.Status = "complete"
	c.WindowFromAt = lstTestPtr(c.StartedAt.Add(-time.Hour))
	c.WindowToAt = lstTestPtr(c.StartedAt)
	b.Funding = []FundingSettlement{{CaptureId: c.CaptureId, InstrumentId: 1, FundingTime: c.StartedAt, FundingRate: decimal.RequireFromString("-0.000000000000000001"), SettlementMarkPriceTickE8: lstTestPtr(int64(300000000001)), SourceId: c.SourceId, RequestedAt: c.StartedAt, ReceivedAt: c.StartedAt, AvailableAt: c.StartedAt, SourcePayloadHash: c.ManifestHash}}
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	b.Funding[0].FundingRate = decimal.Zero
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	b.Funding[0].FundingTime = c.StartedAt.Add(time.Second)
	if b.Seal() == nil {
		t.Fatal("funding outside requested window accepted")
	}
}
func TestLSTEmptyFinalizedLogRangeHasEvidence(t *testing.T) {
	b := lstBatchFixture()
	b.Protocols = nil
	b.Quotes = nil
	c := &b.Capture
	c.ExpectedProtocolRows = 0
	c.ExpectedQuoteRows = 0
	c.CaptureKind = "logs"
	c.Status = "complete"
	c.Finality = "finalized"
	c.ChainId = lstTestPtr(uint64(1))
	c.FromBlock = lstTestPtr(uint64(42))
	c.ToBlock = lstTestPtr(uint64(43))
	c.FromBlockHash = lstTestPtr(CanonicalHash("42"))
	c.ToBlockHash = lstTestPtr(CanonicalHash("43"))
	c.FromBlockTime = lstTestPtr(c.StartedAt)
	c.ToBlockTime = lstTestPtr(c.StartedAt)
	c.EvidenceRootHash = CanonicalHash([]string{"raw empty array evidence"})
	expected := c.EvidenceRootHash
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if c.EvidenceRootHash != expected {
		t.Fatal("empty log evidence overwritten")
	}
	c.Finality = "head"
	if b.Seal() == nil {
		t.Fatal("unfinalized logs accepted")
	}
}

func TestLSTSuccessfulSourceRequiresReceiveAndAvailability(t *testing.T) {
	for _, mutate := range []func(*Batch){
		func(b *Batch) { b.Protocols[0].ReceivedAt = nil; b.Protocols[0].SourceAvailableAt = nil },
		func(b *Batch) { b.Quotes[0].ChainReceivedAt = nil; b.Quotes[0].ChainAvailableAt = nil },
		func(b *Batch) { b.Quotes[0].HedgeDepthReceivedAt = nil; b.Quotes[0].HedgeDepthAvailableAt = nil },
		func(b *Batch) { b.Quotes[0].MarkPriceTickE8 = Ptr(int64(1)) },
	} {
		b := reportGoodMarket(t)
		mutate(&b)
		if b.Seal() == nil {
			t.Fatal("success accepted incomplete source evidence")
		}
	}
	b := lstBatchFixture()
	b.Quotes[0].ChainRequestedAt = Ptr(b.Capture.StartedAt)
	if e := b.Seal(); e != nil {
		t.Fatal("failed request-only evidence must remain valid", e)
	}
}

func TestLSTFinalizationInclusiveSingleRequestRange(t *testing.T) {
	b := reportLogFixture(t)
	if b.Finalizations[0].FromRequestId.Cmp(b.Finalizations[0].ToRequestId) != 0 {
		t.Fatal("fixture must contain one-request finalization")
	}
	if e := b.Seal(); e != nil {
		t.Fatal("single source request [id,id] rejected", e)
	}
	b.Finalizations[0].FromRequestId = big.NewInt(0)
	if b.Seal() == nil {
		t.Fatal("request id zero accepted")
	}
	b.Finalizations[0].FromRequestId = big.NewInt(2)
	if b.Seal() == nil {
		t.Fatal("reversed inclusive range accepted")
	}
}
