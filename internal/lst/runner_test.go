package lst

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestFundingScheduleConfirmationDelayAndMissingBackoff(t *testing.T) {
	at := time.Unix(1790000000, 0).UTC()
	s := fundingSchedule{NextRegular: at.Add(time.Hour)}
	q := Quote{NextFundingTime: Ptr(at), MarkSourceTime: Ptr(at.Add(-time.Hour)), MarkPayloadHash: Ptr(strings.Repeat("h", 32))}
	s.observe(Batch{Quotes: []Quote{q, q}}, at)
	if len(s.Pending) != 1 || s.due(at.Add(2*time.Minute-time.Microsecond)) {
		t.Fatal("confirmation duplicate or before +2m")
	}
	if !s.due(at.Add(2 * time.Minute)) {
		t.Fatal("due confirmation missed")
	}
	missing := Batch{Capture: Capture{Status: "complete"}}
	s.result(at.Add(2*time.Minute), missing, nil)
	if !s.Pending[0].NextAttempt.Equal(at.Add(7 * time.Minute)) {
		t.Fatal("missing publication must wait 5m")
	}
	s.result(at.Add(7*time.Minute), missing, nil)
	if !s.Pending[0].NextAttempt.Equal(at.Add(22 * time.Minute)) {
		t.Fatal("second missing publication must wait 15m")
	}
	s.result(at.Add(22*time.Minute), missing, nil)
	if !s.Pending[0].NextAttempt.Equal(at.Add(82 * time.Minute)) {
		t.Fatal("third missing publication must wait 60m")
	}
	known := missing
	known.Funding = []FundingSettlement{{FundingTime: at.Add(time.Millisecond), SettlementMarkPriceTickE8: Ptr(int64(200000000000))}}
	s.result(at.Add(37*time.Minute), known, nil)
	if len(s.Pending) != 0 {
		t.Fatal("published actual settlement not confirmed")
	}
}

func TestFundingScheduleFailureRetryDoesNotBurstAndPreservesPeriodicQuery(t *testing.T) {
	now := time.Unix(1790000000, 0).UTC()
	s := fundingSchedule{}
	if !s.due(now) {
		t.Fatal("first periodic window missing")
	}
	failed := Batch{Capture: Capture{Status: "failed"}}
	for _, delay := range []time.Duration{5 * time.Minute, 15 * time.Minute, time.Hour} {
		s.result(now, failed, errors.New("source unavailable"))
		if s.due(now.Add(delay-time.Microsecond)) || !s.due(now.Add(delay)) {
			t.Fatal("incorrect source retry gate", delay)
		}
		now = now.Add(delay)
	}
	s.result(now, Batch{Capture: Capture{Status: "complete"}}, nil)
	if s.due(now.Add(14*time.Minute)) || !s.due(now.Add(15*time.Minute)) {
		t.Fatal("healthy periodic cadence changed")
	}
}

func TestRecoverHistoricalSkipsOnlyCompleteContiguousLatestRanges(t *testing.T) {
	now := Now().Truncate(time.Millisecond)
	p := historicalProgress{Manifest: strings.Repeat("m", 32), Days: 30, From: 100, To: 300, Next: 100, FundingFrom: now.Add(-30 * 24 * time.Hour), FundingTo: now}
	cap := func(from, to uint64) Capture {
		return Capture{CaptureId: uuid.New(), ManifestHash: p.Manifest, CaptureKind: "logs", Status: "complete", Committed: true, Canonical: true, Finality: "finalized", Revision: 1, FromBlock: Ptr(from), ToBlock: Ptr(to)}
	}
	a, z := cap(100, 149), cap(150, 199)
	gap := cap(201, 250)
	funding := Capture{CaptureId: uuid.New(), ManifestHash: p.Manifest, CaptureKind: "funding", Status: "complete", Canonical: true, Committed: true, Revision: 1, WindowFromAt: Ptr(p.FundingFrom), WindowToAt: Ptr(p.FundingTo)}
	recoverHistorical(&p, []Capture{gap, z, funding, a})
	if p.Next != 200 || !p.FundingDone {
		t.Fatal("committed cursor recovery failed", p.Next, p.FundingDone)
	}
	orphan := gap
	orphan.Revision = 2
	orphan.Canonical = false
	bridge := cap(200, 200)
	recoverHistorical(&p, []Capture{gap, orphan, bridge})
	if p.Next != 201 {
		t.Fatal("older canonical capture bypassed latest revision", p.Next)
	}
}

func runnerReadyCollector(t *testing.T, store Store, calls *int) *Collector {
	t.Helper()
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) {
		*calls++
		return nil, errors.New("unexpected_public_request")
	})
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	m.CodeHashes = map[string]string{}
	for _, key := range m.CodeRoles() {
		m.CodeHashes[key] = "0x" + strings.Repeat("11", 32)
	}
	m.Pools = map[string]string{"usdt_weth_500": "0x" + strings.Repeat("22", 20), "weth_wsteth_100": "0x" + strings.Repeat("33", 20)}
	cex, e := NewCEX(tr, "https://fapi.example")
	if e != nil {
		t.Fatal(e)
	}
	return &Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, CEX: cex, Manifest: m, Metadata: cexTestMetadata(), Store: store, StateDir: t.TempDir(), Archive: ethereum.Archive{Dir: t.TempDir()}}
}

func TestBackfillDaysMismatchAndCommitBeforeCursorCrashMakeNoRequests(t *testing.T) {
	day := Now().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	b := gasTestBatch(t, day)
	b.Requests = nil
	calls := 0
	store := &gasMemoryStore{}
	c := runnerReadyCollector(t, store, &calls)
	b.Capture.ManifestHash = c.Manifest.Hash
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	store.batches = []Batch{b}
	p := historicalProgress{Manifest: c.Manifest.Hash, Days: 7, From: 100, To: 120, Next: 100, RangeSize: 512, FundingDone: true}
	file := filepath.Join(c.StateDir, "backfill.gob")
	if e := writeGob(file, p); e != nil {
		t.Fatal(e)
	}
	if e := c.Backfill(context.Background(), 30, 1, nil); e == nil || e.Error() != "backfill_days_mismatch" || calls != 0 {
		t.Fatal("days mismatch fetched/replanned", e, calls)
	}
	p.Days = 30
	if e := writeGob(file, p); e != nil {
		t.Fatal(e)
	}
	if e := c.Backfill(context.Background(), 30, 1, nil); e != nil {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal("committed range was fetched after cursor crash")
	}
	if e := readGob(file, &p); e != nil || p.Next != 121 {
		t.Fatal("recovered cursor not persisted", p.Next, e)
	}
}

func TestMetadataRefreshKeepsInstrumentIdAndReplacesProofs(t *testing.T) {
	raw, e := os.ReadFile("../../research/2026-10-02-lst-design/binance-exchange-info.raw.json")
	if e != nil {
		t.Fatal(e)
	}
	changed := false
	tr, _ := transportFixture(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/fapi/v1/exchangeInfo" {
			body := string(raw)
			if changed {
				body = strings.Replace(body, `"onboardDate":1574840700000`, `"onboardDate":1574840700001`, 1)
			}
			return transportReply(200, body), nil
		}
		return transportReply(200, `[]`), nil
	})
	cex, _ := NewCEX(tr, "https://fapi.example")
	m := cexTestMetadata()
	m.ExchangeResponse.PayloadHash = "old-exchange"
	m.FundingInfoResponse.PayloadHash = "old-funding"
	c := Collector{CEX: cex, Metadata: m, IdentityResponses: []Response{{PayloadHash: "chain-proof"}, m.ExchangeResponse, m.FundingInfoResponse}}
	if e = c.refreshMetadata(context.Background()); e != nil {
		t.Fatal(e)
	}
	if c.Metadata.Instrument.ID != 42 || len(c.IdentityResponses) != 3 || c.IdentityResponses[0].PayloadHash != "chain-proof" || c.IdentityResponses[1].PayloadHash == "old-exchange" {
		t.Fatal("metadata id/proof changed incorrectly")
	}
	before := c.Metadata.Instrument
	changed = true
	if e = c.refreshMetadata(context.Background()); e == nil || e.Error() != "metadata_contract_identity_changed" || !c.Metadata.Instrument.SameDefinition(before) {
		t.Fatal("changed contract accepted", e)
	}
}

func TestQuietTaskCannotRunIntoNextMarketRound(t *testing.T) {
	start := Now().Add(-50 * time.Second)
	ctx, cancel := quietTask(context.Background(), start, 10*time.Second)
	defer cancel()
	d, ok := ctx.Deadline()
	if !ok || d.After(start.Add(55*time.Second)) {
		t.Fatal("maintenance extends market deadline")
	}
}
