package justlendkeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	p := filepath.Join("..", "..", "research", "2026-10-02-opportunity-debate", "proposer", name)
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestRealArchivedLiquidationAndReceipt(t *testing.T) {
	var page Page
	b := fixture(t, "tron_energy_7d.raw.json")
	if e := Decode(b, &page); e != nil {
		t.Fatal(e)
	}
	re := page.Data[0]
	re.Evidence = Evidence{Started: time.Now().UTC(), Available: time.Now().UTC(), ResponseHash: Hex(Hash(b))}
	ev, e := EventRow(re)
	if e != nil {
		t.Fatal(e)
	}
	if len(ev.Renter) != 21 || ev.RewardSun.String() != "20000000" {
		t.Fatal(ev)
	}
	raw := fixture(t, "tron_receipt_0.raw.json")
	rr, logs, e := ParseReceipt(raw, Evidence{ResponseHash: Hex(Hash(raw)), Started: time.Now(), Available: time.Now()})
	if e != nil {
		t.Fatal(e)
	}
	if rr.FeeSun != nil || rr.EnergyUsageTotal == nil || *rr.EnergyUsageTotal != 219372 {
		t.Fatal("absent fee must remain unknown", rr)
	}
	found := -1
	for i, l := range logs {
		d, e := DecodeLog(l)
		if e == nil && EventEqual(ev, d) {
			found = i
		}
	}
	if found != 6 {
		t.Fatal("wrong receipt log index", found)
	}
	sum := big.NewInt(0)
	for _, tr := range rr.NativeTransfers {
		if !tr.Rejected && tr.Sender == ev.ContractAddress && tr.Recipient == *ev.Liquidator {
			sum.Add(sum, &tr.AmountSun)
		}
	}
	if sum.Cmp(ev.RewardSun) == 0 {
		t.Fatal("mixed helper transfers must not be treated as reward-only")
	}
	exact := false
	for _, tr := range rr.NativeTransfers {
		if !tr.Rejected && tr.Sender == ev.ContractAddress && tr.Recipient == *ev.Liquidator && tr.AmountSun.Cmp(ev.RewardSun) == 0 {
			exact = true
		}
	}
	if !exact {
		t.Fatal("archived reward transfer missing")
	}
	if _, e = Address("0x66fff827c2d1d2804881e8e74dd465186126f801"); e != nil {
		t.Fatal(e)
	}
}
func TestStrictNumbersAndJSON(t *testing.T) {
	for _, s := range []string{`1.1`, `"1e6"`, `-1`, `"01"`, `null`, `"` + new(big.Int).Lsh(big.NewInt(1), 256).String() + `"`} {
		if _, e := Uint([]byte(s)); e == nil {
			t.Fatal("accepted", s)
		}
	}
	n := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	v, e := Uint([]byte(`"` + n.String() + `"`))
	if e != nil || v.Cmp(n) != 0 {
		t.Fatal(e)
	}
	for _, s := range []string{`{"fee":1,"fee":2}`, `{"x":{"a":1,"a":2}}`, `{} {}`} {
		var v any
		if e := Decode([]byte(s), &v); e == nil {
			t.Fatal("accepted", s)
		}
	}
}
func TestArchivedAPISuccessTVMFailure(t *testing.T) {
	b, e := os.ReadFile("../../research/2026-10-02-keeper-design/non-owner-simulation.raw")
	if e != nil {
		t.Fatal(e)
	}
	cfg := DefaultConfig()
	s := NewState("test", cfg)
	s.IdentityAt = time.Now()
	s.IdentityStatus = "verified_manifest"
	c := &Collector{Config: cfg, State: &s}
	a, _ := Address("0x66fff827c2d1d2804881e8e74dd465186126f801")
	caller, _ := HexAddress(cfg.Callers[0])
	p, e := ParseProbe(b, Evidence{Available: time.Now(), ResponseHash: Hex(Hash(b))}, Candidate{Renter: a, Receiver: a}, caller, time.Now(), c)
	if e != nil || p.Status != "revert" || p.ApiSuccess == nil || !*p.ApiSuccess || p.RewardReturnSun != nil {
		t.Fatal(e, p.Status)
	}
}
func TestLimiterWarmupRestartIdleAndBudget(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	s := NewState("test", cfg)
	path := StateFile(t.TempDir())
	save := func() error { return SaveState(path, s) }
	l := &Limiter{State: &s, Save: save, Now: func() time.Time { return now }, Started: now, Budget: 40000}
	if e := l.Reserve("publicnode", false); e == nil {
		t.Fatal("first request must wait")
	}
	for i := 0; i < 12; i++ {
		now = now.Add(5 * time.Second)
		if e := l.Reserve("publicnode", false); e != nil {
			t.Fatal(e)
		}
		if e := l.Reserve("trongrid", false); e == nil {
			t.Fatal("simultaneous send")
		}
	}
	if s.Used != 12 {
		t.Fatal(s.Used)
	}
	if e := l.Status("publicnode", 429, 30*time.Minute); e != nil {
		t.Fatal(e)
	}
	restored, e := LoadState(path, "test", cfg)
	if e != nil {
		t.Fatal(e)
	}
	s = restored
	old := s.Used
	l.Started = now
	if !l.Ready("publicnode", false).Equal(now.Add(30 * time.Minute)) {
		t.Fatal("lost cooldown")
	}
	now = now.Add(time.Hour)
	if e = l.Reserve("publicnode", false); e != nil {
		t.Fatal(e)
	}
	if e = l.Reserve("publicnode", false); e == nil {
		t.Fatal("idle accumulated burst credits")
	}
	if s.Used != old+1 {
		t.Fatal("budget reset")
	}
	s.Used = 40000
	next := l.Ready("binance", false)
	if next.Hour() != 0 || next.Day() != 4 {
		t.Fatal(next)
	}
}

type memoryStore struct {
	Batches []Batch
	Fail    bool
}

func (s *memoryStore) WriteKeeperBatch(_ context.Context, b Batch) error {
	if e := Validate(b); e != nil {
		return e
	}
	if s.Fail {
		return errors.New("db_failure")
	}
	s.Batches = append(s.Batches, b)
	return nil
}
func testCollector(t *testing.T) (*Collector, *memoryStore, *time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	s := NewState("test", cfg)
	store := &memoryStore{}
	dir := t.TempDir()
	c := NewCollector(cfg, &s, StateFile(dir), Archive{Dir: filepath.Join(dir, "evidence")}, store)
	c.Now = func() time.Time { return now }
	c.API.Now = c.Now
	c.API.Limiter.Now = c.Now
	c.API.Limiter.Started = now
	c.API.Limiter.Save = c.Save
	return c, store, &now
}
func TestHeadersPersistRateLimitBeforeBodyFailure(t *testing.T) {
	c, _, now := testCollector(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1200")
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(429)
		w.Write([]byte("short"))
	}))
	defer server.Close()
	c.Config.EventAPIURL = server.URL
	c.API.Config = c.Config
	*now = now.Add(5 * time.Second)
	ev, _, e := c.API.Send(context.Background(), Request{Source: "trongrid", Path: EventPath("Liquidate", now.Add(-time.Hour), *now, "")})
	if e != nil || ev.Status != 429 || ev.Error != "response_read_error" {
		t.Fatal(e, ev)
	}
	if !c.State.Sources["trongrid"].Cooldown.Equal(now.Add(20 * time.Minute)) {
		t.Fatal("cooldown missing")
	}
	raw, e := os.ReadFile(c.Path)
	if e != nil || len(raw) < 32 {
		t.Fatal(e)
	}
}
func TestNoRedirectAndWriteEndpoints(t *testing.T) {
	c, _, now := testCollector(t)
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; http.Redirect(w, r, "/second", 302) }))
	defer server.Close()
	c.Config.NodeRPCURL = server.URL
	c.API.Config = c.Config
	*now = now.Add(5 * time.Second)
	ev, _, e := c.API.Send(context.Background(), headRequest(false, "test", false))
	if e != nil || ev.Status != 302 || hits != 1 {
		t.Fatal(e, ev, hits)
	}
	if _, _, e = c.API.Send(context.Background(), Post("publicnode", "/wallet/broadcasttransaction", "test", "", map[string]any{}, false)); e == nil {
		t.Fatal("write method allowed")
	}
}
func TestCaptureDigestFrozenRetryAndStateCorruption(t *testing.T) {
	c, store, now := testCollector(t)
	o := c.NewOperation("quote", "live", "binance")
	body := fixture(t, "trx_bookticker.raw.json")
	ev := Evidence{Source: "binance", Started: *now, Received: Ptr(*now), Available: *now, ResponseHash: Hex(Hash(body))}
	if _, e := c.API.Archive.Put(body); e != nil {
		t.Fatal(e)
	}
	if e := c.Process(&o, Request{Role: "quote"}, ev, body); e != nil {
		t.Fatal(e)
	}
	c.State.Ops = []Operation{o}
	store.Fail = true
	if _, _, e := c.Step(context.Background()); e == nil {
		t.Fatal("expected db failure")
	}
	frozen, _ := Freeze(c.State.Ops[0].Batch)
	*now = now.Add(10 * time.Second)
	store.Fail = false
	if _, _, e := c.Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	got, _ := Freeze(store.Batches[0])
	if string(got) != string(frozen) {
		t.Fatal("retry mutated frozen batch")
	}
	b := store.Batches[0]
	b.Costs[0].Symbol = "BAD"
	if Validate(b) == nil {
		t.Fatal("digest ignored mutation")
	}
	if e := os.WriteFile(c.Path, []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := LoadState(c.Path, "test", c.Config); e == nil {
		t.Fatal("corrupt state accepted")
	}
}
func TestStaleProbeAndSameBlockAmbiguity(t *testing.T) {
	c, store, now := testCollector(t)
	var p Page
	if e := Decode(fixture(t, "tron_energy_7d.raw.json"), &p); e != nil {
		t.Fatal(e)
	}
	first := p.Data[0]
	first.Name = "RentResource"
	first.Result["addedAmount"] = json.RawMessage(`"1"`)
	first.Result["addedSecurityDeposit"] = json.RawMessage(`"1"`)
	c.AddCandidates([]RawEvent{first})
	second := first
	second.Transaction = strings.Repeat("1", 64)
	c.AddCandidates([]RawEvent{second})
	if len(c.State.Candidates) != 1 || !c.State.Candidates[0].Ambiguous {
		t.Fatal("same block not ambiguous")
	}
	can := c.State.Candidates[0]
	c.AddSeed(can)
	if len(c.State.Ops[0].Requests) != 0 {
		t.Fatal("ambiguous seed hydrated as verified")
	}
	c.State.Ops = nil
	can.Ambiguous = false
	can.Verified = true
	caller, _ := HexAddress(c.Config.Callers[0])
	c.State.CallersValid = []string{caller}
	c.State.CohortId = uuid.New()
	c.AddProbe(can, now.Add(-time.Minute))
	if _, _, e := c.Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	if c.State.Used != 0 {
		t.Fatal("sent stale probe")
	}
	if _, _, e := c.Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(store.Batches) != 1 || store.Batches[0].Capture.Status != "skipped" {
		t.Fatal("missing skipped capture")
	}
}
func TestPaginationGuard(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now().UTC()
	s := Scan{Kind: "Liquidate", From: now.Add(-time.Hour), To: now}
	path := EventPath(s.Kind, s.From, s.To, "abc")
	body := fmt.Sprintf(`{"success":true,"data":[],"meta":{"links":{"next":%q}}}`, cfg.EventAPIURL+path)
	_, next, e := ParsePage([]byte(body), Evidence{}, cfg, s)
	if e != nil || next != "abc" {
		t.Fatal(e, next)
	}
	for _, u := range []string{"https://other.invalid" + path, cfg.EventAPIURL + strings.ReplaceAll(path, "only_confirmed=true", "only_confirmed=false")} {
		bad := fmt.Sprintf(`{"success":true,"data":[],"meta":{"links":{"next":%q}}}`, u)
		if _, _, e := ParsePage([]byte(bad), Evidence{}, cfg, s); e == nil {
			t.Fatal("unsafe next page")
		}
	}
}
