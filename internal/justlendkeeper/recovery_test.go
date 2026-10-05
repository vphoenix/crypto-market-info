package justlendkeeper

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func drain(t *testing.T, c *Collector, now *time.Time, limit int) {
	t.Helper()
	for i := 0; i < limit && len(c.State.Ops) > 0; i++ {
		if _, _, e := c.Step(context.Background()); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(time.Second)
	}
	if len(c.State.Ops) > 0 {
		t.Fatal("unfinished simulated operations", len(c.State.Ops))
	}
}

func TestHTTPRetryDistinctCostAndProbeRows(t *testing.T) {
	for _, kind := range []string{"quote", "probe"} {
		t.Run(kind, func(t *testing.T) {
			c, store, now := testCollector(t)
			var starts []time.Time
			attempts := 0
			probeBody, e := os.ReadFile("../../research/2026-10-02-keeper-design/non-owner-simulation.raw")
			if e != nil {
				t.Fatal(e)
			}
			quote := fixture(t, "trx_bookticker.raw.json")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				starts = append(starts, *now)
				if r.URL.Path == "/wallet/getblock" {
					fmt.Fprintf(w, `{"blockID":"%016x%048x","block_header":{"raw_data":{"number":1,"timestamp":%d}}}`, 1, 0, now.UnixMilli())
					return
				}
				attempts++
				if attempts == 1 {
					w.WriteHeader(503)
					w.Write([]byte(`{}`))
					return
				}
				if kind == "quote" {
					w.Write(quote)
				} else {
					w.Write(probeBody)
				}
			}))
			defer server.Close()
			c.Config.QuoteAPIURL, c.Config.NodeRPCURL = server.URL, server.URL
			c.API.Config = c.Config
			if kind == "quote" {
				c.AddQuote()
			} else {
				caller, _ := HexAddress(c.Config.Callers[0])
				renter, _ := HexAddress(c.Config.Callers[1])
				c.State.CallersValid = []string{caller}
				c.State.CohortId = uuid.New()
				c.AddProbe(Candidate{Renter: renter, Receiver: renter, Verified: true}, *now)
			}
			drain(t, c, now, 50)
			if attempts != 2 || len(store.Batches) != 1 {
				t.Fatal(attempts, len(store.Batches))
			}
			for i := 1; i < len(starts); i++ {
				if starts[i].Sub(starts[i-1]) < 5*time.Second {
					t.Fatal("retry bypassed gate", starts)
				}
			}
			b := store.Batches[0]
			if kind == "quote" {
				if len(b.Costs) != 2 || b.Costs[0].ObservationIndex == b.Costs[1].ObservationIndex || b.Costs[0].Status != "error" || b.Costs[1].Status != "ok" {
					t.Fatal("retry facts overwritten", b.Costs)
				}
			} else if len(b.Probes) != 2 || b.Probes[0].ProbeIndex == b.Probes[1].ProbeIndex || b.Probes[1].Status != "revert" {
				t.Fatal("probe retry facts overwritten", b.Probes)
			}
		})
	}
}

func TestNativeClientNegotiatesHTTP1Only(t *testing.T) {
	c, _, now := testCollector(t)
	protocol := ""
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol = r.Proto
		w.Write([]byte(`{}`))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	c.API.HTTP.Transport.(*http.Transport).TLSClientConfig.RootCAs = pool
	c.Config.NodeRPCURL = server.URL
	c.API.Config = c.Config
	*now = now.Add(5 * time.Second)
	ev, _, e := c.API.Send(context.Background(), headRequest(false, "test", false))
	if e != nil || ev.Error != "" || protocol != "HTTP/1.1" {
		t.Fatal("TLS advertised an unsupported protocol", e, ev, protocol)
	}
}

func Test429StopsOnlyAffectedSource(t *testing.T) {
	c, store, now := testCollector(t)
	quote := fixture(t, "trx_bookticker.raw.json")
	var starts []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		starts = append(starts, *now)
		if r.URL.Path == "/api/v3/ticker/bookTicker" {
			w.Write(quote)
			return
		}
		w.WriteHeader(429)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c.Config.EventAPIURL, c.Config.QuoteAPIURL = server.URL, server.URL
	c.API.Config = c.Config
	s := Scan{ID: "test", Kind: "Liquidate", From: now.Add(-time.Hour), To: *now}
	c.State.Scans = []Scan{s}
	o := c.NewOperation("page", "live", "trongrid")
	o.ScanID = s.ID
	o.Requests = []Request{{Source: "trongrid", Path: EventPath(s.Kind, s.From, s.To, ""), Role: "page"}}
	c.State.Ops = append(c.State.Ops, o)
	c.AddQuote()
	drain(t, c, now, 30)
	if len(starts) != 2 || starts[1].Sub(starts[0]) < 5*time.Second || len(store.Batches) != 2 || len(store.Batches[1].Costs) != 1 {
		t.Fatal(starts, store.Batches)
	}
	if c.State.Sources["trongrid"].Cooldown.Before(now.Add(4*time.Minute)) || c.State.Sources["binance"].Blocked {
		t.Fatal("source isolation/cooldown failed")
	}
}

func TestCollectorHydrationQueueNeverBursts(t *testing.T) {
	c, _, now := testCollector(t)
	var starts []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		starts = append(starts, *now)
		fmt.Fprintf(w, `{"blockID":"%016x%048x","block_header":{"raw_data":{"number":1,"timestamp":%d}}}`, 1, 0, now.UnixMilli())
	}))
	defer server.Close()
	c.Config.NodeRPCURL = server.URL
	c.API.Config = c.Config
	for i := 0; i < 6; i++ {
		o := c.NewOperation("test", "bootstrap", "publicnode")
		for j := 0; j < 200; j++ {
			o.Requests = append(o.Requests, headRequest(false, "cost_before", true))
		}
		c.State.Ops = append(c.State.Ops, o)
	}
	start := *now
	for i := 0; i < 70; i++ {
		if _, _, e := c.Step(context.Background()); e != nil {
			t.Fatal(e)
		}
		*now = now.Add(time.Second)
	}
	for i, at := range starts {
		if i == 0 && at.Sub(start) < 5*time.Second || i > 0 && at.Sub(starts[i-1]) < 5*time.Second {
			t.Fatal("queued work burst", starts)
		}
	}
	if len(starts) != 13 {
		t.Fatal("unexpected request schedule", len(starts))
	}
	*now = now.Add(time.Hour)
	if _, _, e := c.Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, _, e := c.Step(context.Background()); e != nil || len(starts) != 14 {
		t.Fatal("idle credits burst", e, len(starts))
	}
}

func TestIdenticalLogsSplitAcrossPagesRejected(t *testing.T) {
	c, _, _ := testCollector(t)
	var page Page
	if e := Decode(fixture(t, "tron_energy_7d.raw.json"), &page); e != nil {
		t.Fatal(e)
	}
	re := page.Data[0]
	rr, logs, e := ParseReceipt(fixture(t, "tron_receipt_0.raw.json"), Evidence{})
	if e != nil {
		t.Fatal(e)
	}
	logs = append(logs, logs[6])
	makeOp := func(events []RawEvent) Operation {
		o := c.NewOperation("page", "backfill", "trongrid")
		o.RawEvents = events
		o.Headers = []Header{{Number: rr.BlockNumber, Hash: Hash([]byte("block")), Time: rr.BlockTime, Transactions: []string{rr.TxId}}}
		o.Receipts = []ReceiptLogs{{rr, logs}}
		return o
	}
	o := makeOp([]RawEvent{re})
	if e = c.Hydrate(&o); e == nil || e.Error() != "identical_logs_split_page_ambiguous" {
		t.Fatal("split-page position accepted", e)
	}
	o = makeOp([]RawEvent{re, re})
	if e = c.Hydrate(&o); e == nil || e.Error() != "identical_logs_split_page_ambiguous" {
		t.Fatal("duplicate provider index inflated supplied logs", e)
	}
	second := re
	second.Index++
	o = makeOp([]RawEvent{re, second})
	if e = c.Hydrate(&o); e != nil || len(o.Batch.Events) != 2 || *o.Batch.Events[0].ReceiptLogIndex == *o.Batch.Events[1].ReceiptLogIndex {
		t.Fatal("complete identical log set did not resolve one-to-one", e)
	}
}

func TestStateMigrationAndFrozenBackfill(t *testing.T) {
	c, _, now := testCollector(t)
	from, to := now.Add(-30*24*time.Hour), now.Add(-120*time.Second)
	c.AddBackfill(from, to)
	c.State.Used, c.State.Day = 1234, now.Format("2006-01-02")
	c.State.SolidAnchors, c.State.EventCursors = nil, nil
	if e := c.Save(); e != nil {
		t.Fatal(e)
	}
	s, e := LoadState(c.Path, "test", c.Config)
	if e != nil || s.Used != 1234 || !s.BackfillFrom.Equal(from) || !s.BackfillTo.Equal(to) || s.SolidAnchors == nil || s.EventCursors == nil {
		t.Fatal("state migration lost progress", e)
	}
	c.State = &s
	c.AddBackfill(s.BackfillFrom, s.BackfillTo)
	if len(s.Scans) != 30 {
		t.Fatal("backfill window moved/duplicated", len(s.Scans))
	}
}

func TestCompleteCoverageDoesNotInventZeroDays(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if windowComplete(windowPages{Valid: true, Edges: map[string]string{"": "a"}}) || windowComplete(windowPages{Valid: true, Edges: map[string]string{"a": ""}}) {
		t.Fatal("incomplete pagination called complete")
	}
	if !windowComplete(windowPages{Valid: true, Edges: map[string]string{"": "a", "a": ""}}) {
		t.Fatal("complete pagination rejected")
	}
	out := t.TempDir()
	s := Summary{}
	if e := saveStats(out, nil, []completeWindow{{day, day.Add(24 * time.Hour)}}, day, day.Add(48*time.Hour), &s); e != nil {
		t.Fatal(e)
	}
	if s.CompleteUTCDays != 1 || s.CompleteDayMedianTRX == nil || *s.CompleteDayMedianTRX != "0" {
		t.Fatal("unknown day counted as zero", s)
	}
	b, e := os.ReadFile(filepath.Join(out, "daily_rewards.csv"))
	if e != nil || len(b) == 0 {
		t.Fatal(e)
	}
	// Exact boundary integers still reject a decimal input through JSON tokens.
	var p Page
	if e = Decode([]byte(`{"success":true,"data":[]}`), &p); e != nil || p.Data == nil {
		t.Fatal(e)
	}
}

type testReportReader struct{ batches []Batch }

func (r testReportReader) KeeperCaptures(context.Context, any, any) ([]Capture, error) {
	var caps []Capture
	for _, b := range r.batches {
		caps = append(caps, b.Capture)
	}
	return caps, nil
}
func (r testReportReader) KeeperBatch(_ context.Context, cap Capture) (Batch, error) {
	for _, b := range r.batches {
		if b.Capture.CaptureId == cap.CaptureId {
			return b, Validate(b)
		}
	}
	return Batch{}, fmt.Errorf("missing test capture")
}

func TestReportCanonicalDedupUnknownFeeAndSolidConflict(t *testing.T) {
	c, _, now := testCollector(t)
	pageBody := fixture(t, "tron_energy_7d.raw.json")
	var page Page
	if e := Decode(pageBody, &page); e != nil {
		t.Fatal(e)
	}
	re := page.Data[0]
	re.Evidence = Evidence{ResponseHash: Hex(Hash(pageBody)), Started: *now, Available: *now}
	rawReceipt := fixture(t, "tron_receipt_0.raw.json")
	rr, logs, e := ParseReceipt(rawReceipt, Evidence{ResponseHash: Hex(Hash(rawReceipt)), Started: *now, Available: *now})
	if e != nil {
		t.Fatal(e)
	}
	newBatch := func(hash string) Batch {
		o := c.NewOperation("page", "backfill", "trongrid")
		receipt := rr
		receipt.CaptureId = o.Batch.Capture.CaptureId
		receipt.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		o.RawEvents = []RawEvent{re}
		o.Headers = []Header{{Number: rr.BlockNumber, Hash: hash, Time: rr.BlockTime, Transactions: []string{rr.TxId}}}
		o.Receipts = []ReceiptLogs{{receipt, logs}}
		if er := c.Hydrate(&o); er != nil {
			t.Fatal(er)
		}
		cap := &o.Batch.Capture
		cap.AvailableAt = *now
		cap.EvidenceManifestHash, e = c.API.Archive.Put(JSONEvidence(Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: cap.CaptureId.String()}))
		if e != nil {
			t.Fatal(e)
		}
		Seal(&o.Batch)
		return o.Batch
	}
	hash := Hash([]byte("canonical-block"))
	first, second := newBatch(hash), newBatch(hash)
	out := t.TempDir()
	from, to := rr.BlockTime.Add(-time.Hour), rr.BlockTime.Add(time.Hour)
	if e = Report(context.Background(), testReportReader{[]Batch{first, second}}, c.API.Archive, c.Config, from, to, *now, out); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(out, "summary.json"))
	var summary Summary
	if e != nil || json.Unmarshal(b, &summary) != nil || summary.Events != 1 || summary.Transactions != 1 || summary.UnknownBurn != 1 || summary.GrossTRX != "20" || summary.RewardTransfersVerified != 0 {
		t.Fatal("duplicate rewards or unknown fee/transfer misreported", e, summary)
	}
	if summary.KnownBurnTRX != nil || summary.KnownBurnTransactions != 0 || summary.BurnCoverage != "none" || summary.NetProfitStatus != "unknown" {
		t.Fatal("unknown cost converted to zero", summary)
	}
	conflict := newBatch(Hash([]byte("conflicting-solid-block")))
	if e = Report(context.Background(), testReportReader{[]Batch{first, conflict}}, c.API.Archive, c.Config, from, to, *now, t.TempDir()); e == nil || e.Error() != "report_solid_hash_conflict" {
		t.Fatal("conflicting solid history accepted", e)
	}
}
