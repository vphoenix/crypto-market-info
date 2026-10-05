package justlendkeeper

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSavedBoundaryPagesAndHalfOpenEnvelope(t *testing.T) {
	a := Archive{Dir: "../../research/2026-10-03-keeper-repair/fixtures"}
	for _, tc := range []struct {
		kind, from, hash           string
		logical, boundary, aligned int
	}{
		{"RentResource", "2026-10-02T20:54:03.378227Z", "57f4f50ce618dde5c4bf1485d3205f9b0757a752159dbd111460caf90bafd827", 7, 2, 9},
		{"ReturnResource", "2026-10-03T00:34:39.138186Z", "05e83726dbb9166459def85f58cdc9e901dea91bbbaf465ecc0670e70e06bdc6", 0, 1, 1},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			h, _ := BinaryHex(tc.hash, 32)
			raw, e := a.Get(h)
			if e != nil {
				t.Fatal(e)
			}
			from, _ := time.Parse(time.RFC3339Nano, tc.from)
			s := Scan{Kind: tc.kind, From: from, To: from.Add(30 * time.Second)}
			p, next, e := ParsePage(raw, Evidence{}, DefaultConfig(), s)
			if e != nil || next != "" || len(p.Data) != tc.logical || int(p.ExcludedBoundary) != tc.boundary {
				t.Fatal(e, len(p.Data), p.ExcludedBoundary)
			}
			s.From, s.To = s.From.Truncate(time.Second), s.To.Truncate(time.Second)
			p, _, e = ParsePage(raw, Evidence{}, DefaultConfig(), s)
			if e != nil || len(p.Data) != tc.aligned {
				t.Fatal(e, len(p.Data))
			}
			re := p.Data[0]
			for _, edge := range []struct {
				at    time.Time
				count int
				fail  bool
			}{
				{s.From, 1, false}, {s.To.Add(-time.Millisecond), 1, false}, {s.From.Add(-time.Millisecond), 0, true}, {s.To, 0, true},
			} {
				re.BlockTimestamp = edge.at.UnixMilli()
				body := JSONEvidence(Page{Success: Ptr(true), Data: []RawEvent{re}})
				out, _, err := ParsePage(body, Evidence{}, DefaultConfig(), s)
				if (err != nil) != edge.fail || (!edge.fail && len(out.Data) != edge.count) {
					t.Fatal(edge, err)
				}
			}
			s.From = from
			s.To = from.Add(30 * time.Second)
			re.BlockTimestamp = s.To.Truncate(time.Second).UnixMilli()
			// The upper envelope includes this event, but the logical half-open
			// interval includes it only when strictly before To.
			re.BlockTimestamp = s.To.Truncate(time.Second).Add(500 * time.Millisecond).UnixMilli()
			out, _, err := ParsePage(JSONEvidence(Page{Success: Ptr(true), Data: []RawEvent{re}}), Evidence{}, DefaultConfig(), s)
			if err != nil || len(out.Data) != 0 || out.ExcludedBoundary != 1 {
				t.Fatal(err, out)
			}
			u, _ := url.Parse(EventPath(s.Kind, s.From, s.To, "fp"))
			if u.Query().Get("min_block_timestamp") != fmt.Sprint(s.From.Truncate(time.Second).UnixMilli()) {
				t.Fatal(u)
			}
			bad := DefaultConfig().EventAPIURL + EventPath(s.Kind, s.From.Add(time.Second), s.To, "fp")
			if _, _, err = ParsePage([]byte(fmt.Sprintf(`{"success":true,"data":[],"meta":{"links":{"next":%q}}}`, bad)), Evidence{}, DefaultConfig(), s); err == nil {
				t.Fatal("changed pagination envelope accepted")
			}
		})
	}
}

func TestFractionalMigrationFrozenRetryAndFailureBackoff(t *testing.T) {
	c, _, now := testCollector(t)
	c.State.BootstrapReady = true
	from := now.Add(-time.Hour).Add(378227 * time.Microsecond)
	old := Scan{ID: "watch:RentResource:" + from.Format(time.RFC3339Nano), Kind: "RentResource", Mode: "live", From: from, To: from.Add(30 * time.Second)}
	c.State.Scans = []Scan{old}
	c.State.EventCursors[old.Kind] = from
	c.AddPage(old)
	oldID := c.State.Ops[0].Batch.Capture.CaptureId
	c.Schedule(true)
	if c.State.EventCursors[old.Kind] != from || c.State.Ops[0].Batch.Capture.CaptureId != oldID || !c.State.Ops[0].Failed || c.State.Ops[0].Batch.Capture.Reason != "legacy_fractional_window_replaced" {
		t.Fatal("unsafe migration")
	}
	var replacement Scan
	for _, s := range c.State.Scans {
		if s.Kind == old.Kind && !s.Done {
			replacement = s
		}
	}
	if !replacement.From.Equal(from.Truncate(time.Second)) || replacement.To.Sub(replacement.From) != 30*time.Minute || replacement.ID == old.ID {
		t.Fatal(replacement)
	}
	// Frozen content is not mutated by retirement.
	c2, _, _ := testCollector(t)
	c2.State.Scans = []Scan{old}
	c2.AddPage(old)
	c2.State.Ops[0].Frozen = true
	before, _ := Freeze(c2.State.Ops[0])
	c2.retireFractionalWatch()
	after, _ := Freeze(c2.State.Ops[0])
	if string(before) != string(after) || c2.State.Scans[0].Done {
		t.Fatal("frozen migration changed")
	}
	// Failed daily rescans keep their original token/bounds and retry deadline.
	c3, _, clock := testCollector(t)
	c3.State.BootstrapReady = true
	s := Scan{ID: "rescan:2026-10-03", Kind: "Liquidate", Mode: "catchup", From: clock.Add(-24 * time.Hour), To: *clock, Fingerprint: "stable-token"}
	c3.State.Scans = []Scan{s}
	c3.AddPage(s)
	o := &c3.State.Ops[0]
	o.Failed = true
	o.Batch.Capture.Status = "partial"
	o.Requests = nil
	if e := c3.Finish(context.Background(), o); e != nil {
		t.Fatal(e)
	}
	retry := c3.State.Scans[0].RetryAt
	c3.State.Ops = nil
	c3.Schedule(true)
	if !c3.State.Scans[0].Done || !retry.Equal(clock.Add(time.Minute)) {
		t.Fatal("no failure cooldown")
	}
	*clock = retry
	c3.Schedule(true)
	if c3.State.Scans[0].Done || c3.State.Scans[0].Fingerprint != "stable-token" || c3.State.Scans[0].To != s.To {
		t.Fatal("failed window not resumed intact")
	}
}

func TestProbeCompletionAfterAdmissionDeadlineAndTVMClasses(t *testing.T) {
	c, store, now := testCollector(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"blockID":"%016x%048x","block_header":{"raw_data":{"number":1,"timestamp":%d}}}`, 1, 0, now.UnixMilli())
	}))
	defer server.Close()
	c.Config.NodeRPCURL = server.URL
	c.API.Config = c.Config
	o := c.NewOperation("probe", "live", "publicnode")
	o.Scheduled = now.Add(-time.Minute)
	o.Headers = []Header{{Number: 1, Hash: Hash([]byte("before")), Time: *now}}
	addr, _ := HexAddress(ContractHex)
	renter, _ := HexAddress(c.Config.Callers[1])
	caller, _ := HexAddress(c.Config.Callers[0])
	o.Batch.Probes = []Probe{{CaptureId: o.Batch.Capture.CaptureId, CaptureStartedAt: o.Batch.Capture.CaptureStartedAt, ContractAddress: addr, Renter: renter, Receiver: renter, CallerAddress: caller, StateBinding: "node_latest_unpinned", ResourceType: 1, Status: "tvm_failure", AvailableAt: *now}}
	o.Requests = []Request{headRequest(false, "probe_after", false)}
	c.State.Ops = []Operation{o}
	*now = now.Add(5 * time.Second)
	drain(t, c, now, 20)
	if len(store.Batches) != 1 || store.Batches[0].Capture.Status == "skipped" || store.Batches[0].Probes[0].HeadAfterNumber == nil {
		t.Fatal("completed simulation discarded")
	}
	can := Candidate{Origin: RawEvent{Transaction: fmt.Sprintf("%064x", 1)}}
	for _, tc := range []struct{ ret, status string }{
		{`{"contractRet":"REVERT"}`, "revert"}, {`{"contractRet":2}`, "revert"}, {`{"contractRet":"OUT_OF_ENERGY"}`, "tvm_failure"}, {`{"contractRet":"OUT_OF_TIME"}`, "tvm_failure"}, {`{"ret":"FAILED"}`, "tvm_failure"}, {`{"contractRet":"DEFAULT"}`, "unknown"},
	} {
		raw := []byte(fmt.Sprintf(`{"result":{"result":true},"transaction":{"ret":[%s]}}`, tc.ret))
		p, e := ParseProbe(raw, Evidence{Available: *now}, can, "", *now, c)
		if e != nil || p.Status != tc.status {
			t.Fatal(tc, e, p.Status)
		}
	}
}

func TestSingleProbeAdmissionAndFrozenHistoryWindow(t *testing.T) {
	c, _, now := testCollector(t)
	c.API.Limiter.Started = now.Add(-5 * time.Minute)
	c.State.BootstrapReady = true
	for _, kind := range []string{"RentResource", "ReturnResource", "Liquidate"} {
		c.State.EventCursors[kind] = now.Add(-2 * time.Minute)
	}
	c.State.NextEvents = now.Add(time.Hour)
	c.State.Cohort = []Candidate{{Verified: true}, {Verified: true}, {Verified: true}}
	c.Schedule(true)
	n := 0
	for _, o := range c.State.Ops {
		if o.Kind == "probe" {
			n++
		}
	}
	if n != 1 || c.State.Rotation != 1 || c.State.NextProbe.Sub(*now) != 6*time.Second {
		t.Fatal(n, c.State.Rotation)
	}
	c.EnsureHistory(30)
	c.State.EventCursors["Liquidate"] = now.Add(-150 * time.Second)
	c.State.NextEvents = *now
	c.Schedule(true)
	history, live := false, false
	for _, s := range c.State.Scans {
		if s.ID == "history:"+c.State.HistoryFrom.Format(time.RFC3339) {
			history = true
		}
		if s.Kind == "Liquidate" && len(s.ID) > 9 && s.ID[:9] == "watch:v2:" {
			live = true
		}
	}
	if !history || !live {
		t.Fatal("history starved live Liquidate", c.State.Scans)
	}
	from, to := c.State.HistoryFrom, c.State.HistoryTo
	*now = now.Add(48 * time.Hour)
	c.EnsureHistory(30)
	if c.State.HistoryFrom != from || c.State.HistoryTo != to || to.Sub(from) != 30*24*time.Hour {
		t.Fatal("history drifted on restart")
	}
	if c.lifecycleCurrent(*now) {
		t.Fatal("stale lifecycle allowed")
	}
	c.State.Ops = nil
	c.State.NextProbe = *now
	c.Schedule(true)
	if c.Active("probe") {
		t.Fatal("stale lifecycle probed")
	}
}

func TestWarmupDoesNotAdmitProbeQueue(t *testing.T) {
	c, _, now := testCollector(t)
	c.State.BootstrapReady = true
	for _, kind := range []string{"RentResource", "ReturnResource"} {
		c.State.EventCursors[kind] = now.Add(-2 * time.Minute)
	}
	c.State.Cohort = []Candidate{{Verified: true}}
	c.Schedule(true)
	if c.Active("probe") {
		t.Fatal("warmup queued an unserviceable probe")
	}
}

// NULL must survive the report boundary even when the sum of known fees is 0.
func TestSummaryUnknownFeeIsNull(t *testing.T) {
	b, _ := json.Marshal(Summary{NetProfitStatus: "unknown"})
	var m map[string]any
	json.Unmarshal(b, &m)
	if m["known_whole_transaction_burn_trx"] != nil {
		t.Fatal(string(b))
	}
}

func TestAPIExecutionExceptionAndLegacyReportClassification(t *testing.T) {
	c, _, now := testCollector(t)
	actualHash, _ := BinaryHex("3f45d5a0d9dabf2492111b14cbbffd027bb76848bd5941ea974ec2ac31a708d4", 32)
	actual, err := (Archive{Dir: "../../research/2026-10-03-keeper-repair/fixtures"}).Get(actualHash)
	if err != nil {
		t.Fatal(err)
	}
	actualProbe, err := ParseProbe(actual, Evidence{Available: *now}, Candidate{Origin: RawEvent{Transaction: fmt.Sprintf("%064x", 1)}}, "", *now, c)
	if err != nil || actualProbe.Status != "tvm_failure" || actualProbe.ApiSuccess != nil {
		t.Fatal("omitted API result fabricated or VM exception missed", err, actualProbe)
	}
	message := "class org.tron.core.vm.program.Program$OutOfTimeException : CPU timeout for 'JUMP' operation executing"
	raw := []byte(fmt.Sprintf(`{"result":{"result":false,"code":"OTHER_ERROR","message":%q}}`, hex.EncodeToString([]byte(message))))
	h, e := c.API.Archive.Put(raw)
	if e != nil {
		t.Fatal(e)
	}
	can := Candidate{Origin: RawEvent{Transaction: fmt.Sprintf("%064x", 1)}}
	p, e := ParseProbe(raw, Evidence{Available: *now, ResponseHash: Hex(h)}, can, "", *now, c)
	if e != nil || p.Status != "tvm_failure" || p.ApiSuccess == nil || *p.ApiSuccess {
		t.Fatal("VM exception treated as network error", e, p)
	}
	// The legacy report must also derive the classification when protobuf
	// omitted result.result, as in the real archived CPU timeout response.
	raw = actual
	h, e = c.API.Archive.Put(raw)
	if e != nil {
		t.Fatal(e)
	}
	p, e = ParseProbe(raw, Evidence{Available: *now, ResponseHash: Hex(h)}, can, "", *now, c)
	if e != nil {
		t.Fatal(e)
	}
	o := c.NewOperation("probe", "live", "publicnode")
	p.CaptureId, p.CaptureStartedAt = o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt
	p.CallerAddress = "caller"
	p.Renter = "renter"
	p.Receiver = "receiver"
	p.Status = "rpc_error" // unchanged legacy stored status
	o.Batch.Probes = []Probe{p}
	o.Batch.Capture.EvidenceManifestHash, e = c.API.Archive.Put(JSONEvidence(Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: o.Batch.Capture.CaptureId.String()}))
	if e != nil {
		t.Fatal(e)
	}
	Seal(&o.Batch)
	out := t.TempDir()
	if e = Report(context.Background(), testReportReader{[]Batch{o.Batch}}, c.API.Archive, c.Config, now.Add(-time.Minute), now.Add(time.Minute), *now, out); e != nil {
		t.Fatal(e)
	}
	var summary Summary
	b, e := os.ReadFile(filepath.Join(out, "summary.json"))
	if e != nil {
		t.Fatal(e)
	}
	json.Unmarshal(b, &summary)
	if summary.ProbeStatus["tvm_failure"] != 1 || summary.ProbeObservedStatus["rpc_error"] != 1 || summary.ProbeReclassified != 1 || o.Batch.Probes[0].Status != "rpc_error" {
		t.Fatal("legacy report mutated or misclassified", summary)
	}
	if classifyTVMFailure(`contractRet:"OUT_OF_ENERGY"`, "REVERT", nil) != "tvm_failure" {
		t.Fatal("explicit non-REVERT result masked")
	}
}

type bulkTestReader struct {
	testReportReader
	calls  int
	tamper bool
}

func (r *bulkTestReader) KeeperBatch(context.Context, Capture) (Batch, error) {
	return Batch{}, fmt.Errorf("N+1 path used")
}
func (r *bulkTestReader) KeeperBatches(_ context.Context, caps []Capture) (map[uuid.UUID]Batch, error) {
	r.calls++
	if len(caps) > 256 {
		return nil, fmt.Errorf("unbounded group")
	}
	out := map[uuid.UUID]Batch{}
	for _, cap := range caps {
		out[cap.CaptureId] = Batch{Capture: cap}
	}
	if r.tamper && len(caps) > 0 {
		b := out[caps[0].CaptureId]
		b.Probes = []Probe{{}}
		out[caps[0].CaptureId] = b
	}
	return out, nil
}
func TestBulkReportBoundedAndAuthenticatesEveryCapture(t *testing.T) {
	c, _, now := testCollector(t)
	r := &bulkTestReader{}
	for i := 0; i < 260; i++ {
		o := c.NewOperation("quote", "live", "binance")
		o.Batch.Capture.EvidenceManifestHash, _ = c.API.Archive.Put(JSONEvidence(Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: o.Batch.Capture.CaptureId.String()}))
		Seal(&o.Batch)
		r.batches = append(r.batches, o.Batch)
	}
	if e := Report(context.Background(), r, c.API.Archive, c.Config, now.Add(-time.Hour), *now, *now, t.TempDir()); e != nil || r.calls != 2 {
		t.Fatal(e, r.calls)
	}
	r.tamper = true
	if e := Report(context.Background(), r, c.API.Archive, c.Config, now.Add(-time.Hour), *now, *now, t.TempDir()); e == nil {
		t.Fatal("bulk membership bypassed")
	}
}
func TestParallelReportEvidenceFailsClosedAndRetainsResponses(t *testing.T) {
	c, _, _ := testCollector(t)
	raw := []byte(`{"result":{"code":"OTHER_ERROR"}}`)
	h, err := c.API.Archive.Put(raw)
	if err != nil {
		t.Fatal(err)
	}
	caps := []Capture{}
	for i := 0; i < 17; i++ {
		o := c.NewOperation("quote", "live", "binance")
		m := Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: o.Batch.Capture.CaptureId.String(), Requests: []Evidence{{ResponseHash: Hex(h)}}}
		o.Batch.Capture.EvidenceManifestHash, err = c.API.Archive.Put(JSONEvidence(m))
		if err != nil {
			t.Fatal(err)
		}
		Seal(&o.Batch)
		caps = append(caps, o.Batch.Capture)
	}
	verified := map[string]bool{}
	manifests, bodies, err := reportEvidence(context.Background(), c.API.Archive, caps, verified, map[string]bool{h: true})
	if err != nil || len(manifests) != len(caps) || string(bodies[h]) != string(raw) || !verified[h] {
		t.Fatal("parallel evidence", err, len(manifests), bodies)
	}
	bad := append([]Capture(nil), caps...)
	bad[0].EvidenceManifestHash = caps[1].EvidenceManifestHash
	if _, _, err = reportEvidence(context.Background(), c.API.Archive, bad, verified, nil); err == nil {
		t.Fatal("cached raw hash bypassed capture binding")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = reportEvidence(ctx, c.API.Archive, caps, verified, nil); err == nil {
		t.Fatal("cancel ignored")
	}
	p := filepath.Join(c.API.Archive.Dir, Hex(h)[:2], Hex(h)+".gz")
	if err = os.WriteFile(p, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	fresh := map[string]bool{}
	if _, _, err = reportEvidence(context.Background(), c.API.Archive, caps, fresh, nil); err == nil || len(fresh) != 0 {
		t.Fatal("corrupt evidence accepted or cached", err, fresh)
	}
}
func TestReportResponseCacheBound(t *testing.T) {
	a := Archive{Dir: t.TempDir()}
	retain := map[string]bool{}
	for i := 0; i < 3; i++ {
		raw := bytes.Repeat([]byte{byte('a' + i)}, 6*1024*1024)
		h, err := a.Put(raw)
		if err != nil {
			t.Fatal(err)
		}
		retain[h] = true
	}
	verified := map[string]bool{}
	_, bodies, err := reportEvidence(context.Background(), a, nil, verified, retain)
	if err != nil || len(verified) != 3 || len(bodies) != 2 {
		t.Fatal(err, len(verified), len(bodies))
	}
	size := 0
	for _, body := range bodies {
		size += len(body)
	}
	if size > 16*1024*1024 {
		t.Fatal("unbounded response cache", size)
	}
	for h := range retain {
		if _, ok := bodies[h]; !ok {
			if body, err := a.Get(h); err != nil || len(body) != 6*1024*1024 {
				t.Fatal("uncached response cannot be authenticated again", err)
			}
		}
	}
}
func TestUTCIndexDelayForHistoryAndRescan(t *testing.T) {
	c, _, now := testCollector(t)
	*now = time.Date(2026, 10, 4, 0, 0, 30, 0, time.UTC)
	c.EnsureHistory(1)
	if c.State.HistoryTo.After(now.Add(-120 * time.Second)) {
		t.Fatal("history beyond indexing delay")
	}
	c.State.BootstrapReady = true
	c.State.RescanDay = "2026-10-03"
	c.Schedule(true)
	for _, s := range c.State.Scans {
		if s.ID == "rescan:2026-10-04" {
			t.Fatal("premature rescan")
		}
	}
	*now = time.Date(2026, 10, 4, 0, 2, 0, 0, time.UTC)
	c.Schedule(true)
	found := false
	for _, s := range c.State.Scans {
		if s.ID == "rescan:2026-10-04" {
			found = true
		}
	}
	if !found {
		t.Fatal("rescan not admitted after delay")
	}
}
