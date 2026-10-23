package justlendkeeper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRentalStateControlsConditionalProbe(t *testing.T) {
	for _, liquidatable := range []bool{false, true} {
		t.Run(fmt.Sprint(liquidatable), func(t *testing.T) {
			c, can, caller, ev, now := rewardParseContext()
			c.Now = func() time.Time { return ev.Available }
			c.State.CallersValid = []string{caller}
			c.State.Candidates = []Candidate{can}
			c.State.Candidates[0].Origin.Transaction = strings.Repeat("c", 64)
			can = c.State.Candidates[0]
			can.Origin = syntheticResearchOrigin(can, now)
			c.State.Candidates[0] = can
			c.addRentalState(can, "opportunity", now)
			o := &c.State.Ops[0]
			o.Requests = o.Requests[2:] // before/state have already been dequeued by Step
			flag := 0
			if liquidatable {
				flag = 1
			}
			body, _ := json.Marshal(map[string]any{"result": map[string]any{"result": true}, "transaction": map[string]any{"ret": []any{map[string]any{}}}, "constant_result": []string{fmt.Sprintf("%064x%064x%064x", 20, 3, flag)}})
			if err := c.Process(o, Request{Role: "rental_state"}, ev, body); err != nil {
				t.Fatal(err)
			}
			if len(o.Batch.RentalStates) != 1 || o.Batch.RentalStates[0].Liquidatable == nil || *o.Batch.RentalStates[0].Liquidatable != liquidatable {
				t.Fatal("state lost", o.Batch)
			}
			probes := 0
			for _, r := range o.Requests {
				if r.Role == "probe" {
					probes++
					if !r.Research {
						t.Fatal("conditional probe bypasses shared request gate")
					}
				}
			}
			want := 0
			if liquidatable {
				want = 1
			}
			if probes != want {
				t.Fatalf("liquidatable=%v generated %d probes", liquidatable, probes)
			}
			if o.Caller != caller {
				t.Fatal("caller changed")
			}
		})
	}
}

func TestRentalFinishMissingAfterHeadRetainsAttemptAndDowngradesReward(t *testing.T) {
	c, store, now := testCollector(t)
	parsed, can, caller, ev, _ := rewardParseContext()
	can.Origin = syntheticResearchOrigin(can, ev.Started)
	c.State.IdentityStatus = parsed.State.IdentityStatus
	c.State.IdentityAt = parsed.State.IdentityAt
	c.State.Implementation = parsed.State.Implementation
	c.State.CodeHash = parsed.State.CodeHash
	c.State.CallersValid = []string{caller}
	c.State.Candidates = []Candidate{can}
	*now = ev.Available.Add(time.Second)
	c.addRentalState(can, "opportunity", ev.Started)
	o := &c.State.Ops[0]
	o.Requests = nil
	p := parseSyntheticReward(t, syntheticRewardResponse(t))
	p.CaptureId = o.Batch.Capture.CaptureId
	p.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
	o.Batch.Probes = []Probe{p}
	stateBody := []byte(`{"result":{"result":true},"transaction":{"ret":[{}]},"constant_result":["` + fmt.Sprintf("%064x%064x%064x", 20, 3, 1) + `"]}`)
	row, err := ParseRentalState(stateBody, ev, can, caller, ev.Started, c)
	if err != nil {
		t.Fatal(err)
	}
	row.CaptureId = o.Batch.Capture.CaptureId
	row.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
	o.Batch.RentalStates = []RentalStateObservation{row}
	o.Headers = []Header{{Number: 10, Hash: strings.Repeat("a", 32), Time: ev.Started.Add(-time.Second)}}
	o.Failed = true
	o.Batch.Capture.Status = "partial"
	if err = c.Finish(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if len(store.Batches) != 1 {
		t.Fatal("attempt was not retained")
	}
	b := store.Batches[0]
	if b.Probes[0].Status == "success_reward" || b.RentalStates[0].Status == "ok" {
		t.Fatal("missing anchor certified success")
	}
	if b.RentalStates[0].Liquidatable == nil || !*b.RentalStates[0].Liquidatable {
		t.Fatal("known state tuple discarded")
	}
	if err = Validate(b); err != nil {
		t.Fatal("failed after-head permanently blocks commit", err)
	}
}

func syntheticResearchOrigin(can Candidate, now time.Time) RawEvent {
	raw := opportunityCandidate(1, now, true).Origin
	raw.Result["renter"] = json.RawMessage(fmt.Sprintf("%q", Hex(can.Renter)))
	raw.Result["receiver"] = json.RawMessage(fmt.Sprintf("%q", Hex(can.Receiver)))
	return raw
}

func TestRestoredResearchChainsDrainSerially(t *testing.T) {
	c, _, now := testCollector(t)
	c.Opportunities = true
	c.API.Limiter.Started = now.Add(-time.Hour)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprintf(w, `{"blockID":"%016x%048x","block_header":{"raw_data":{"number":1,"timestamp":%d}}}`, 1, 0, now.UnixMilli())
	}))
	defer server.Close()
	c.Config.NodeRPCURL = server.URL
	c.API.Config = c.Config
	for range 2 {
		o := c.NewOperation("seed", "catchup", "publicnode")
		o.Batch.Capture.CaptureKind = "events"
		o.Requests = []Request{headRequest(true, "solid", true), headRequest(true, "solid", true)}
		c.State.Ops = append(c.State.Ops, o)
	}
	for range 2 {
		if _, _, err := c.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	*now = now.Add(5 * time.Second)
	if _, _, err := c.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("legacy operations did not retain a single request chain", calls)
	}
	if len(c.State.Ops[0].Evidence) != 0 && len(c.State.Ops[1].Evidence) != 0 {
		t.Fatal("two old chains interleaved")
	}
	if _, _, err := c.Step(context.Background()); err != nil {
		t.Fatal(err)
	} // commit owner
	*now = now.Add(29 * time.Second)
	if _, _, err := c.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("restored next chain bypassed 30-second completion pause")
	}
	*now = now.Add(time.Second)
	if _, _, err := c.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatal("next restored chain was not resumed", calls)
	}
}
