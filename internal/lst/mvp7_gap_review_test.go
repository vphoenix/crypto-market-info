package lst

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMVP7RPCGapAppliesToStartupSteadyAndBackfill(t *testing.T) {
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) { return transportReply(200, `{}`), nil })
	tr.cfg.RPCGap = 10 * time.Second
	tr.cfg.StartupRPCGap = 2 * time.Second
	if tr.spacing("rpc", clock.Now()) != 10*time.Second || tr.spacing("binance", clock.Now()) != 5*time.Second {
		t.Fatal("configured minimum bypassed at startup or changed Binance")
	}
	tr.MarkInitialized()
	clock.Sleep(context.Background(), 2*time.Minute)
	if tr.spacing("rpc", clock.Now()) != 10*time.Second {
		t.Fatal("configured steady gap ignored")
	}
	tr.cfg.Backfill = true
	if tr.spacing("rpc", clock.Now()) != 10*time.Second {
		t.Fatal("explicit gap bypassed in backfill")
	}
	tr.cfg.RPCGap = 0
	if tr.spacing("rpc", clock.Now()) != time.Second {
		t.Fatal("original backfill default changed")
	}
	tr.cfg.Backfill = false
	if tr.spacing("rpc", clock.Now()) != 500*time.Millisecond {
		t.Fatal("original default changed")
	}
	for _, gap := range []time.Duration{499 * time.Millisecond, 10*time.Second + time.Nanosecond} {
		cfg := tr.cfg
		cfg.RPCGap = gap
		if _, e := NewTransport(cfg); e == nil {
			t.Fatal("invalid RPC gap accepted", gap)
		}
	}
}

func TestMVP7SlowerGapPersistsAfterResponsesAndRestart(t *testing.T) {
	var clock *transportFakeClock
	var sent, received []time.Time
	tr, fake := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sent = append(sent, clock.Now())
		clock.Sleep(context.Background(), 3*time.Second)
		received = append(received, clock.Now())
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	clock = fake
	tr.cfg.RPCGap = 2 * time.Second
	tr.cfg.StartupRPCGap = 10 * time.Second
	tr.started = clock.Now().Add(-2 * time.Minute)
	tr.MarkInitialized()
	for range 2 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	if sent[1].Sub(received[0]) < 2*time.Second {
		t.Fatal("slow response earned shorter gap", sent, received)
	}
	g, e := tr.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	oldNext := g.state.NextAt
	oldRecent, e := json.Marshal(g.state.Recent)
	if e != nil {
		t.Fatal(e)
	}
	clock.Sleep(context.Background(), time.Millisecond)
	restarted, e := NewTransport(tr.cfg)
	if e != nil {
		t.Fatal(e)
	}
	restarted.MarkInitialized()
	g2, e := restarted.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	newRecent, e := json.Marshal(g2.state.Recent)
	if e != nil {
		t.Fatal(e)
	}
	if !g2.state.NextAt.Equal(oldNext) || !bytes.Equal(oldRecent, newRecent) || len(g2.state.Recent) != 2 {
		t.Fatal("restart changed frozen reservations")
	}
	atRestart := clock.Now()
	if _, e = restarted.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
		t.Fatal(e)
	}
	if len(sent) != 3 || sent[2].Sub(atRestart) < 10*time.Second || sent[2].Sub(received[1]) < 2*time.Second || len(g2.state.Recent) != 3 {
		t.Fatal("restart bypassed cold gap or discarded quota", sent, received, g2.state.Recent)
	}
}

func TestMVP7CooldownRestartDoesNotEraseFrozenQuotaOrRetryEarly(t *testing.T) {
	calls := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 3 {
			return transportReply(429, `{"jsonrpc":"2.0","id":1,"error":{"code":15,"message":"rate limit"}}`), nil
		}
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	clock.mu.Lock()
	clock.at = Now()
	clock.mu.Unlock()
	tr.cfg.RPCGap = 2 * time.Second
	tr.cfg.StartupRPCGap = 10 * time.Second
	tr.cfg.RPCRequestsPerMinute = 20
	tr.started = clock.Now().Add(-2 * time.Minute)
	tr.MarkInitialized()
	for range 2 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	g, e := tr.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	// Seed only this private fixture's already-persisted limit-hit history.
	// The callback never acquires a gate lock held by Do.
	g.mu.Lock()
	g.state.LimitHits = 10
	e = saveGate(g)
	g.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	res, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	if e == nil || calls != 3 || res.HTTPStatus != 429 || res.PayloadHash == "" || g.state.LimitHits != 11 || g.state.CooldownUntil.Sub(clock.Now()) < 30*time.Minute {
		t.Fatal("limit evidence/backoff not durable", calls, res, g.state, e)
	}
	frozen, e := json.Marshal(g.state)
	if e != nil {
		t.Fatal(e)
	}
	restarted, e := NewTransport(tr.cfg)
	if e != nil {
		t.Fatal(e)
	}
	restarted.MarkInitialized()
	g2, e := restarted.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	afterLoad, e := json.Marshal(g2.state)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(frozen, afterLoad) {
		t.Fatal("restart changed frozen state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := clock.Now()
	if e = restarted.WaitRPCSlots(ctx, "https://rpc.example", 10); e != nil || !clock.Now().Equal(before) {
		t.Fatal("capacity wait hid cooldown", e)
	}
	missing, e := restarted.Do(ctx, "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	afterFailure, je := json.Marshal(g2.state)
	if e == nil || !strings.Contains(e.Error(), "source_cooldown") || calls != 3 || !missing.RequestedAt.IsZero() || missing.HTTPStatus != 0 || missing.PayloadHash != "" || je != nil || !bytes.Equal(frozen, afterFailure) {
		t.Fatal("cold restart retried, fabricated HTTP evidence, or changed quota/backoff", calls, missing, g2.state, e)
	}
}
