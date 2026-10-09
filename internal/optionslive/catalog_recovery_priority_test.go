package optionslive

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
)

func TestScopeRefreshDoesNotWaitBehindFullSymbolQueue(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	s.scopeJobs = make(chan *catalogJob, 16)
	for n := 0; n < cap(s.jobs); n++ {
		if !s.enqueue(&catalogJob{kind: "instrument", symbol: e.item.Spec.Instrument.ExchangeSymbol, scope: e.scope}) {
			t.Fatal("fill ordinary queue")
		}
	}
	s.requestScope(e.scope)
	if len(s.scopeJobs) != 1 {
		t.Fatal("bulk refresh was refused behind a full symbol queue")
	}
	methods := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods <- r.URL.Path
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer server.Close()
	s.c = deribit.NewClient(server.URL, "")
	s.c.RESTGate = exchange.NewRequestGate(time.Hour)
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	s.ctx = ctx
	s.persisted = make(chan catalogResult, 1)
	done := make(chan struct{})
	go func() { s.persistLoop(s.jobs); close(done) }()
	select {
	case method := <-methods:
		if method != "/api/v2/public/get_instruments" {
			t.Fatalf("symbol work starved bulk refresh: %s", method)
		}
	case <-ctx.Done():
		t.Fatal("no metadata request")
	}
	cancel()
	<-done
}

func TestDelayedCatalogPublicationCannotRenewObservationClock(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	j := freshCatalogJob(s, e, "catalog", 0)
	j.observation.ObservedAt = time.Now().UTC().Add(-34 * time.Minute).Truncate(time.Microsecond)
	j.observation.RequestedAt = j.observation.ObservedAt.Add(-time.Second)
	s.published(catalogResult{job: j})
	if !s.scopes[e.scope.String()].Equal(j.observation.ObservedAt) {
		t.Fatal("database publication extended old source evidence lifetime")
	}
}

func TestMemberProofAgeSchedulesBulkEvenWhenScopeClockIsNew(t *testing.T) {
	s, _, e, _ := supervisorFixture(t)
	now := time.Now().UTC()
	s.lifecycleReady = true
	s.scopeJobs = make(chan *catalogJob, 16)
	expiry := now.Add(time.Hour)
	e.item.Spec.Instrument.ExpiryTime = &expiry
	for _, scope := range catalogScopes() {
		s.scopes[scope.String()] = now
	}
	e.state.StateKind = 2
	e.state.ObservedAt = now
	e.state.StateObservedAt = now.Add(-34 * time.Minute)
	s.tick(now)
	if len(s.scopeJobs) != 1 || (<-s.scopeJobs).scope != e.scope {
		t.Fatal("old referenced member proof was masked by the scope refresh clock")
	}
}

func TestFreshScopePendingSuppressesOnlyGenericSymbolRepair(t *testing.T) {
	for _, barrier := range []bool{false, true} {
		s, _, e, _ := supervisorFixture(t)
		now := time.Now().UTC()
		s.lifecycleReady = true
		expiry := now.Add(time.Hour)
		e.item.Spec.Instrument.ExpiryTime = &expiry
		for _, scope := range catalogScopes() {
			s.scopes[scope.String()] = now
		}
		e.state.Known = false
		s.inflight[e.scope.String()] = &catalogJob{kind: "catalog", scope: e.scope, epoch: s.epoch}
		if barrier {
			s.pendingStates[e.item.Spec.Instrument.ExchangeSymbol] = pendingState{s.epoch, 9}
		}
		s.tick(now)
		if (!barrier && len(s.jobs) != 0) || (barrier && len(s.jobs) != 1) {
			t.Fatalf("bulk recovery bypassed source barrier or flooded generic repair: barrier=%t jobs=%d", barrier, len(s.jobs))
		}
	}
}

func TestConfirmedMemberDropsUncapturedRetryButKeepsPreparedEvidence(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		s, _, e, _ := supervisorFixture(t)
		now := time.Now().UTC()
		expiry := now.Add(time.Hour)
		e.item.Spec.Instrument.ExpiryTime = &expiry
		for _, scope := range catalogScopes() {
			s.scopes[scope.String()] = now
		}
		e.state.Known, e.state.Epoch = true, s.epoch
		e.state.ObservedAt, e.state.StateObservedAt = now, now
		j := &catalogJob{kind: "instrument", symbol: e.item.Spec.Instrument.ExchangeSymbol, scope: e.scope}
		if prepared {
			j.result = &deribit.ScopeResult{}
		}
		s.scheduleRetry(j, time.Hour)
		s.tick(now)
		if prepared && (len(s.retries) != 1 || s.retries[0].job != j || s.jobsBudget.Used() == 0) {
			t.Fatal("ambiguous captured source evidence lost its identity")
		}
		if !prepared && (len(s.retries) != 0 || s.jobsBudget.Used() != 0 || s.inflight[e.scope.String()+":"+j.symbol] != nil) {
			t.Fatal("already confirmed generic retry retained budget or single-flight slot")
		}
	}
}
