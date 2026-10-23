package justlendkeeper

import (
	"testing"
	"time"
)

func TestResearchGateCountsActualRequestsAndPersistsReservation(t *testing.T) {
	c, _, now := testCollector(t)
	c.API.Limiter.Started = now.Add(-time.Hour)
	r := Request{Source: "publicnode", Research: true}
	if err := c.API.Limiter.ReserveRequest(r); err != nil {
		t.Fatal(err)
	}
	if c.State.Used != 1 || !c.State.NextResearchRequest.Equal(now.Add(5*time.Second)) {
		t.Fatal("request was not reserved", c.State.Used, c.State.NextResearchRequest)
	}
	saved, err := LoadState(c.Path, c.State.Target, c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if !saved.NextResearchRequest.Equal(c.State.NextResearchRequest) || saved.Used != 1 {
		t.Fatal("reservation lost on restart")
	}
	*now = now.Add(time.Second)
	if err := c.API.Limiter.ReserveRequest(r); err == nil || c.State.Used != 1 {
		t.Fatal("second research request bypassed gate")
	}
	if err := c.API.Limiter.Reserve("binance", false); err != nil {
		t.Fatal("research gate blocked unrelated source", err)
	}
	*now = now.Add(4 * time.Second)
	if err := c.API.Limiter.ReserveRequest(r); err != nil {
		t.Fatal(err)
	}
	if c.State.Used != 3 {
		t.Fatal("not all requests counted")
	}
}

func TestResearchGateExtendsFromResponseAndDoesNotBankIdleCredit(t *testing.T) {
	c, _, now := testCollector(t)
	c.API.Limiter.Started = now.Add(-time.Hour)
	r := Request{Source: "publicnode", Research: true}
	if err := c.API.Limiter.ReserveRequest(r); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(20 * time.Second)
	c.API.Limiter.ExtendRequestGap(r, *now)
	if got := c.API.Limiter.ReadyRequest(r); !got.Equal(now.Add(5 * time.Second)) {
		t.Fatal("slow response compressed interval", got)
	}
	*now = now.Add(time.Hour)
	if err := c.API.Limiter.ReserveRequest(r); err != nil {
		t.Fatal(err)
	}
	if err := c.API.Limiter.ReserveRequest(r); err == nil {
		t.Fatal("idle credit caused burst")
	}
}
