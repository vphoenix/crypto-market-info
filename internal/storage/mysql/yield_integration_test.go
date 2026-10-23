package mysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/aave"
	"github.com/vphoenix/crypto-market-info/internal/yield/okxearn"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMySQLYieldStrictCollectorsExactDecimalsAndRetry(t *testing.T) {
	client := integrationClient(t)
	ctx := context.Background()
	var err error
	at := time.Date(2026, 8, 27, 0, 0, 0, 123000000, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/okx/api/v5/finance/savings/lending-rate-history":
			if r.URL.Query().Get("after") != "" {
				fmt.Fprint(w, `{"code":"0","data":[]}`)
				return
			}
			fmt.Fprintf(w, `{"code":"0","data":[{"ccy":"AVAX","ts":"%d","lendingRate":"0.0206","rate":"99"},{"ccy":"AVAX","ts":"%d","lendingRate":"0"}]}`, at.Add(-time.Hour).UnixMilli(), at.Add(-2*time.Hour).UnixMilli())
		case "/v3":
			fmt.Fprint(w, `{"data":{"market":{"address":"0x794a61358d6845594f94dc1db02a252b5b4814ad","chain":{"chainId":43114},"reserves":[{"underlyingToken":{"address":"0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7","decimals":18},"aToken":{"address":"0x6d80113e533a2c0fe82eabd35f1875dcea89ea97","decimals":18}}]},"supplyAPYHistory":[{"date":"2026-08-26T23:00:00.123Z","avgRate":{"raw":"6668427376540457481976400","decimals":27}},{"date":"2026-08-26T22:00:00.123Z","avgRate":{"raw":"0","decimals":27}}]}}`)
		case "/v4":
			fmt.Fprint(w, `{"data":{"reserve":{"id":"NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA==","onChainId":"0","chain":{"chainId":43114},"spoke":{"address":"0x435272ceff93a1e657e8abfdf0a13e95900a3a56"},"asset":{"underlying":{"address":"0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7","info":{"decimals":18}}}},"supplyApyHistory":[{"date":"2026-08-26T23:00:00.123Z","avgRate":{"normalized":"1.36082551852043339681246800"}},{"date":"2026-08-26T22:00:00.123Z","avgRate":{"normalized":"0"}}]}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	okxCollector := okxearn.NewCollector(server.URL + "/okx")
	v3, v4 := aave.NewV3Collector(server.URL+"/v3"), aave.NewV4Collector(server.URL+"/v4")
	okxCollector.Now, v3.Now, v4.Now = func() time.Time { return at }, func() time.Time { return at }, func() time.Time { return at }
	okxCollector.Retry.Cooldown = exchange.NewRequestGate(0)
	var batches []marketyield.Batch
	for _, collector := range []marketyield.Collector{okxCollector, v3, v4} {
		batch, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			t.Fatal(collectErr)
		}
		if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
	}

	for range 2 {
		for _, batch := range batches {
			if e := client.WriteYieldBatch(ctx, batch); e != nil {
				t.Fatal(e)
			}
		}
	}
	routes, e := client.yieldRoutes(ctx, client.db)
	if e != nil || len(routes) != 3 {
		t.Fatal(routes, e)
	}
	observations, e := client.records(ctx, client.db, "yield_observation", "ORDER BY yield_route_id,observation_time DESC")
	if e != nil || len(observations) != 6 {
		t.Fatal("retry duplicates", len(observations), e)
	}
	for i, want := range []string{"0.0206", "0.006668427376540457", "0.013608255185204333"} {
		v := observations[i*2]
		rate := v["rate"].(decimal.Decimal)
		if rate.String() != want || v["observation_time"] != at.Add(-time.Hour) || v["source_payload_hash"] == nil {
			t.Fatal("decimal/source identity", v)
		}
	}
	// Polling the same source history refreshes collected_at and payload proof.
	// A retry retains its version, so older deliveries cannot undo that refresh.
	refresh := func(b marketyield.Batch, collected time.Time) marketyield.Batch {
		b.Items = append([]marketyield.CollectedYield(nil), b.Items...)
		b.CollectedAt = collected
		for i := range b.Items {
			b.Items[i].Observation.CollectedAt = collected
		}
		return b
	}
	newer := refresh(batches[0], at.Add(time.Minute))
	hash := strings.Repeat("b", 64)
	rate := decimal.RequireFromString("0.023456789012345678987")
	newer.Items[0].Observation.SourcePayloadHash = &hash
	newer.Items[0].Observation.Rate = &rate
	if e = client.WriteYieldBatch(ctx, newer); e != nil {
		t.Fatal("overlapping source refresh", e)
	}
	if e = client.WriteYieldBatch(ctx, batches[0]); e != nil {
		t.Fatal("superseded retry", e)
	}
	observations, e = client.records(ctx, client.db, "yield_observation", "ORDER BY yield_route_id,observation_time DESC")
	if e != nil || len(observations) != 6 {
		t.Fatal("refresh lost or duplicated", e)
	}
	foundRefresh := false
	for _, row := range observations {
		if row["yield_route_id"] == routes[0].ID && row["observation_time"] == newer.Items[0].Observation.ObservationTime {
			foundRefresh = true
			if row["collected_at"] != newer.CollectedAt || row["rate"].(decimal.Decimal).String() != "0.023456789012345678" {
				t.Fatal("superseded retry undid refresh", row["collected_at"], row["rate"])
			}
		}
	}
	if !foundRefresh {
		t.Fatal("refreshed observation missing")
	}
	conflict := refresh(newer, newer.CollectedAt)
	badRate := decimal.RequireFromString("0.03")
	conflict.Items[0].Observation.Rate = &badRate
	newPoint := conflict.Items[1]
	newPoint.Observation.ObservationTime = at.Add(-3 * time.Hour)
	conflict.Items = append([]marketyield.CollectedYield{newPoint}, conflict.Items...)
	if e = client.WriteYieldBatch(ctx, conflict); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("same-version conflict accepted", e)
	}
	observations, e = client.records(ctx, client.db, "yield_observation", "")
	if e != nil || len(observations) != 6 {
		t.Fatal("batch escaped rollback", len(observations), e)
	}
	anchored := refresh(newer, at.Add(2*time.Hour))
	anchored.Items = anchored.Items[:1]
	o := &anchored.Items[0].Observation
	o.ObservationTime = at
	height, block, finality := uint64(123), strings.Repeat("a", 64), "safe"
	o.BlockHeight, o.BlockHash, o.Finality = &height, &block, &finality
	if e = client.WriteYieldBatch(ctx, anchored); e != nil {
		t.Fatal(e)
	}
	finalized := refresh(anchored, at.Add(3*time.Hour))
	f := "finalized"
	finalized.Items[0].Observation.Finality = &f
	if e = client.WriteYieldBatch(ctx, finalized); e != nil {
		t.Fatal("finality upgrade rejected", e)
	}
	for _, mutate := range []func(*marketyield.YieldObservation){
		func(o *marketyield.YieldObservation) { o.Finality = &finality },
		func(o *marketyield.YieldObservation) { v := uint64(124); o.BlockHeight = &v },
		func(o *marketyield.YieldObservation) { v := strings.Repeat("c", 64); o.BlockHash = &v },
		func(o *marketyield.YieldObservation) { o.BlockHash = nil; o.BlockHeight = nil; o.Finality = nil },
	} {
		bad := refresh(finalized, at.Add(4*time.Hour))
		mutate(&bad.Items[0].Observation)
		if e = client.WriteYieldBatch(ctx, bad); !errors.Is(e, contracts.ErrConflict) {
			t.Fatal("anchor/finality regression accepted", e)
		}
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if e = client.WriteYieldBatch(cancelled, batches[0]); e == nil {
		t.Fatal("cancelled write accepted")
	}
}
