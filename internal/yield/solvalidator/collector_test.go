package solvalidator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testVote = "CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1"

func TestCollectorWritesOnlyCompletedEpochsAndKeepsReportedAPY(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/validators" || r.URL.Query().Get("epochs") != "10" || r.URL.Query().Get("limit") != "1" || r.URL.Query().Get("query_vote_accounts") != testVote {
			t.Fatalf("request=%s", r.URL.String())
		}
		fmt.Fprintf(w, `{"total_count":1,"validators":[{"vote_account":%q,"info_name":"Figment","epoch_stats":[{"epoch":1021,"epoch_end_at":null,"apr":null,"apy":null,"commission_effective":null,"mev_commission_bps":null,"activated_stake":"3"},{"epoch":1019,"epoch_end_at":"2026-08-21T07:45:00.833593Z","apr":0.04,"apy":0.041,"commission_effective":6,"mev_commission_bps":600,"activated_stake":"1000000000"},{"epoch":1020,"epoch_end_at":"2026-08-23T03:47:00.728017Z","apr":0.05,"apy":0.052,"commission_effective":7,"mev_commission_bps":700,"activated_stake":"2000000000"}]}]}`, testVote)
	}))
	defer server.Close()
	collector, err := NewCollector(server.URL, testVote)
	if err != nil {
		t.Fatal(err)
	}
	collector.Now = func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Items) != 2 || batch.Items[0].Observation.ObservationTime.Day() != 21 {
		t.Fatalf("items=%+v", batch.Items)
	}
	latest := batch.Items[1]
	if latest.Observation.Rate.String() != "0.052" || latest.Observation.PerformanceFeeRate.String() != "0.07" || latest.Observation.TVL.String() != "2" || latest.Observation.RewardComponentRates[0].String() != "0.052" {
		t.Fatalf("latest=%+v", latest)
	}
	if latest.Route.ProductCode != "validator:"+testVote || latest.Route.ContractAddress == nil || *latest.Route.ContractAddress != testVote || latest.Observation.BlockHeight != nil || latest.Observation.SourcePayloadHash == nil {
		t.Fatalf("latest=%+v", latest)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRejectsWrongValidatorAndInvalidCommission(t *testing.T) {
	for name, body := range map[string]string{
		"wrong validator": `{"total_count":1,"validators":[{"vote_account":"11111111111111111111111111111111","epoch_stats":[]}]}`,
		"commission":      fmt.Sprintf(`{"total_count":1,"validators":[{"vote_account":%q,"epoch_stats":[{"epoch":1,"epoch_end_at":"2026-08-23T00:00:00Z","apr":0.04,"apy":0.05,"commission_effective":101,"activated_stake":"1"}]}]}`, testVote),
		"duplicate epoch": fmt.Sprintf(`{"total_count":1,"validators":[{"vote_account":%q,"epoch_stats":[{"epoch":1,"epoch_end_at":"2026-08-22T00:00:00Z","apr":0.04,"apy":0.05,"commission_effective":1,"activated_stake":"1"},{"epoch":1,"epoch_end_at":"2026-08-23T00:00:00Z","apr":0.04,"apy":0.05,"commission_effective":1,"activated_stake":"1"}]}]}`, testVote),
		"invalid stake":   fmt.Sprintf(`{"total_count":1,"validators":[{"vote_account":%q,"epoch_stats":[{"epoch":1,"epoch_end_at":"2026-08-23T00:00:00Z","apr":0.04,"apy":0.05,"commission_effective":1,"activated_stake":"-1"}]}]}`, testVote),
		"stale":           fmt.Sprintf(`{"total_count":1,"validators":[{"vote_account":%q,"epoch_stats":[{"epoch":1,"epoch_end_at":"2026-08-01T00:00:00Z","apr":0.04,"apy":0.05,"commission_effective":1,"activated_stake":"1"}]}]}`, testVote),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			collector, err := NewCollector(server.URL, testVote)
			if err != nil {
				t.Fatal(err)
			}
			collector.Now = func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) }
			if _, err = collector.Collect(context.Background()); err == nil {
				t.Fatal("invalid validator response accepted")
			}
		})
	}
	if _, err := NewCollector("", "bad"); err == nil {
		t.Fatal("invalid vote account accepted")
	}
}
