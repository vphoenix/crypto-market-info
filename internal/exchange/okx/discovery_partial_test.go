package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFailedForecastEndpointDoesNotPreventCompleteVolumeObservation(t *testing.T) {
	i, err := mapInstrument(pairWire("SWAP", "BTC"), model.MarketPerpetual)
	if err != nil {
		t.Fatal(err)
	}
	i.ID = 1
	var candleCalled atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v5/public/funding-rate":
			fmt.Fprint(w, `{"code":"51000","msg":"temporarily unavailable","data":[]}`)
		case "/api/v5/market/candles":
			candleCalled.Store(true)
			minute := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute).UnixMilli()
			fmt.Fprintf(w, `{"code":"0","data":[["%d","100","100","100","100","2","0.02","2","1"]]}`, minute)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			http.Error(w, "unexpected", 400)
		}
	}))
	defer srv.Close()
	client := NewClient()
	client.BaseURL = srv.URL
	client.HTTP = srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	forecast, bar, err := client.DiscoveryMarketFacts(ctx, i)
	if err == nil || forecast.ID != "" || !candleCalled.Load() || bar.ID == "" || bar.Validate() != nil || !bar.Final || bar.BaseVolume.String() != "0.02" {
		t.Fatalf("failure lost independent volume or fabricated forecast: %v %+v %+v called=%v", err, forecast, bar, candleCalled.Load())
	}
}
