package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/universe"
)

func dryRunConfig(t *testing.T, endpoint string) config.Config {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.BinanceFuturesREST = endpoint
	cfg.OKXREST = endpoint
	cfg.BybitREST = endpoint
	cfg.ClickHouse.Addresses = []string{"not-a-database.invalid:9000"}
	cfg.BinanceSpotSymbols = nil
	cfg.OKXSpotSymbols = nil
	cfg.PerpAssetAliasesFile = "../../config/perpetual-asset-aliases.json"
	for i := range cfg.PerpetualSelection.Venues {
		cfg.PerpetualSelection.Venues[i].Mode = universe.Auto
		cfg.PerpetualSelection.Venues[i].Include = []string{}
	}
	return cfg
}

func TestPerpetualDryRunHasCompleteMembershipWithoutDatabaseOrWebsocket(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			io.WriteString(w, `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","onboardDate":1585526400000,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}`)
		case "/api/v5/public/instruments":
			io.WriteString(w, `{"code":"0","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","state":"live","baseCcy":"","quoteCcy":"","settleCcy":"USDT","ctType":"linear","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC","tickSz":"0.1","lotSz":"1","listTime":"1597026383085"}]}`)
		case "/v5/market/instruments-info":
			io.WriteString(w, `{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","symbolId":5,"contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","launchTime":"1585526400000","fundingInterval":480,"isPreListing":false,"priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001"}}],"nextPageCursor":""}}`)
		default:
			t.Errorf("unexpected non-metadata request %s", r.URL)
			http.Error(w, "forbidden", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	cfg := dryRunConfig(t, server.URL)
	cfg.BinanceFuturesWS = "ws://not-allowed.invalid"
	cfg.OKXWS = cfg.BinanceFuturesWS
	cfg.BybitWS = cfg.BinanceFuturesWS
	var output bytes.Buffer
	if err := PrintPerpetualUniverse(context.Background(), cfg, &output, slog.Default()); err != nil {
		t.Fatal(err)
	}
	var d PerpetualDiscovery
	if err := json.Unmarshal(output.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 3 || len(d.Selected) != 3 || d.GroupCount != 1 || d.Capacity.WSConnections != 6 {
		t.Fatalf("requests=%d discovery=%+v", requests.Load(), d)
	}
	if strings.Contains(output.String(), "run_id") {
		t.Fatal("dry run invented a persisted run")
	}
	for _, s := range d.Selected {
		if s.Instrument.ID != 0 {
			t.Fatal("dry run registered an instrument")
		}
	}
}

func TestIncompleteCatalogFailsBeforeDatabaseAndWebsocket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"symbols":null}`) }))
	defer server.Close()
	cfg := dryRunConfig(t, server.URL)
	err := Run(context.Background(), cfg, slog.Default())
	if err == nil || !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("catalog must fail before attempting invalid database: %v", err)
	}
}

func TestPerpetualCapacityRefusesTruncation(t *testing.T) {
	cfg := dryRunConfig(t, "http://metadata.invalid")
	r := universe.Result{Selected: make([]universe.SelectedInstrument, 501)}
	for i := range r.Selected {
		r.Selected[i].Instrument.Exchange = "Bybit"
	}
	p, err := perpetualCapacity(cfg, r)
	if err == nil || p.TotalInstruments != 501 || !strings.Contains(err.Error(), "Bybit instruments 501 > 500") {
		t.Fatalf("capacity silently truncated: %+v %v", p, err)
	}
	cfg.PerpetualSelection.Venues[1].MaxInstruments = 700
	cfg.MinuteQueueCapacity = 1001
	if _, err = perpetualCapacity(cfg, r); err == nil {
		t.Fatal("queue below two complete source minutes accepted")
	}
}

func TestBybitCapacityUsesShardsAndOptionalFunding(t *testing.T) {
	for _, count := range []int{0, 1, 20, 21, 40} {
		for _, funding := range []bool{false, true} {
			cfg := dryRunConfig(t, "http://metadata.invalid")
			cfg.FundingEnabled = funding
			r := universe.Result{Selected: make([]universe.SelectedInstrument, count)}
			for i := range r.Selected {
				r.Selected[i].Instrument.Exchange = "Bybit"
			}
			p, err := perpetualCapacity(cfg, r)
			if err != nil {
				t.Fatal(err)
			}
			want := (count + 19) / 20
			if funding && count > 0 {
				want++
			}
			if p.WSConnections != want {
				t.Fatalf("count=%d funding=%t connections=%d want=%d", count, funding, p.WSConnections, want)
			}
		}
	}
}
