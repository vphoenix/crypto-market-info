package kamino

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

type fakeAccounts struct {
	account solana.Account
	err     error
}

func (f fakeAccounts) AccountInfo(context.Context, string) (solana.Account, error) {
	return f.account, f.err
}

func TestCollectorMapsHourlyHistoryWithoutFakeBlockAnchor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kamino-market/"+Market+"/reserves/"+Reserve+"/metrics/history" || r.URL.Query().Get("frequency") != "hour" || r.URL.Query().Get("env") != "mainnet-beta" || r.URL.Query().Get("start") == "" || r.URL.Query().Get("end") == "" {
			t.Fatalf("request=%s", r.URL.String())
		}
		fmt.Fprintf(w, `{"reserve":%q,"history":[`+
			`{"timestamp":"2026-08-25T22:00:00Z","metrics":{"status":"Inactive","symbol":"SOL","decimals":9,"mintAddress":%q,"totalSupply":"50","exchangeRate":"0.4","isUIDeprecated":false,"supplyInterestAPY":0.05,"reserveDepositLimit":"1000000000000","depositTvl":999999}},`+
			`{"timestamp":"2026-08-25T23:00:00Z","metrics":{"status":"Active","symbol":"SOL","decimals":9,"mintAddress":%q,"totalSupply":"100","exchangeRate":"0.5","isUIDeprecated":false,"supplyInterestAPY":"0.075","reserveDepositLimit":"1000000000000","depositTvl":999999}}]}`, Reserve, solana.WSOLMintAddress, solana.WSOLMintAddress)
	}))
	defer server.Close()
	collector := NewCollector(server.URL, fakeAccounts{account: solana.Account{Owner: Program}})
	collector.Now = func() time.Time { return time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC) }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Items) != 2 || batch.Items[0].Observation.Availability != "unavailable" {
		t.Fatalf("batch=%+v", batch)
	}
	item := batch.Items[1]
	o := item.Observation
	if o.Rate.String() != "0.075" || o.ExposureRatio.String() != "2" || o.TVL.String() != "100" || o.Capacity.String() != "1000" || o.RemainingCapacity.String() != "900" || o.Availability != "available" {
		t.Fatalf("observation=%+v", o)
	}
	if o.BlockHeight != nil || o.BlockHash != nil || o.Finality != nil || o.SourcePayloadHash == nil || o.UnbondingSeconds == nil || *o.UnbondingSeconds != 0 {
		t.Fatalf("evidence=%+v", o)
	}
	if item.Route.DepositAssetKey != solana.WSOLAsset || item.Route.RedeemAssetKey != solana.WSOLAsset || item.Route.PriceExposureAsset == nil || *item.Route.PriceExposureAsset != solana.SOLAsset {
		t.Fatalf("route=%+v", item.Route)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRejectsInvalidIdentityAndHistory(t *testing.T) {
	now := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	valid := fmt.Sprintf(`{"reserve":%q,"history":[{"timestamp":"2026-08-25T23:00:00Z","metrics":{"status":"Active","symbol":"SOL","decimals":9,"mintAddress":%q,"totalSupply":"100","exchangeRate":"0.5","isUIDeprecated":false,"supplyInterestAPY":"0.075","reserveDepositLimit":"1000000000000"}}]}`, Reserve, solana.WSOLMintAddress)
	fixtures := map[string]string{
		"wrong reserve": strings.Replace(valid, Reserve, solana.BSOLPoolAddress, 1),
		"wrong mint":    strings.Replace(valid, solana.WSOLMintAddress, solana.BSOLMintAddress, 1),
		"zero ratio":    strings.Replace(valid, `"0.5"`, `"0"`, 1),
		"stale":         strings.Replace(valid, "2026-08-25T23:00:00Z", "2026-08-20T23:00:00Z", 1),
		"duplicate":     strings.Replace(valid, `]}`, `,{"timestamp":"2026-08-25T23:00:00Z","metrics":{"status":"Active","symbol":"SOL","decimals":9,"mintAddress":"`+solana.WSOLMintAddress+`","totalSupply":"1","exchangeRate":"1","isUIDeprecated":false,"supplyInterestAPY":"0.01","reserveDepositLimit":"1"}}]}`, 1),
	}
	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			collector := NewCollector(server.URL, fakeAccounts{account: solana.Account{Owner: Program}})
			collector.Now = func() time.Time { return now }
			if _, err := collector.Collect(context.Background()); err == nil {
				t.Fatal("invalid Kamino response accepted")
			}
		})
	}
	collector := NewCollector("https://unused.test", fakeAccounts{account: solana.Account{Owner: solana.StakePoolProgram}})
	collector.Now = func() time.Time { return now }
	if _, err := collector.Collect(context.Background()); err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("wrong owner error=%v", err)
	}
}
