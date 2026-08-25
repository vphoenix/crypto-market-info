package save

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

type fakeRPC struct {
	account solana.Account
	block   solana.BlockAnchor
	slot    uint64
}

func (f *fakeRPC) AccountInfo(context.Context, string) (solana.Account, error) { return f.account, nil }
func (f *fakeRPC) Block(_ context.Context, slot uint64) (solana.BlockAnchor, error) {
	f.slot = slot
	return f.block, nil
}

func validCurrentResponse(mintSupply string) string {
	return fmt.Sprintf(`{"results":[{"reserve":{"lastUpdate":{"slot":"123","stale":1},"lendingMarket":%q,"liquidity":{"mintPubkey":%q,"mintDecimals":9,"availableAmount":"100000000000","borrowedAmountWads":"50000000000000000000000000000","accumulatedProtocolFeesWads":"1000000000000000000000000000"},"collateral":{"mintPubkey":%q,"mintTotalSupply":%q},"config":{"depositLimit":"200000000000"},"pubkey":%q,"address":%q},"cTokenExchangeRate":"1.49","rates":{"supplyInterest":"1.82"},"rewards":[{"apy":"999"}]}],"next":null}`, Market, solana.WSOLMintAddress, CollateralMint, mintSupply, Reserve, Reserve)
}

func TestCollectorMapsCurrentAndHistoryWithDistinctUnitsAndAnchors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/reserves":
			if r.URL.Query().Get("scope") != "reserve" || r.URL.Query().Get("ids") != Reserve {
				t.Fatalf("current query=%s", r.URL.RawQuery)
			}
			fmt.Fprint(w, validCurrentResponse("100000000000"))
		case "/v1/reserves/historical-interest-rates":
			if r.URL.Query().Get("ids") != Reserve || r.URL.Query().Get("span") != "30d" {
				t.Fatalf("history query=%s", r.URL.RawQuery)
			}
			fmt.Fprintf(w, `{%q:[{"supplyAPY":0.025,"reserveID":%q,"cTokenExchangeRate":"1.48","timestamp":1787587200},{"supplyAPY":0.026,"reserveID":%q,"cTokenExchangeRate":"1.49","timestamp":1787673600}]}`, Reserve, Reserve, Reserve)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rpc := &fakeRPC{account: solana.Account{Owner: Program}, block: solana.BlockAnchor{Height: 100, Hash: "hash123", Time: 1787673600, Payload: []byte(`{"block":123}`)}}
	collector := NewCollector(server.URL, rpc)
	collector.Now = func() time.Time { return time.Unix(1787677200, 0).UTC() }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rpc.slot != 123 || len(batch.Items) != 2 {
		t.Fatalf("slot=%d items=%d", rpc.slot, len(batch.Items))
	}
	history, current := batch.Items[0], batch.Items[1]
	if history.Observation.Rate.String() != "0.025" || history.Observation.ExposureRatio.String() != "1.48" || history.Observation.BlockHeight != nil || history.Observation.TVL != nil {
		t.Fatalf("history=%+v", history.Observation)
	}
	o := current.Observation
	if o.Rate.String() != "0.0182" || o.ExposureRatio.String() != "1.49" || o.TVL.String() != "149" || o.Capacity.String() != "200" || o.RemainingCapacity.String() != "51" {
		t.Fatalf("current=%+v", o)
	}
	if o.BlockHeight == nil || *o.BlockHeight != 100 || o.BlockHash == nil || *o.BlockHash != "hash123" || o.Finality == nil || *o.Finality != "finalized_anchor" || o.Availability != "unknown" || o.UnbondingSeconds == nil || *o.UnbondingSeconds != 0 {
		t.Fatalf("current evidence=%+v", o)
	}
	if current.Route.PositionAssetKey != CollateralAsset || current.Route.DepositAssetKey != solana.WSOLAsset || current.Route.PriceExposureAsset == nil || *current.Route.PriceExposureAsset != solana.SOLAsset {
		t.Fatalf("route=%+v", current.Route)
	}
	if len(o.RewardAssetKeys) != 1 || o.RewardAssetKeys[0] != solana.WSOLAsset || len(o.RewardComponentRates) != 1 || o.RewardComponentRates[0].String() != "0.0182" {
		t.Fatalf("rewards=%+v %+v", o.RewardAssetKeys, o.RewardComponentRates)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRejectsIdentityShapeAndTVLMismatch(t *testing.T) {
	history := fmt.Sprintf(`{%q:[{"supplyAPY":0.025,"reserveID":%q,"cTokenExchangeRate":"1.48","timestamp":1787587200}]}`, Reserve, Reserve)
	for name, current := range map[string]string{
		"tvl mismatch":    validCurrentResponse("100000000001"),
		"bad next":        strings.Replace(validCurrentResponse("100000000000"), `"next":null`, `"next":"cursor"`, 1),
		"extra top level": strings.TrimSuffix(validCurrentResponse("100000000000"), "}") + `,"extra":1}`,
		"wrong market":    strings.Replace(validCurrentResponse("100000000000"), Market, solana.BSOLPoolAddress, 1),
		"invalid stale":   strings.Replace(validCurrentResponse("100000000000"), `"stale":1`, `"stale":2`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/reserves" {
					fmt.Fprint(w, current)
					return
				}
				fmt.Fprint(w, history)
			}))
			defer server.Close()
			rpc := &fakeRPC{account: solana.Account{Owner: Program}, block: solana.BlockAnchor{Height: 100, Hash: "hash", Time: 1787673600, Payload: []byte("block")}}
			collector := NewCollector(server.URL, rpc)
			collector.Now = func() time.Time { return time.Unix(1787677200, 0).UTC() }
			if _, err := collector.Collect(context.Background()); err == nil {
				t.Fatal("invalid Save response accepted")
			}
		})
	}
}

func TestCollectorRejectsInvalidHistory(t *testing.T) {
	valid := fmt.Sprintf(`{%q:[{"supplyAPY":0.025,"reserveID":%q,"cTokenExchangeRate":"1.48","timestamp":1787587200}]}`, Reserve, Reserve)
	fixtures := map[string]string{
		"wrong key":    strings.Replace(valid, Reserve, solana.BSOLPoolAddress, 1),
		"negative APY": strings.Replace(valid, "0.025", "-0.025", 1),
		"zero ratio":   strings.Replace(valid, `"1.48"`, `"0"`, 1),
		"stale":        strings.Replace(valid, "1787587200", "1787000000", 1),
		"duplicate": fmt.Sprintf(`{%q:[{"supplyAPY":0.025,"reserveID":%q,"cTokenExchangeRate":"1.48","timestamp":1787587200},`+
			`{"supplyAPY":0.026,"reserveID":%q,"cTokenExchangeRate":"1.49","timestamp":1787587200}]}`, Reserve, Reserve, Reserve),
	}
	// Keep the dynamic key correct while changing only reserveID.
	fixtures["wrong reserve id"] = fmt.Sprintf(`{%q:[{"supplyAPY":0.025,"reserveID":%q,"cTokenExchangeRate":"1.48","timestamp":1787587200}]}`, Reserve, solana.BSOLPoolAddress)
	for name, history := range fixtures {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/reserves" {
					fmt.Fprint(w, validCurrentResponse("100000000000"))
					return
				}
				fmt.Fprint(w, history)
			}))
			defer server.Close()
			rpc := &fakeRPC{account: solana.Account{Owner: Program}, block: solana.BlockAnchor{Height: 100, Hash: "hash", Time: 1787673600, Payload: []byte("block")}}
			collector := NewCollector(server.URL, rpc)
			collector.Now = func() time.Time { return time.Unix(1787677200, 0).UTC() }
			if _, err := collector.Collect(context.Background()); err == nil {
				t.Fatal("invalid Save history accepted")
			}
		})
	}
}
