package okx

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const loanFixture = `{"code":"0","data":[{"configCcyList":[{"ccy":"USDT","rate":"0.0001"}],"basic":[{"ccy":"BTC","rate":"0.00001392","quota":"175"},{"ccy":"ONLY","rate":"0","quota":"0"}],"vip":[{"level":"VIP 1","irDiscount":"","loanQuotaCoef":"5"}],"regular":[{"level":"Lv1","irDiscount":"","loanQuotaCoef":"1"}],"config":[{"level":"Lv1","ccy":"USDT","stgyType":"1","quota":""}]}]}`

func pairWire(kind, base string) instrumentWire {
	version := "1597026383085"
	w := instrumentWire{InstType: kind, InstID: base + "-USDT", State: "live", BaseCcy: base, QuoteCcy: "USDT", TickSz: "0.1", LotSz: "0.0001", MinSz: "0.001", Category: "1", RuleType: "normal"}
	if kind == "SWAP" {
		w.InstID += "-SWAP"
		w.CtType = "linear"
		w.SettleCcy = "USDT"
		w.CtVal = "0.01"
		w.CtMult = "1"
		w.CtValCcy = base
		w.ListTime = &version
		w.LotSz = "1"
		w.MinSz = "1"
	}
	return w
}
func TestPairedSelectionIndependentOfCrossVenueUniverse(t *testing.T) {
	p, err := ParseLoanPolicy([]byte(loanFixture))
	if err != nil {
		t.Fatal(err)
	}
	spot := []instrumentWire{pairWire("SPOT", "BTC"), pairWire("SPOT", "ONLY")}
	swap := []instrumentWire{pairWire("SWAP", "BTC"), pairWire("SWAP", "ONLY")}
	margin := []instrumentWire{pairWire("MARGIN", "BTC"), pairWire("MARGIN", "ONLY")}
	pairs, err := MatchPairedCatalogs(spot, swap, margin, p, 256)
	if err != nil || len(pairs) != 1 || pairs[0].Base != "BTC" || pairs[0].Spot.MarketType != model.MarketSpot || pairs[0].Perpetual.MarketType != model.MarketPerpetual {
		t.Fatalf("bad selection: %+v %v", pairs, err)
	}
	if _, err = MatchPairedCatalogs(append(spot, spot[0]), swap, margin, p, 256); err == nil {
		t.Fatal("duplicate catalog accepted")
	}
	margin[0].Category = "3"
	if _, err = MatchPairedCatalogs(spot, swap, margin, p, 256); err == nil {
		t.Fatal("empty eligible scope accepted")
	}
}
func TestLoanTermsNullableAndStrictDecimals(t *testing.T) {
	p, err := ParseLoanPolicy([]byte(loanFixture))
	if err != nil {
		t.Fatal(err)
	}
	if p.Basic[0].DailyRate.String() != "0.00001392" || p.Levels[0].InterestDiscount != nil || p.Levels[0].QuotaCoefficient.String() != "5" || p.Levels[2].Quota != nil {
		t.Fatal("terms changed or unknown became zero")
	}
	for _, bad := range []string{
		strings.Replace(loanFixture, `"rate":"0.00001392"`, `"rate":0.00001392`, 1),
		strings.Replace(loanFixture, `"quota":"175"`, `"quota":"-1"`, 1),
		strings.Replace(loanFixture, `"code":"0"`, `"code":"0","Code":"1"`, 1),
		strings.Replace(loanFixture, `"regular":[{"level":"Lv1","irDiscount":"","loanQuotaCoef":"1"}]`, `"regular":null`, 1),
	} {
		if _, err := ParseLoanPolicy([]byte(bad)); err == nil {
			t.Fatal("malformed/incomplete policy accepted")
		}
	}
}
func TestPairedCatalogUsesOnlyPublicEndpointsAndHashes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("OK-ACCESS-KEY") != "" {
			t.Error("private authorization used")
		}
		if r.URL.Path == "/api/v5/public/interest-rate-loan-quota" {
			fmt.Fprint(w, loanFixture)
			return
		}
		if r.URL.Path != "/api/v5/public/instruments" {
			t.Errorf("unexpected endpoint %s", r.URL)
			http.Error(w, "bad", 400)
			return
		}
		kind := r.URL.Query().Get("instType")
		wire := pairWire(kind, "BTC")
		fmt.Fprintf(w, `{"code":"0","data":[{"instType":%q,"instId":%q,"state":"live","instCategory":"1","ruleType":"normal","baseCcy":"BTC","quoteCcy":"USDT","ctType":"linear","settleCcy":"USDT","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC","tickSz":"0.1","lotSz":%q,"minSz":%q,"listTime":"1597026383085"}]}`, kind, wire.InstID, wire.LotSz, wire.MinSz)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	got, err := c.PairedCatalog(context.Background(), 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Observation.Pairs) != 1 || len(got.Observation.SourceHashes) != 4 || !model.ValidDigest(got.Policy.PayloadHash) || got.Observation.LoanPolicyID != got.Policy.ID {
		t.Fatal("missing complete public evidence")
	}
}

func TestPartialCatalogSelectionFieldsRejectWholeBatch(t *testing.T) {
	policy, err := ParseLoanPolicy([]byte(loanFixture))
	if err != nil {
		t.Fatal(err)
	}
	policy.Basic = append(policy.Basic, model.OKXBasicLoan{Currency: "ETH", DailyRate: policy.Basic[0].DailyRate, PublicQuota: policy.Basic[0].PublicQuota})
	for _, field := range []string{"state", "category", "rule", "quote", "base", "contract_type", "settle", "contract_base"} {
		t.Run(field, func(t *testing.T) {
			spot := []instrumentWire{pairWire("SPOT", "BTC"), pairWire("SPOT", "ETH")}
			swaps := []instrumentWire{pairWire("SWAP", "BTC"), pairWire("SWAP", "ETH")}
			margin := []instrumentWire{pairWire("MARGIN", "BTC"), pairWire("MARGIN", "ETH")}
			switch field {
			case "state":
				spot[1].State = ""
			case "category":
				spot[1].Category = ""
			case "rule":
				spot[1].RuleType = ""
			case "quote":
				spot[1].QuoteCcy = ""
			case "base":
				margin[1].BaseCcy = ""
			case "contract_type":
				swaps[1].CtType = ""
			case "settle":
				swaps[1].SettleCcy = ""
			case "contract_base":
				swaps[1].CtValCcy = ""
			}
			if _, err := MatchPairedCatalogs(spot, swaps, margin, policy, 256); err == nil {
				t.Fatal("malformed catalog silently shrank universe")
			}
		})
	}
}
