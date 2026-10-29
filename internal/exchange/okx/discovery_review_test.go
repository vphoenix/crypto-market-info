package okx

import (
	"encoding/json"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"strings"
	"testing"
)

func TestDiscoveryDiscountMissingRestrictionCannotBecomePermission(t *testing.T) {
	for _, field := range []string{"", `"collateralRestrict":null,`, `"collateralRestrict":true,`, `"collateralRestrict":false,`} {
		r := PublicResponse{Data: json.RawMessage(`[{"ccy":"BTC",` + field + `"details":[{"minAmt":"0","maxAmt":"20","discountRate":".98","liqPenaltyRate":".02"}]}]`)}
		_, err := ParseDiscoveryDiscount(r, "BTC")
		if (field == `"collateralRestrict":false,`) != (err == nil) {
			t.Fatal("missing/null restriction authorized", field, err)
		}
	}
}
func TestDiscoveryBorrowRiskUsesCurrencyRequestAndNativeMaxSz(t *testing.T) {
	body := `[{"instFamily":"","instId":"","minSz":"0","maxSz":"20","baseMaxLoan":"","imr":".1","mmr":".02","maxLever":"10"}]`
	source := "https://www.okx.com/api/v5/public/position-tiers?ccy=BTC&instType=MARGIN&tdMode=cross"
	i := model.Instrument{BaseAsset: "BTC"}
	f := PublicResponse{Data: json.RawMessage(body), Source: model.PublicSource{URL: source}}
	tiers, err := ParseDiscoveryRisk(f, i, true)
	if err != nil || tiers[0].Maximum.String() != "20" {
		t.Fatal("actual currency wire rejected", err)
	}
	for _, bad := range []PublicResponse{
		{Data: json.RawMessage(body), Source: model.PublicSource{URL: strings.ReplaceAll(source, "ccy=BTC", "ccy=ETH")}},
		{Data: json.RawMessage(strings.ReplaceAll(body, `"instId":""`, `"instId":"BTC-USDT"`)), Source: f.Source},
		{Data: json.RawMessage(strings.ReplaceAll(body, `"instId":""`, `"ccy":"ETH","instId":""`)), Source: f.Source},
	} {
		if _, err := ParseDiscoveryRisk(bad, i, true); err == nil {
			t.Fatal("wrong risk scope accepted")
		}
	}
}
func TestDiscoveryPerpRiskAcceptsStoredMultiplierScale(t *testing.T) {
	body := `[{"instFamily":"ETH-USDT","instId":"","minSz":"0","maxSz":"5000","imr":".01","mmr":".004","maxLever":"100"},` +
		`{"instFamily":"ETH-USDT","instId":"","minSz":"5000.01","maxSz":"10000","imr":".015","mmr":".005","maxLever":"66.66"}]`
	// Multipliers read back from DECIMAL(38,18) keep eighteen fractional digits.
	i := model.Instrument{BaseAsset: "ETH", ContractMultiplier: decimal.RequireFromString("0.100000000000000000"), QuantityStepSize: decimal.RequireFromString("0.010000000000000000")}
	tiers, err := ParseDiscoveryRisk(PublicResponse{Data: json.RawMessage(body)}, i, false)
	if err != nil || tiers[1].Minimum.String() != "500.001" {
		t.Fatal(tiers, err)
	}
	for _, x := range tiers {
		for _, n := range []decimal.Decimal{x.Minimum, x.Maximum} {
			if !model.ReferenceDecimal(n) {
				t.Fatal("exact tier bound rejected", n, n.Exponent())
			}
		}
	}
}
