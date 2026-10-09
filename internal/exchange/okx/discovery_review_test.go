package okx

import (
	"encoding/json"
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
