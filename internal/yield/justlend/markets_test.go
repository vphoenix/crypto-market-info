package justlend

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/lendingmarket"
)

var marketTime = time.Date(2026, 11, 2, 9, 55, 0, 123000000, time.UTC)

func marketFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/jtoken-20261102.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func findMarket(t *testing.T, b lendingmarket.Batch, symbol string) lendingmarket.Observation {
	t.Helper()
	for _, m := range b.Markets {
		if m.MarketSymbol == symbol {
			return m
		}
	}
	t.Fatalf("market %s not found", symbol)
	return lendingmarket.Observation{}
}

// The fixture is the real 2026-11-02 09:55 UTC response (24 markets).
func TestParseMarketsRealResponseIsExact(t *testing.T) {
	raw := marketFixture(t)
	batch, err := ParseMarkets(raw, marketTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Markets) != 24 || !batch.CollectedAt.Equal(marketTime) {
		t.Fatalf("batch = %d markets at %s", len(batch.Markets), batch.CollectedAt)
	}
	digest := sha256.Sum256(raw)
	for _, m := range batch.Markets {
		if m.PayloadHash != string(digest[:]) || m.BatchMarketCount != 24 || !m.ObservationTime.Equal(marketTime) {
			t.Fatalf("%s carries wrong batch identity", m.MarketSymbol)
		}
	}
	usdt := findMarket(t, batch, "jUSDT")
	want := map[string][2]decimal.Decimal{
		"supply": {usdt.SupplyRateAPR, decimal.RequireFromString("0.489601360707408")},
		"borrow": {usdt.BorrowRateAPR, decimal.RequireFromString("0.578200104625536")},
		"cash":   {usdt.Cash, decimal.RequireFromString("8881573.682891")},
		"cf":     {usdt.CollateralFactor, decimal.RequireFromString("0.75")},
	}
	for name, pair := range want {
		if !pair[0].Equal(pair[1]) {
			t.Fatalf("jUSDT %s = %s, want %s", name, pair[0], pair[1])
		}
	}
	if usdt.UnderlyingAddress != "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" || usdt.UnderlyingDecimals != 6 {
		t.Fatalf("jUSDT underlying = %s/%d", usdt.UnderlyingAddress, usdt.UnderlyingDecimals)
	}
	trx := findMarket(t, batch, "jTRX")
	if !trx.BorrowRateAPR.Equal(decimal.RequireFromString("0.044039272202784")) || !trx.UnderlyingPriceTRX.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("jTRX borrow %s price %s", trx.BorrowRateAPR, trx.UnderlyingPriceTRX)
	}
	// The smallest source amount uses all 18 fractional digits and must survive exactly.
	tiny := 0
	for _, m := range batch.Markets {
		if m.TotalBorrows.Equal(decimal.RequireFromString("0.000000000092541702")) {
			tiny++
		}
	}
	if tiny != 1 || !strings.Contains(string(raw), `"totalBorrows":"0.000000000092541702"`) {
		t.Fatalf("18-digit amount parsed %d times", tiny)
	}
	// borrowIndex is stored as the raw 1e18-scaled integer divided by 1e18.
	if !usdt.BorrowIndex.Equal(decimal.RequireFromString("1.184253299792460419")) {
		t.Fatalf("jUSDT borrow index = %s", usdt.BorrowIndex)
	}
}

func TestParseMarketsRejectsIncompleteOrMalformedResponses(t *testing.T) {
	raw := string(marketFixture(t))
	jusdt := `"address":"TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccvd","symbol":"jUSDT"`
	if !strings.Contains(raw, jusdt) {
		t.Fatal("fixture layout changed")
	}
	cases := map[string]string{
		"business code":    strings.Replace(raw, `"code":0`, `"code":1`, 1),
		"missing code":     strings.Replace(raw, `"code":0,`, ``, 1),
		"required market":  strings.Replace(raw, jusdt, `"address":"TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccvd","symbol":"jUSDX"`, 1),
		"missing field":    strings.Replace(raw, `"borrowRate":`, `"borrowRateX":`, 1),
		"null field":       strings.Replace(raw, `"reserves":"`, `"reserves":null,"x":"`, 1),
		"negative":         strings.Replace(raw, `"cash":"`, `"cash":"-`, 1),
		"exponent form":    strings.Replace(raw, `"supplyRate":"0.003314103807024000"`, `"supplyRate":"3.314103807024E-3"`, 1),
		"numeric decimal":  strings.Replace(raw, `"supplyRate":"0.003314103807024000"`, `"supplyRate":0.003314103807024`, 1),
		"too many digits":  strings.Replace(raw, `"supplyRate":"0.003314103807024000"`, `"supplyRate":"0.0033141038070240001"`, 1),
		"factor above one": strings.Replace(raw, `"collateralFactor":"0.750000000000000000"`, `"collateralFactor":"1.5"`, 1),
		"bad checksum":     strings.Replace(raw, `TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwP`, `TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwQ`, 1),
		"duplicate":        strings.Replace(raw, `"tokenList":[`, `"tokenList":[`+firstMarket(t, raw)+`,`, 1),
		"trailing":         raw + `{}`,
		"missing list":     `{"code":0,"message":"SUCCESS","data":{}}`,
		"empty list":       `{"code":0,"message":"SUCCESS","data":{"tokenList":[]}}`,
	}
	for name, body := range cases {
		if body == raw {
			t.Fatalf("%s: fixture replacement did not apply", name)
		}
		if _, err := ParseMarkets([]byte(body), marketTime); err == nil {
			t.Fatalf("%s: malformed response accepted", name)
		}
	}
}

func firstMarket(t *testing.T, raw string) string {
	t.Helper()
	start := strings.Index(raw, `"tokenList":[`) + len(`"tokenList":[`)
	end := strings.Index(raw[start:], `},{`)
	if end < 0 {
		t.Fatal("fixture has a single market")
	}
	return raw[start : start+end+1]
}

func TestMarketCollectorUsesReceiptTimeAndOnlyTheMarketEndpoint(t *testing.T) {
	raw := marketFixture(t)
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		fmt.Fprint(w, string(raw))
	}))
	defer server.Close()
	client := NewClient(server.URL)
	client.Retry.Cooldown = exchange.NewRequestGate(0)
	collector := &MarketCollector{Client: client, Now: func() time.Time { return marketTime.Add(456 * time.Microsecond) }}
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/lend/jtoken" || !batch.CollectedAt.Equal(marketTime) || batch.CollectedAt.Location() != time.UTC {
		t.Fatalf("paths %v collected %s", paths, batch.CollectedAt)
	}
}
