package bybit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testHTTPResponse(status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

func instrumentEnvelope(symbol string, symbolID int64, launchTime, cursor string) string {
	return instrumentsEnvelope([]string{instrumentRow(symbol, symbolID, launchTime)}, cursor)
}

func instrumentRow(symbol string, symbolID int64, launchTime string) string {
	return fmt.Sprintf(`{"symbol":%q,"symbolId":%d,"contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","launchTime":%q,"fundingInterval":480,"isPreListing":false,"priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"qtyStep":"0.001"}}`, symbol, symbolID, launchTime)
}

func instrumentsEnvelope(rows []string, cursor string) string {
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[%s],"nextPageCursor":%q}}`, strings.Join(rows, ","), cursor)
}

func TestClientClassifiesForbiddenByBodyInsteadOfStatusAlone(t *testing.T) {
	client := NewClient()
	if _, exists := client.Retry.RateLimitStatuses[http.StatusForbidden]; exists {
		t.Fatal("Bybit client classified every HTTP 403 as a rate limit")
	}
	if client.ForbiddenCooldown != 10*time.Minute {
		t.Fatalf("Bybit access-too-frequent cooldown=%s", client.ForbiddenCooldown)
	}
	if client.Retry.RateLimitStatuses[http.StatusTooManyRequests] != 0 || client.Retry.RateLimitFallback != time.Minute {
		t.Fatalf("Bybit 429 policy changed: statuses=%v fallback=%s", client.Retry.RateLimitStatuses, client.Retry.RateLimitFallback)
	}
}

func TestParseInstrumentsPageFiltersExactUSDTPerpetualAndBuildsVersion(t *testing.T) {
	payload := []byte(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
{"symbol":"BTCUSDT","symbolId":5,"contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","launchTime":"1585526400000","fundingInterval":480,"isPreListing":false,"priceFilter":{"tickSize":"0.10"},"lotSizeFilter":{"qtyStep":"0.001"}},
{"symbol":"BTCUSDC","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDC","settleCoin":"USDC","isPreListing":false},
{"symbol":"ETHUSDT-30DEC","contractType":"LinearFutures","status":"Trading","baseCoin":"ETH","quoteCoin":"USDT","settleCoin":"USDT","isPreListing":false},
{"symbol":"NEWUSDT","contractType":"LinearPerpetual","status":"PreLaunch","baseCoin":"NEW","quoteCoin":"USDT","settleCoin":"USDT","isPreListing":true}
],"nextPageCursor":""}}`)
	got, cursor, err := ParseInstrumentsPage(payload)
	if err != nil || cursor != "" || len(got) != 1 {
		t.Fatalf("instruments=%+v cursor=%q err=%v", got, cursor, err)
	}
	if got[0].Exchange != "Bybit" || got[0].MarketType != model.MarketPerpetual || got[0].VenueContractVersion != "5:1585526400000" || got[0].ExchangeSymbol != "BTCUSDT" {
		t.Fatalf("instrument=%+v", got[0])
	}
}

func TestParseInstrumentsPageRejectsMissingIdentityAndPrecision(t *testing.T) {
	for name, payload := range map[string]string{
		"category":   strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `"category":"linear"`, `"category":"spot"`, 1),
		"symbol id":  strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `"symbolId":5,`, "", 1),
		"launch":     strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `"launchTime":"1585526400000"`, `"launchTime":"0"`, 1),
		"prelisting": strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `,"isPreListing":false`, "", 1),
		"interval":   strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `"fundingInterval":480`, `"fundingInterval":61`, 1),
		"tick":       strings.Replace(instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), `"tickSize":"0.1"`, `"tickSize":"bad"`, 1),
		"null list":  `{"retCode":0,"result":{"category":"linear","list":null,"nextPageCursor":""}}`,
		"ret code":   `{"retCode":10001,"retMsg":"bad","result":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseInstrumentsPage([]byte(payload)); err == nil {
				t.Fatal("malformed metadata was accepted")
			}
		})
	}
}

func TestInstrumentsPaginatesCursorExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	var requests []*url.URL
	responses := []string{
		instrumentEnvelope("BTCUSDT", 5, "1585526400000", "first%3DBTC%26last%3DBTC"),
		instrumentEnvelope("ETHUSDT", 6, "1585526400001", ""),
	}
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		copyURL := *request.URL
		requests = append(requests, &copyURL)
		return testHTTPResponse(http.StatusOK, responses[len(requests)-1], nil), nil
	})}
	got, err := client.Instruments(context.Background(), model.MarketPerpetual)
	if err != nil || len(got) != 2 {
		t.Fatalf("instruments=%+v err=%v", got, err)
	}
	if len(requests) != 2 || requests[1].Query().Get("cursor") != "first=BTC&last=BTC" || strings.Contains(requests[1].RawQuery, "%253D") {
		t.Fatalf("requests=%+v", requests)
	}
	for index, request := range requests {
		query := request.Query()
		if query.Get("category") != "linear" || query.Get("status") != "Trading" || query.Get("limit") != "1000" {
			t.Fatalf("request %d query=%v", index, query)
		}
	}
}

func TestInstrumentsRejectsRepeatedCursor(t *testing.T) {
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	calls := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		symbol := "BTCUSDT"
		if calls > 1 {
			symbol = "ETHUSDT"
		}
		return testHTTPResponse(http.StatusOK, instrumentEnvelope(symbol, int64(4+calls), fmt.Sprint(1585526399999+calls), "same"), nil), nil
	})}
	if _, err := client.Instruments(context.Background(), model.MarketPerpetual); err == nil || !strings.Contains(err.Error(), "repeated cursor") {
		t.Fatalf("error=%v", err)
	}
}

func TestInstrumentsLoadsMoreThan500RowsAndRejectsCrossPageDuplicate(t *testing.T) {
	firstRows := make([]string, 501)
	for index := range firstRows {
		firstRows[index] = instrumentRow(fmt.Sprintf("ASSET%03dUSDT", index), int64(index+1), fmt.Sprint(1585526400000+index))
	}
	responses := []string{
		instrumentsEnvelope(firstRows, "next"),
		instrumentEnvelope("TARGETUSDT", 999, "1585526500000", ""),
	}
	calls := 0
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := responses[calls]
		calls++
		return testHTTPResponse(http.StatusOK, response, nil), nil
	})}
	got, err := client.Instruments(context.Background(), model.MarketPerpetual)
	if err != nil || len(got) != 502 || got[501].ExchangeSymbol != "TARGETUSDT" {
		t.Fatalf("rows=%d calls=%d err=%v", len(got), calls, err)
	}

	duplicateResponses := []string{
		instrumentEnvelope("BTCUSDT", 5, "1585526400000", "next"),
		instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""),
	}
	calls = 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := duplicateResponses[calls]
		calls++
		return testHTTPResponse(http.StatusOK, response, nil), nil
	})}
	if _, err = client.Instruments(context.Background(), model.MarketPerpetual); err == nil || !strings.Contains(err.Error(), "duplicate paginated symbol") {
		t.Fatalf("cross-page duplicate error=%v", err)
	}
}

func TestMetadataRateLimitWaitsInProcessAndCanBeCancelled(t *testing.T) {
	var mu sync.Mutex
	var calls []time.Time
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client.ForbiddenCooldown = 20 * time.Millisecond
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, time.Now())
		if len(calls) == 1 {
			return testHTTPResponse(http.StatusForbidden, "access too frequent", nil), nil
		}
		return testHTTPResponse(http.StatusOK, instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), nil), nil
	})}
	if _, err := client.Instruments(context.Background(), model.MarketPerpetual); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1].Sub(calls[0]) < 15*time.Millisecond {
		t.Fatalf("metadata calls=%v", calls)
	}

	cancelClient := NewClient()
	cancelClient.BaseURL = "https://bybit.test"
	cancelClient.Logger = client.Logger
	cancelClient.ForbiddenCooldown = time.Second
	var cancelCalls int
	cancelClient.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancelCalls++
		return testHTTPResponse(http.StatusForbidden, "access too frequent", nil), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := cancelClient.Instruments(ctx, model.MarketPerpetual); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel error=%v", err)
	}
	if cancelCalls != 1 {
		t.Fatalf("cancelled metadata made %d calls", cancelCalls)
	}
}

func TestMetadataCountryBlockedForbiddenFailsFast(t *testing.T) {
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	calls := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return testHTTPResponse(http.StatusForbidden, "The Amazon CloudFront distribution is configured to block access from your country", nil), nil
	})}
	_, err := client.Instruments(context.Background(), model.MarketPerpetual)
	if err == nil {
		t.Fatal("country-blocked 403 unexpectedly succeeded")
	}
	var limited *exchange.RateLimitError
	if errors.As(err, &limited) {
		t.Fatalf("country-blocked 403 was classified as rate limit: %v", err)
	}
	if calls != 1 || !strings.Contains(err.Error(), "block access from your country") {
		t.Fatalf("country-blocked calls=%d error=%v", calls, err)
	}
}

func TestBodyRateLimitUsesResetHeaderAndFallback(t *testing.T) {
	for _, test := range []struct {
		name   string
		header string
	}{
		{name: "header", header: fmt.Sprint(time.Now().Add(time.Second).UnixMilli())},
		{name: "fallback", header: "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := NewClient()
			client.BaseURL = "https://bybit.test"
			client.BodyRateLimitFallback = 30 * time.Millisecond
			client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				header := http.Header{"X-Bapi-Limit-Reset-Timestamp": []string{test.header}}
				return testHTTPResponse(http.StatusOK, `{"retCode":10006,"retMsg":"Too many visits!","result":{}}`, header), nil
			})}
			_, err := client.apiGet(context.Background(), "/v5/test", nil, client.Retry)
			var limited *exchange.RateLimitError
			if !errors.As(err, &limited) || limited.Code != 10006 || limited.RetryAt.IsZero() {
				t.Fatalf("error=%v", err)
			}
			if test.name == "fallback" && time.Until(limited.RetryAt) > 100*time.Millisecond {
				t.Fatalf("fallback retry_at=%v", limited.RetryAt)
			}
		})
	}
}

func TestMetadataWaitsForBodyRateLimitInSameProcess(t *testing.T) {
	var calls []time.Time
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client.BodyRateLimitFallback = 20 * time.Millisecond
	client.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls = append(calls, time.Now())
		if len(calls) == 1 {
			return testHTTPResponse(http.StatusOK, `{"retCode":10006,"retMsg":"Too many visits!","result":{}}`, nil), nil
		}
		return testHTTPResponse(http.StatusOK, instrumentEnvelope("BTCUSDT", 5, "1585526400000", ""), nil), nil
	})}
	if _, err := client.Instruments(context.Background(), model.MarketPerpetual); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1].Sub(calls[0]) < 15*time.Millisecond {
		t.Fatalf("metadata body-limit calls=%v", calls)
	}
}
