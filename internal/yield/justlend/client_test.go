package justlend

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientUsesDifferentV1AndV2SuccessCodes(t *testing.T) {
	responses := map[string]string{
		"/lend/strx":           `{"code":0,"data":{"stakeInfo":{}}}`,
		"/lend/jtoken":         `{"code":0,"data":{"tokenList":[]}}`,
		"/mining/apy":          `{"code":0,"data":{}}`,
		"/v2/index/vault/list": `{"code":200,"timestamp":1000,"data":{"allVaults":{"list":[]}}}`,
	}
	client := NewClient("https://justlend.invalid")
	client.Retry = exchange.HTTPRetryConfig{MaxAttempts: 1, Cooldown: exchange.NewRequestGate(0)}
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, ok := responses[request.URL.Path]
		if !ok {
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	if _, err := client.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	responses["/lend/jtoken"] = `{"code":200,"message":"wrong V1 code","data":{"tokenList":[]}}`
	if _, err := client.Fetch(context.Background()); err == nil {
		t.Fatal("V1 accepted V2 success code")
	}
	responses["/lend/jtoken"] = `{"code":0,"data":{"tokenList":[]}}`
	responses["/v2/index/vault/list"] = `{"code":0,"timestamp":1000,"data":{"allVaults":{"list":[]}}}`
	if _, err := client.Fetch(context.Background()); err == nil {
		t.Fatal("V2 accepted V1 success code")
	}
}

func TestClientRejectsInvalidV2TimestampAndTrailingJSON(t *testing.T) {
	for name, v2 := range map[string]string{
		"missing timestamp":  `{"code":200,"data":{"allVaults":{"list":[]}}}`,
		"zero timestamp":     `{"code":200,"timestamp":0,"data":{"allVaults":{"list":[]}}}`,
		"overflow timestamp": `{"code":200,"timestamp":9223372036854775808,"data":{"allVaults":{"list":[]}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixtureClient(t, nil, v2).Fetch(context.Background()); err == nil {
				t.Fatal("invalid timestamp accepted")
			}
		})
	}
	t.Run("trailing JSON", func(t *testing.T) {
		if _, err := fixtureClient(t, map[string]string{"/lend/jtoken": `{"code":0,"data":{"tokenList":[]}} {}`}, "").Fetch(context.Background()); err == nil {
			t.Fatal("trailing JSON accepted")
		}
	})
}

func TestClientRejectsMissingBusinessCode(t *testing.T) {
	for name, overrides := range map[string]map[string]string{
		"V1": {"/lend/strx": `{"data":{"stakeInfo":{}}}`},
		"V2": {"/v2/index/vault/list": `{"timestamp":1000,"data":{"allVaults":{"list":[]}}}`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixtureClient(t, overrides, "").Fetch(context.Background()); err == nil {
				t.Fatal("missing business code accepted")
			}
		})
	}
}

func fixtureClient(t *testing.T, overrides map[string]string, v2 string) *Client {
	t.Helper()
	responses := map[string]string{"/lend/strx": `{"code":0,"data":{"stakeInfo":{}}}`, "/lend/jtoken": `{"code":0,"data":{"tokenList":[]}}`, "/mining/apy": `{"code":0,"data":{}}`, "/v2/index/vault/list": `{"code":200,"timestamp":1000,"data":{"allVaults":{"list":[]}}}`}
	for path, body := range overrides {
		responses[path] = body
	}
	if v2 != "" {
		responses["/v2/index/vault/list"] = v2
	}
	client := NewClient("https://justlend.invalid")
	client.Retry = exchange.HTTPRetryConfig{MaxAttempts: 1, Cooldown: exchange.NewRequestGate(0)}
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(responses[request.URL.Path])), Header: make(http.Header)}, nil
	})}
	return client
}
