package avalanche

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const testHash = "0x4c621d56b2a6749290b40b6e31b78cd7ecafe02bbcd723ff6c82ea6fa2063d48"
const testAddress = "0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be"

var testBlockTime = time.Date(2026, 8, 26, 16, 57, 19, 0, time.UTC)

func anchorFixture(at time.Time) string {
	// Deliberately shuffled: request identity, not response array position, is
	// authoritative. Extra source fields must not break strict required fields.
	return fmt.Sprintf(`[{"jsonrpc":"2.0","id":2,"result":{"number":"0x%x","hash":%q,"timestamp":"0x%x","unrelated":true}},{"jsonrpc":"2.0","id":1,"result":"0xa86a"}]`, 93742828, testHash, at.Unix())
}

func word(n int64) string { return fmt.Sprintf("0x%064x", n) }

func stateFixture() string {
	return fmt.Sprintf(`[{"jsonrpc":"2.0","id":2,"result":"0x1"},{"jsonrpc":"2.0","id":1,"result":%q}]`, word(18))
}

func testReads() []Read {
	return []Read{{Address: testAddress, Data: "0x313ce567"}, {Address: testAddress}}
}

func configuredClient(endpoint string) *Client {
	c := NewClient(endpoint)
	c.Retry.MaxAttempts = 1
	c.Retry.Cooldown = exchange.NewRequestGate(0)
	c.Now = func() time.Time { return testBlockTime.Add(time.Minute + 123*time.Millisecond) }
	return c
}

func TestReadUsesOneCanonicalFinalizedHashAndRawEvidence(t *testing.T) {
	var calls atomic.Int32
	requests := make(chan []byte, 2)
	anchor, state := anchorFixture(testBlockTime), stateFixture()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		var batch []struct {
			ID     int               `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.Unmarshal(body, &batch) != nil || len(batch) != 2 {
			t.Error("invalid RPC request")
		}
		if calls.Add(1) == 1 {
			if batch[0].Method != "eth_chainId" || batch[1].Method != "eth_getBlockByNumber" || string(batch[1].Params[0]) != `"finalized"` || string(batch[1].Params[1]) != "false" {
				t.Error("anchor is not mainnet + finalized")
			}
			fmt.Fprint(w, anchor)
			return
		}
		for _, item := range batch {
			var pinned struct {
				Hash      string `json:"blockHash"`
				Canonical bool   `json:"requireCanonical"`
			}
			if len(item.Params) != 2 || json.Unmarshal(item.Params[1], &pinned) != nil || pinned.Hash != testHash || !pinned.Canonical {
				t.Error("state request is not pinned to canonical anchor hash")
			}
		}
		if batch[0].Method != "eth_call" || batch[1].Method != "eth_getBalance" {
			t.Error("wrong state methods")
		}
		fmt.Fprint(w, state)
	}))
	defer server.Close()
	c := configuredClient(server.URL)
	c.Now = func() time.Time {
		if calls.Load() != 2 {
			t.Error("collected_at must be read after both requests complete")
		}
		return testBlockTime.Add(time.Minute + 123*time.Millisecond).In(time.FixedZone("CST", 8*3600))
	}
	s, err := c.Read(context.Background(), testReads())
	if err != nil {
		t.Fatal(err)
	}
	wantHash := marketyield.HashPayloads(
		marketyield.Payload{Name: "anchor-request", Body: <-requests}, marketyield.Payload{Name: "anchor-response", Body: []byte(anchor)},
		marketyield.Payload{Name: "state-request", Body: <-requests}, marketyield.Payload{Name: "state-response", Body: []byte(state)})
	if s.BlockHeight != 93742828 || s.BlockHash != testHash || !s.BlockTime.Equal(testBlockTime) || s.Results[0] != word(18) || s.Results[1] != "0x1" ||
		s.PayloadHash != wantHash || s.CollectedAt.Location() != time.UTC || s.CollectedAt.Nanosecond() != 123000000 {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestReadRejectsBrokenAnchorsAndBatches(t *testing.T) {
	valid := anchorFixture(testBlockTime)
	fixtures := map[string]string{
		"null": "null", "empty": "[]", "object instead of batch": `{}`, "trailing JSON": valid + `{}`,
		"wrong version": strings.Replace(valid, "2.0", "1.0", 1),
		"duplicate ID":  strings.Replace(valid, `"id":1`, `"id":2`, 1), "unknown ID": strings.Replace(valid, `"id":1`, `"id":3`, 1),
		"null ID": strings.Replace(valid, `"id":1`, `"id":null`, 1), "string ID": strings.Replace(valid, `"id":1`, `"id":"1"`, 1),
		"missing ID": strings.Replace(valid, `"id":1,`, "", 1), "fractional ID": strings.Replace(valid, `"id":1`, `"id":1.5`, 1),
		"wrong chain": strings.Replace(valid, "0xa86a", "0x1", 1), "invalid chain quantity": strings.Replace(valid, "0xa86a", "0x0a86a", 1),
		"null finalized":     `[{"jsonrpc":"2.0","id":1,"result":"0xa86a"},{"jsonrpc":"2.0","id":2,"result":null}]`,
		"missing field":      strings.Replace(valid, `"timestamp":`, `"notTimestamp":`, 1),
		"invalid hash":       strings.Replace(valid, testHash, "0x1", 1),
		"height overflow":    strings.Replace(valid, fmt.Sprintf("0x%x", 93742828), "0x10000000000000000", 1),
		"timestamp overflow": strings.Replace(valid, fmt.Sprintf("0x%x", testBlockTime.Unix()), "0x8000000000000000", 1),
		"stale":              anchorFixture(testBlockTime.Add(-11 * time.Minute)), "future": anchorFixture(testBlockTime.Add(3 * time.Minute)),
		"partial error": `[{"jsonrpc":"2.0","id":1,"result":"0xa86a"},{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"no finalized"}}]`,
	}
	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, _ := io.ReadAll(r.Body)
				if bytes.Contains(request, []byte("eth_chainId")) {
					fmt.Fprint(w, body)
				} else {
					fmt.Fprint(w, stateFixture())
				}
			}))
			defer server.Close()
			if snapshot, err := configuredClient(server.URL).Read(context.Background(), testReads()); err == nil || len(snapshot.Results) != 0 {
				t.Fatalf("invalid anchor returned snapshot=%+v err=%v", snapshot, err)
			}
		})
	}
}

func TestReadRejectsIncompleteOrUnsupportedState(t *testing.T) {
	fixtures := []string{
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":1,"result":"0x1"}]`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":3,"result":"0x1"}]`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":null}]`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":123}]`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"blockHash unsupported"}}]`,
	}
	for i, body := range fixtures {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					fmt.Fprint(w, anchorFixture(testBlockTime))
				} else {
					fmt.Fprint(w, body)
				}
			}))
			defer server.Close()
			if snapshot, err := configuredClient(server.URL).Read(context.Background(), testReads()); err == nil || len(snapshot.Results) != 0 {
				t.Fatalf("incomplete state accepted: %+v %v", snapshot, err)
			}
			if calls.Load() != 2 {
				t.Fatal("unexpected fallback/retry instead of rejecting unsupported state")
			}
		})
	}
}

func TestFixedABIDecodersAndQuantityAreDistinct(t *testing.T) {
	for _, raw := range []string{"", "0x", "0x1", word(1) + "00", "0x" + strings.Repeat("g", 64)} {
		if _, err := Uint256(raw); err == nil {
			t.Fatalf("invalid ABI word accepted %q", raw)
		}
	}
	for _, raw := range []string{"", "1", "0x", "0x00", "0x01", "0X1", "0x-1", "0x+1", "0xg", "0x" + strings.Repeat("f", 65)} {
		if _, err := Quantity(raw); err == nil {
			t.Fatalf("invalid quantity accepted %q", raw)
		}
	}
	if value, err := Quantity("0x0"); err != nil || value.Sign() != 0 {
		t.Fatal("zero quantity must be valid")
	}
	if value, err := Uint256("0x" + strings.Repeat("f", 64)); err != nil || value.BitLen() != 256 {
		t.Fatal("full uint256 must remain exact")
	}
	for _, n := range []int64{0, 1} {
		if value, err := Bool(word(n)); err != nil || value != (n == 1) {
			t.Fatal("valid bool rejected")
		}
	}
	if _, err := Bool(word(2)); err == nil {
		t.Fatal("bool 2 accepted")
	}
	addressWord := "0x" + strings.Repeat("0", 24) + testAddress[2:]
	if value, err := Address(addressWord); err != nil || value != testAddress {
		t.Fatal("valid address rejected")
	}
	if _, err := Address("0x1" + addressWord[3:]); err == nil {
		t.Fatal("nonzero address high bytes accepted")
	}
	market := word(1) + word(0)[2:] + word(0)[2:]
	if value, err := Markets(market); err != nil || !value {
		t.Fatal("valid market tuple rejected")
	}
	for _, raw := range []string{word(1), market + word(0)[2:], word(2) + word(0)[2:] + word(0)[2:], word(1) + word(0)[2:] + word(2)[2:]} {
		if _, err := Markets(raw); err == nil {
			t.Fatal("invalid market tuple accepted")
		}
	}
	if _, err := Uint64("0x" + strings.Repeat("f", 64)); err == nil {
		t.Fatal("uint256 overflow into seconds accepted")
	}
}

func TestDecimalBoundariesAndPositiveRatioTruncation(t *testing.T) {
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if _, err := Scaled(limit, -18); err == nil {
		t.Fatal("Decimal overflow accepted")
	}
	if value, err := Scaled(new(big.Int).Sub(limit, big.NewInt(1)), -18); err != nil || value.String() != "99999999999999999999.999999999999999999" {
		t.Fatalf("valid boundary lost precision: %s %v", value, err)
	}
	if _, err := PositiveScaled(big.NewInt(1), -28); err == nil {
		t.Fatal("ratio truncated to zero accepted")
	}
	if _, err := Scaled(big.NewInt(-1), -18); err == nil {
		t.Fatal("negative amount accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type errorCollector struct{ client *Client }

func (c errorCollector) Collect(ctx context.Context) (marketyield.Batch, error) {
	_, err := c.client.Read(ctx, testReads())
	return marketyield.Batch{}, err
}

type noWriteSink struct{ t *testing.T }

func (s noWriteSink) WriteYieldBatch(context.Context, marketyield.Batch) error {
	s.t.Error("failed RPC wrote a batch")
	return nil
}

func TestRPCErrorBoundaryAndRunnerLogsNeverRevealSecrets(t *testing.T) {
	for _, mode := range []string{"http", "rpc", "json", "transport"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch mode {
				case "http":
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprint(w, "provider secret FAKE_BODY_KEY")
				case "rpc":
					fmt.Fprint(w, `[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"FAKE_BODY_KEY","data":"FAKE_DATA_KEY"}},{"jsonrpc":"2.0","id":2,"result":{}}]`)
				default:
					fmt.Fprint(w, "invalid JSON FAKE_BODY_KEY")
				}
			}))
			defer server.Close()
			endpoint, _ := url.Parse(server.URL + "/FAKE_PATH_KEY?api_key=FAKE_QUERY_KEY")
			endpoint.User = url.UserPassword("FAKE_USER", "FAKE_PASSWORD")
			client := configuredClient(endpoint.String())
			if mode == "transport" {
				client.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New(endpoint.String() + " FAKE_TRANSPORT_KEY")
				})}
			}
			_, err := client.Read(context.Background(), testReads())
			if err == nil || strings.Contains(err.Error(), "FAKE_") || strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), "provider secret") {
				t.Fatalf("unsafe error: %v", err)
			}
			var logs bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			runner := marketyield.Runner{Source: "safe-source", Collector: errorCollector{client}, Sink: noWriteSink{t}, Interval: time.Hour, RetryInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(&logs, nil))}
			if err = runner.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if logs.Len() == 0 || strings.Contains(logs.String(), "FAKE_") || strings.Contains(logs.String(), server.URL) {
				t.Fatalf("unsafe or missing log: %s", logs.String())
			}
		})
	}
}

func TestSharedClientGateSerializesConcurrentRequestStarts(t *testing.T) {
	var requests atomic.Int32
	c := configuredClient("https://rpc.test")
	c.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		response := stateFixture()
		if bytes.Contains(body, []byte("eth_chainId")) {
			response = anchorFixture(testBlockTime)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(response))}, nil
	})}
	const interval = 30 * time.Millisecond
	c.Retry.Cooldown = exchange.NewRequestGate(interval)
	var wg sync.WaitGroup
	started := time.Now()
	for range 2 {
		wg.Go(func() {
			if _, err := c.Read(context.Background(), testReads()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	// Four requests reserve slots at least 0/1/2/3 intervals from startup.
	// Per-handler arrival gaps are not reliable: a descheduled earlier request
	// may reach the transport close to a later one even with a correct gate.
	if requests.Load() != 4 || time.Since(started) < 3*interval-time.Millisecond {
		t.Fatalf("shared gate requests=%d elapsed=%s", requests.Load(), time.Since(started))
	}
}
