package ankr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
)

const (
	testToken     = "0xc3344870d52688874b06d844e0c36cc39fc727f6"
	testPool      = "0x7baa1e3bfe49db8361680785182b80bb420a836d"
	testNative    = "eip155:43114:native:AVAX"
	testBlockHash = "0x1062e758cb290e7ef7abb2ed5a448cc81afdb632c53d57c8fa403c664fcb65fa"
)

var testBlockTime = time.Date(2026, 8, 26, 16, 53, 55, 0, time.UTC)

// The fixture checks the actual requests and retains their original bytes for
// hash assertions. Response order is deliberately the reverse of request order.
type rpcFixture struct {
	mu           sync.Mutex
	values       map[string]any
	fault        string
	paddingStage string
	blockHash    string
	blockTime    time.Time
	height       uint64
	payloads     []marketyield.Payload
}

func newTestCollector(t *testing.T, fixture *rpcFixture) *Collector {
	t.Helper()
	fixture.blockHash, fixture.blockTime, fixture.height = testBlockHash, testBlockTime, 93742640
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		body, err := io.ReadAll(r.Body)
		var requests []struct {
			JSONRPC string
			ID      int
			Method  string
			Params  []json.RawMessage
		}
		if r.Method != http.MethodPost || err != nil || json.Unmarshal(body, &requests) != nil || len(requests) == 0 {
			t.Error("expected a JSON-RPC POST batch")
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		stage := "state"
		for _, request := range requests {
			if request.Method == "eth_chainId" {
				stage = "anchor"
			}
		}
		wantCount := 3
		if stage == "anchor" {
			wantCount = 2
		}
		if len(requests) != wantCount {
			t.Errorf("%s request count = %d, want %d", stage, len(requests), wantCount)
		}
		seen := make(map[string]bool)
		responses := make([]map[string]any, 0, len(requests))
		for _, request := range requests {
			if request.JSONRPC != "2.0" {
				t.Errorf("request JSON-RPC version = %q", request.JSONRPC)
			}
			var key string
			var value any
			switch request.Method {
			case "eth_chainId":
				key, value = "chain", "0xa86a"
				if len(request.Params) != 0 {
					t.Error("eth_chainId must have no parameters")
				}
			case "eth_getBlockByNumber":
				key = "block"
				if len(request.Params) != 2 || string(request.Params[0]) != "\"finalized\"" || string(request.Params[1]) != "false" {
					t.Error("anchor must request the finalized block without transactions")
				}
				value = map[string]any{
					"number": fmt.Sprintf("0x%x", fixture.height), "hash": fixture.blockHash,
					"timestamp": fmt.Sprintf("0x%x", fixture.blockTime.Unix()), "unusedField": "allowed",
				}
			case "eth_call", "eth_getBalance":
				var anchor struct {
					BlockHash        string
					RequireCanonical bool
				}
				if len(request.Params) != 2 || json.Unmarshal(request.Params[1], &anchor) != nil || anchor.BlockHash != fixture.blockHash || !anchor.RequireCanonical {
					t.Error("every state read must use the same canonical block hash")
					http.Error(w, "invalid anchor", http.StatusBadRequest)
					return
				}
				if request.Method == "eth_getBalance" {
					var address string
					if json.Unmarshal(request.Params[0], &address) != nil || address != testPool {
						t.Error("cash must be read from the Ankr pool, not the token or another address")
					}
					key, value = "cash", quantity("123456789012345678901")
				} else {
					var call struct{ To, Data string }
					if json.Unmarshal(request.Params[0], &call) != nil || call.To != testToken {
						t.Error("token getters must use the ankrAVAX proxy")
					}
					switch call.Data {
					case "0x313ce567":
						key, value = "decimals", word("18")
					case "0x71ca337d":
						key, value = "ratio", word("707043246512935099")
					default:
						t.Errorf("unexpected contract selector %q", call.Data)
					}
				}
			default:
				t.Errorf("unexpected RPC method %q", request.Method)
			}
			if seen[key] {
				t.Errorf("duplicate read %q", key)
			}
			seen[key] = true
			if override, ok := fixture.values[key]; ok {
				value = override
			}
			responses = append(responses, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": value})
		}
		if stage == "anchor" && fixture.fault == "partial-anchor" {
			responses = responses[:1]
		}
		if stage == "state" {
			switch fixture.fault {
			case "partial":
				responses = responses[:len(responses)-1]
			case "duplicate":
				responses[1]["id"] = responses[0]["id"]
			case "unknown-id":
				responses[0]["id"] = 99
			case "rpc":
				responses[0]["error"] = map[string]any{"code": -32000, "message": "fixture failure"}
			case "http":
				http.Error(w, "fixture failure", http.StatusServiceUnavailable)
				return
			}
		}
		for left, right := 0, len(responses)-1; left < right; left, right = left+1, right-1 {
			responses[left], responses[right] = responses[right], responses[left]
		}
		response, err := json.Marshal(responses)
		if err != nil {
			t.Errorf("marshal fixture: %v", err)
			http.Error(w, "fixture error", http.StatusInternalServerError)
			return
		}
		if stage == fixture.paddingStage {
			response = append(response, '\n', ' ')
		}
		if stage == "state" {
			switch fixture.fault {
			case "trailing-json":
				response = append(response, []byte("{}")...)
			case "invalid-json":
				response = []byte("{")
			}
		}
		fixture.payloads = append(fixture.payloads,
			marketyield.Payload{Name: stage + "-request", Body: body},
			marketyield.Payload{Name: stage + "-response", Body: response})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
	t.Cleanup(server.Close)
	client := avalanche.NewClient(server.URL)
	client.HTTPClient = server.Client()
	client.Retry = exchange.HTTPRetryConfig{MaxAttempts: 1, Cooldown: exchange.NewRequestGate(0)}
	client.Now = func() time.Time {
		return testBlockTime.Add(2*time.Minute + 123*time.Millisecond).In(time.FixedZone("fixture", 8*60*60))
	}
	return NewCollector(client)
}

func word(value string) string {
	number, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid decimal fixture")
	}
	return fmt.Sprintf("0x%064x", number)
}

func quantity(value string) string {
	number, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid decimal fixture")
	}
	return "0x" + number.Text(16)
}

func TestCollectorRecordsExactRatioCashAndUnknownRules(t *testing.T) {
	fixture := &rpcFixture{}
	collector := newTestCollector(t, fixture)
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Items) != 1 {
		t.Fatalf("items = %d, want one complete route", len(batch.Items))
	}
	route, observation := batch.Items[0].Route, batch.Items[0].Observation
	if route.Provider != "Ankr" || route.ProviderType != "protocol" || route.ProductCode != "avalanche-ankravax-staking" || route.YieldType != "liquid_staking" || route.IncomeSource != "issuance" {
		t.Fatalf("unexpected route: %+v", route)
	}
	if route.DepositAssetKey != testNative || route.RedeemAssetKey != testNative || route.PositionAssetKey != "eip155:43114:erc20:"+testToken {
		t.Fatalf("wrong asset identity: %+v", route)
	}
	if route.Network == nil || *route.Network != "avalanche-c-mainnet" || route.ContractAddress == nil || *route.ContractAddress != testPool ||
		route.PriceExposureAsset == nil || *route.PriceExposureAsset != "AVAX" || !route.CollectionEnabled {
		t.Fatalf("wrong network, pool or exposure identity: %+v", route)
	}
	if route.SourceURL != "https://www.ankr.com/docs/staking-for-developers/smart-contract-api/avax-api/" {
		t.Fatalf("source URL = %q", route.SourceURL)
	}
	if observation.ExposureRatio == nil || observation.ExposureRatio.String() != "1.414340643138729653" {
		t.Fatalf("inverse ratio = %v", observation.ExposureRatio)
	}
	if observation.PoolCash == nil || observation.PoolCash.String() != "123.456789012345678901" {
		t.Fatalf("pool cash = %v", observation.PoolCash)
	}
	for name, value := range map[string]*decimal.Decimal{
		"rate": observation.Rate, "entry_fee_rate": observation.EntryFeeRate, "exit_fee_rate": observation.ExitFeeRate,
		"performance_fee_rate": observation.PerformanceFeeRate, "fixed_penalty_rate": observation.FixedPenaltyRate,
		"entry_fee_amount": observation.EntryFeeAmount, "exit_fee_amount": observation.ExitFeeAmount,
		"fixed_principal_loss_rate": observation.FixedPrincipalLossRate, "capacity": observation.Capacity,
		"remaining_capacity": observation.RemainingCapacity, "tvl": observation.TVL,
	} {
		if value != nil {
			t.Errorf("%s = %s, want NULL", name, value)
		}
	}
	if observation.UnbondingSeconds != nil || observation.RedemptionWindowSeconds != nil || observation.FixedFeeAssetKey != nil || observation.LockSeconds != 0 {
		t.Errorf("invented fee or exit rule: %+v", observation)
	}
	if observation.Availability != "unknown" || observation.RuleEligibility != "unknown" || observation.RulePrincipalLossMode != "unknown" ||
		observation.EligibilityReason == nil || *observation.EligibilityReason != "redemption_rules_not_fully_reviewed" {
		t.Errorf("invented availability or eligibility: %+v", observation)
	}
	if observation.TierNo != 1 || !observation.TierMinAmount.IsZero() || observation.TierMaxAmount != nil || observation.TierMode != "none" ||
		observation.RateKind != "unknown" || observation.RateOrigin != "reported" || observation.RateMode != "variable" {
		t.Errorf("wrong tier/rate semantics: %+v", observation)
	}
	if len(observation.RewardAssetKeys) != 1 || observation.RewardAssetKeys[0] != testNative || len(observation.RewardComponentRates) != 1 || observation.RewardComponentRates[0] != nil {
		t.Errorf("wrong reward components: %+v", observation)
	}
	wantCollected := testBlockTime.Add(2*time.Minute + 123*time.Millisecond)
	if !observation.ObservationTime.Equal(testBlockTime) || !observation.CollectedAt.Equal(wantCollected) || !batch.CollectedAt.Equal(wantCollected) ||
		observation.ObservationTime.Location() != time.UTC || observation.CollectedAt.Location() != time.UTC {
		t.Errorf("wrong source or collection time: %+v", observation)
	}
	if observation.BlockHeight == nil || *observation.BlockHeight != 93742640 || observation.BlockHash == nil || *observation.BlockHash != testBlockHash ||
		observation.Finality == nil || *observation.Finality != "finalized" {
		t.Errorf("wrong chain anchor: %+v", observation)
	}
	if err := batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatalf("live batch validation: %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.payloads) != 4 || observation.SourcePayloadHash == nil || *observation.SourcePayloadHash != marketyield.HashPayloads(fixture.payloads...) {
		t.Error("hash must cover the exact two request/response batches")
	}
}

func TestCollectorPreservesCashAndRatioBoundaries(t *testing.T) {
	for _, test := range []struct{ name, cash, ratio, wantCash, wantRatio string }{
		{"zero cash", "0", "707043246512935099", "0", "1.414340643138729653"},
		{"one wei cash", "1", "707043246512935099", "0.000000000000000001", "1.414340643138729653"},
		{"maximum decimal cash", strings.Repeat("9", 38), "707043246512935099", "99999999999999999999.999999999999999999", "1.414340643138729653"},
		{"smallest representable ratio", "0", "1" + strings.Repeat("0", 36), "0", "0.000000000000000001"},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := newTestCollector(t, &rpcFixture{values: map[string]any{"cash": quantity(test.cash), "ratio": word(test.ratio)}})
			batch, err := collector.Collect(context.Background())
			if err != nil || len(batch.Items) != 1 {
				t.Fatalf("collect: batch=%+v err=%v", batch, err)
			}
			observation := batch.Items[0].Observation
			if observation.PoolCash == nil || observation.PoolCash.String() != test.wantCash || observation.ExposureRatio == nil || observation.ExposureRatio.String() != test.wantRatio {
				t.Fatalf("cash=%v ratio=%v", observation.PoolCash, observation.ExposureRatio)
			}
			if observation.Availability != "unknown" || observation.Capacity != nil || observation.RemainingCapacity != nil || observation.TVL != nil {
				t.Fatal("cash must not become availability, capacity or TVL")
			}
		})
	}
}

func TestCollectorRejectsInvalidOrIncompleteInputWithoutBatch(t *testing.T) {
	for name, fixture := range map[string]*rpcFixture{
		"wrong decimals":            {values: map[string]any{"decimals": word("8")}},
		"decimals overflow":         {values: map[string]any{"decimals": word("18446744073709551616")}},
		"zero denominator":          {values: map[string]any{"ratio": word("0")}},
		"inverse truncates to zero": {values: map[string]any{"ratio": word("1" + strings.Repeat("0", 35) + "1")}},
		"maximum uint256 ratio":     {values: map[string]any{"ratio": "0x" + strings.Repeat("f", 64)}},
		"cash exceeds decimal":      {values: map[string]any{"cash": quantity("1" + strings.Repeat("0", 38))}},
		"empty ABI word":            {values: map[string]any{"ratio": "0x"}},
		"quantity instead of ABI":   {values: map[string]any{"ratio": "0x1"}},
		"oversized ABI word":        {values: map[string]any{"ratio": "0x" + strings.Repeat("0", 66)}},
		"invalid ABI hex":           {values: map[string]any{"ratio": "0x" + strings.Repeat("g", 64)}},
		"padded cash quantity":      {values: map[string]any{"cash": "0x00"}},
		"empty cash quantity":       {values: map[string]any{"cash": "0x"}},
		"invalid cash quantity":     {values: map[string]any{"cash": "0xgg"}},
		"overflow cash quantity":    {values: map[string]any{"cash": "0x1" + strings.Repeat("0", 64)}},
		"nonstring result":          {values: map[string]any{"ratio": 123}},
		"null result":               {values: map[string]any{"ratio": nil}},
		"wrong chain":               {values: map[string]any{"chain": "0x1"}},
		"incomplete anchor":         {fault: "partial-anchor"},
		"incomplete state":          {fault: "partial"},
		"duplicate response id":     {fault: "duplicate"},
		"unknown response id":       {fault: "unknown-id"},
		"RPC failure among results": {fault: "rpc"},
		"HTTP failure":              {fault: "http"},
		"trailing JSON after batch": {fault: "trailing-json"},
		"malformed JSON response":   {fault: "invalid-json"},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err := newTestCollector(t, fixture).Collect(context.Background())
			if err == nil || !reflect.DeepEqual(batch, marketyield.Batch{}) {
				t.Fatalf("invalid input produced batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestCollectorDoesNotFilterLaterExchangeRatioDecline(t *testing.T) {
	fixture := &rpcFixture{}
	collector := newTestCollector(t, fixture)
	first, err := collector.Collect(context.Background())
	if err != nil || len(first.Items) != 1 {
		t.Fatalf("first collect: batch=%+v err=%v", first, err)
	}
	fixture.mu.Lock()
	fixture.values = map[string]any{"ratio": word("1000000000000000000")}
	fixture.blockTime = testBlockTime.Add(time.Minute)
	fixture.height++
	fixture.blockHash = "0x" + strings.Repeat("2", 64)
	fixture.mu.Unlock()
	second, err := collector.Collect(context.Background())
	if err != nil || len(second.Items) != 1 {
		t.Fatalf("later declining ratio must remain valid: batch=%+v err=%v", second, err)
	}
	before, after := first.Items[0].Observation, second.Items[0].Observation
	if after.ExposureRatio == nil || after.ExposureRatio.String() != "1" || !after.ExposureRatio.LessThan(*before.ExposureRatio) || !after.ObservationTime.After(before.ObservationTime) {
		t.Fatalf("decline was filtered or replaced: before=%+v after=%+v", before, after)
	}
}

func TestCollectorHashesRawResponseBytes(t *testing.T) {
	hashes := make(map[string]bool)
	for _, stage := range []string{"", "anchor", "state"} {
		fixture := &rpcFixture{paddingStage: stage}
		batch, err := newTestCollector(t, fixture).Collect(context.Background())
		if err != nil || len(batch.Items) != 1 {
			t.Fatalf("stage %q: batch=%+v err=%v", stage, batch, err)
		}
		observation := batch.Items[0].Observation
		if observation.SourcePayloadHash == nil {
			t.Fatal("missing payload hash")
		}
		fixture.mu.Lock()
		expected := marketyield.HashPayloads(fixture.payloads...)
		fixture.mu.Unlock()
		got := *observation.SourcePayloadHash
		if got != expected || len(got) != 64 || hashes[got] {
			t.Fatalf("stage %q: hash=%q expected=%q; response whitespace must affect the hash", stage, got, expected)
		}
		hashes[got] = true
	}
}
