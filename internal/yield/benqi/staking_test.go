package benqi

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
)

const blockHash = "0x4c621d56b2a6749290b40b6e31b78cd7ecafe02bbcd723ff6c82ea6fa2063d48"

var blockTime = time.Date(2026, 8, 26, 16, 57, 19, 0, time.UTC)

func abi(text string) string {
	value, ok := new(big.Int).SetString(text, 10)
	if !ok {
		panic("invalid test integer")
	}
	return fmt.Sprintf("0x%064x", value)
}

func callKey(address, data string) string { return address + ":" + data }

type rpcFixture struct {
	mu     sync.Mutex
	values map[string]string
	seen   []string
}

func fixtureClient(t *testing.T, values map[string]string) (*avalanche.Client, *rpcFixture) {
	t.Helper()
	f := &rpcFixture{values: values}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []struct {
			ID     int               `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&requests) != nil || len(requests) == 0 {
			t.Error("invalid test RPC request")
			return
		}
		if requests[0].Method == "eth_chainId" {
			fmt.Fprintf(w, `[{"jsonrpc":"2.0","id":2,"result":{"number":"0x59666ec","hash":%q,"timestamp":"0x%x"}},{"jsonrpc":"2.0","id":1,"result":"0xa86a"}]`, blockHash, blockTime.Unix())
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		var responses []map[string]any
		for _, request := range requests {
			var pinned struct {
				Hash      string `json:"blockHash"`
				Canonical bool   `json:"requireCanonical"`
			}
			if len(request.Params) != 2 || json.Unmarshal(request.Params[1], &pinned) != nil || pinned.Hash != blockHash || !pinned.Canonical {
				t.Error("state call not anchored")
			}
			var key string
			if request.Method == "eth_getBalance" {
				var address string
				_ = json.Unmarshal(request.Params[0], &address)
				key = callKey(address, "balance")
			} else if request.Method == "eth_call" {
				var call struct{ To, Data string }
				_ = json.Unmarshal(request.Params[0], &call)
				key = callKey(call.To, call.Data)
			} else {
				t.Error("non-read RPC method", request.Method)
			}
			f.seen = append(f.seen, key)
			value, exists := f.values[key]
			if !exists {
				t.Error("unexpected getter", key)
			}
			responses = append(responses, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": value})
		}
		_ = json.NewEncoder(w).Encode(responses)
	}))
	t.Cleanup(server.Close)
	client := avalanche.NewClient(server.URL)
	client.Now = func() time.Time { return blockTime.Add(time.Minute + 123*time.Millisecond) }
	client.Retry.MaxAttempts, client.Retry.Cooldown = 1, exchange.NewRequestGate(0)
	return client, f
}

func stakingValues() map[string]string {
	return map[string]string{
		callKey(SAVAX, "0x313ce567"): abi("18"),
		callKey(SAVAX, "0x4a36d6c1"+fmt.Sprintf("%064x", big.NewInt(1_000_000_000_000_000_000))): abi("1281376817186495921"),
		callKey(SAVAX, "0x629e8056"): abi("100000000000000000000"),
		callKey(SAVAX, "0x5cd47487"): "0x" + strings.Repeat("f", 64),
		callKey(SAVAX, "0x6e34637c"): abi("100000000000000000"),
		callKey(SAVAX, "0x04646a49"): abi("1296000"),
		callKey(SAVAX, "0x40a233a6"): abi("172800"),
		callKey(SAVAX, "0x5c975abb"): abi("0"),
		callKey(SAVAX, "0xe1a283d6"): abi("0"),
		callKey(SAVAX, "balance"):    "0x1",
	}
}

func TestStakingSnapshotUnlimitedCapFeesAndSeparateExitWindow(t *testing.T) {
	client, f := fixtureClient(t, stakingValues())
	batch, err := NewStakingCollector(client).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertChainBatch(t, batch, "avalanche-savax-staking", SAVAX)
	o := batch.Items[0].Observation
	if o.Rate != nil || o.RateKind != "unknown" || o.ExposureRatio.String() != "1.281376817186495921" || o.PoolCash.String() != "0.000000000000000001" || o.TVL.String() != "100" ||
		o.Capacity != nil || o.RemainingCapacity != nil || o.PerformanceFeeRate.String() != "0.1" || !o.EntryFeeRate.IsZero() || !o.ExitFeeRate.IsZero() ||
		*o.UnbondingSeconds != 1296000 || *o.RedemptionWindowSeconds != 172800 || o.RuleEligibility != "candidate" || o.EligibilityReason != nil || o.Availability != "available" || o.RewardComponentRates[0] != nil {
		t.Fatalf("staking observation=%+v", o)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seen) != 10 {
		t.Fatalf("staking must read exactly 10 fields, got %d", len(f.seen))
	}
}

func assertChainBatch(t *testing.T, batch marketyield.Batch, product, token string) {
	t.Helper()
	if err := batch.NormalizeAndValidateForLiveCollection(); err != nil || len(batch.Items) != 1 {
		t.Fatalf("invalid single-row live batch: %v", err)
	}
	r, o := batch.Items[0].Route, batch.Items[0].Observation
	if r.ProductCode != product || r.Provider != "BENQI" || r.ProviderType != "protocol" || r.DepositAssetKey != avalanche.NativeAsset || r.RedeemAssetKey != avalanche.NativeAsset ||
		r.PositionAssetKey != "eip155:43114:erc20:"+token || *r.ContractAddress != token || *r.Network != avalanche.Network || *r.PriceExposureAsset != "AVAX" || strings.Contains(r.SourceURL, "127.0.0.1") {
		t.Fatalf("route=%+v", r)
	}
	if !o.ObservationTime.Equal(blockTime) || !o.CollectedAt.Equal(blockTime.Add(time.Minute+123*time.Millisecond)) || *o.BlockHash != blockHash || *o.BlockHeight != 93742828 || *o.Finality != "finalized" ||
		o.SourcePayloadHash == nil || len(*o.SourcePayloadHash) != 64 || o.TierNo != 1 || o.TierMode != "none" || o.RulePrincipalLossMode != "none" || len(o.RewardAssetKeys) != 1 || o.RewardAssetKeys[0] != avalanche.NativeAsset ||
		o.FixedPenaltyRate != nil || o.EntryFeeAmount != nil || o.ExitFeeAmount != nil || o.FixedFeeAssetKey != nil || o.FixedPrincipalLossRate != nil || o.LockSeconds != 0 {
		t.Fatalf("common observation=%+v", o)
	}
}

func TestStakingPreservesPausedExhaustedAndZeroWindowStates(t *testing.T) {
	for _, test := range []struct {
		name, cap, paused, mint, window, availability, eligibility, expectedCapacity, expectedRemaining string
	}{
		{"finite capacity", "150000000000000000000", "0", "0", "172800", "available", "candidate", "150", "50"},
		{"rewards above cap", "90000000000000000000", "0", "0", "172800", "unavailable", "candidate", "90", "0"},
		{"equal cap", "100000000000000000000", "0", "0", "172800", "unavailable", "candidate", "100", "0"},
		{"paused exhausted", "90000000000000000000", "1", "0", "172800", "paused", "candidate", "90", "0"},
		{"mint paused", "150000000000000000000", "0", "1", "172800", "paused", "candidate", "150", "50"},
		{"empty redemption window", "150000000000000000000", "0", "0", "0", "available", "unknown", "150", "50"},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := stakingValues()
			values[callKey(SAVAX, "0x5cd47487")], values[callKey(SAVAX, "0x5c975abb")] = abi(test.cap), abi(test.paused)
			values[callKey(SAVAX, "0xe1a283d6")], values[callKey(SAVAX, "0x40a233a6")] = abi(test.mint), abi(test.window)
			client, _ := fixtureClient(t, values)
			batch, err := NewStakingCollector(client).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			o := batch.Items[0].Observation
			if o.Availability != test.availability || o.RuleEligibility != test.eligibility || o.Capacity == nil || o.RemainingCapacity == nil || o.Capacity.String() != test.expectedCapacity || o.RemainingCapacity.String() != test.expectedRemaining {
				t.Fatalf("state=%+v", o)
			}
			if test.window == "0" && (*o.RedemptionWindowSeconds != 0 || o.EligibilityReason == nil || *o.EligibilityReason != "redemption_window_empty") {
				t.Fatal("empty claim window lost or incorrectly eligible")
			}
		})
	}
	values := stakingValues()
	values[callKey(SAVAX, "0x40a233a6")] = abi("0")
	client, fixture := fixtureClient(t, values)
	collector := NewStakingCollector(client)
	first, err := collector.Collect(context.Background())
	if err != nil || first.Items[0].Observation.RuleEligibility != "unknown" {
		t.Fatalf("zero window: %v", err)
	}
	fixture.mu.Lock()
	fixture.values[callKey(SAVAX, "0x40a233a6")] = abi("172800")
	fixture.values[callKey(SAVAX, pooledAVAXCall)] = abi("1200000000000000000")
	fixture.mu.Unlock()
	second, err := collector.Collect(context.Background())
	if err != nil || second.Items[0].Observation.RuleEligibility != "candidate" || second.Items[0].Observation.EligibilityReason != nil || second.Items[0].Observation.ExposureRatio.String() != "1.2" {
		t.Fatalf("recovered window or decreasing ratio rejected: %v", err)
	}
}

func TestStakingRejectsInvalidRequiredFieldsWithoutBatch(t *testing.T) {
	for _, test := range []struct{ selector, value string }{
		{"0x313ce567", abi("8")}, {pooledAVAXCall, abi("0")}, {pooledAVAXCall, "0x"},
		{"0x5cd47487", "0x" + strings.Repeat("f", 63) + "e"},
		{"0x6e34637c", abi("1000000000000000001")}, {"0x5c975abb", abi("2")}, {"0xe1a283d6", abi("2")},
		{"0x04646a49", abi("18446744073709551616")}, {"0x40a233a6", abi("18446744073709551616")},
		{"0x629e8056", abi("100000000000000000000000000000000000000")},
		{"balance", "0x00"}, {"balance", "0x" + strings.Repeat("f", 64)},
	} {
		t.Run(test.selector+"/"+test.value, func(t *testing.T) {
			values := stakingValues()
			values[callKey(SAVAX, test.selector)] = test.value
			client, _ := fixtureClient(t, values)
			if batch, err := NewStakingCollector(client).Collect(context.Background()); err == nil || len(batch.Items) != 0 {
				t.Fatalf("invalid state returned batch=%+v err=%v", batch, err)
			}
		})
	}
}
