package justlendkeeper

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

// These responses are SYNTHETIC parser cases, never captured positive simulations.
// Removing per-response certification, accepting resource operations as cash, or
// summing duplicate liquidation logs must change the outcomes below.
func syntheticRewardResponse(t *testing.T) map[string]any {
	t.Helper()
	caller := "41020c064bab81eb2ef520f1cc2a26f5f1867e29b3"
	renter, receiver := "41"+strings.Repeat("11", 20), "41"+strings.Repeat("22", 20)
	word := func(n int64) string { return fmt.Sprintf("%064x", n) }
	return map[string]any{
		"result":          map[string]any{"result": true},
		"transaction":     map[string]any{"ret": []any{map[string]any{}}},
		"constant_result": []string{word(20000000)}, "energy_used": 90000,
		"logs": []any{map[string]any{"address": ContractHex[2:], "topics": []string{
			"b0dbe18c6ffdf0da655dd690e77211d379205c497be44c64447c3f5f021b5167",
			strings.Repeat("0", 24) + caller[2:], strings.Repeat("0", 24) + renter[2:], strings.Repeat("0", 24) + receiver[2:]},
			"data": word(1000000000) + word(1) + word(10) + word(20000000) + word(5)}},
		"internal_transactions": []any{map[string]any{"caller_address": ContractHex, "transferTo_address": caller,
			"callValueInfo": []any{map[string]any{"callValue": 20000000}}, "note": "63616c6c"}},
	}
}

func rewardParseContext() (*Collector, Candidate, string, Evidence, time.Time) {
	now := time.Date(2026, 10, 24, 1, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	state := NewState("synthetic", cfg)
	state.IdentityStatus = "verified_manifest"
	state.IdentityAt = now.Add(-time.Minute)
	state.Implementation, _ = HexAddress(ImplementationHex)
	state.CodeHash = mustHex(ImplementationSHA)
	caller, _ := HexAddress(cfg.Callers[0])
	renter, _ := HexAddress("41" + strings.Repeat("11", 20))
	receiver, _ := HexAddress("41" + strings.Repeat("22", 20))
	ev := Evidence{Started: now, Received: Ptr(now.Add(time.Second)), Available: now.Add(time.Second), RequestHash: strings.Repeat("a", 64), ResponseHash: strings.Repeat("b", 64)}
	return &Collector{Config: cfg, State: &state}, Candidate{Renter: renter, Receiver: receiver, Verified: true}, caller, ev, now
}

func parseSyntheticReward(t *testing.T, body map[string]any) Probe {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	c, can, caller, ev, now := rewardParseContext()
	p, err := ParseProbe(raw, ev, can, caller, now, c)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPositiveRewardRequiresPerResponseEvidence(t *testing.T) {
	p := parseSyntheticReward(t, syntheticRewardResponse(t))
	if p.Status != "success_reward" || p.RewardConsistency != "matched" || p.RewardReturnSun.Cmp(big.NewInt(20000000)) != 0 {
		t.Fatalf("complete three-way evidence was not accepted: status=%s consistency=%s error=%s", p.Status, p.RewardConsistency, p.ErrorCode)
	}
}

func TestRewardResourceOperationIsNotCash(t *testing.T) {
	body := syntheticRewardResponse(t)
	transfers := body["internal_transactions"].([]any)
	resource := map[string]any{"caller_address": ContractHex, "transferTo_address": "41020c064bab81eb2ef520f1cc2a26f5f1867e29b3", "callValueInfo": []any{map[string]any{"callValue": 99000000}}, "note": hex.EncodeToString([]byte("unDelegateResourceOfEnergy"))}
	body["internal_transactions"] = append(transfers, resource)
	p := parseSyntheticReward(t, body)
	if p.RewardTransferSun == nil || p.RewardTransferSun.String() != "20000000" || p.Status != "success_reward" {
		t.Fatalf("resource amount contaminated reward: %+v", p)
	}
	body["internal_transactions"] = []any{resource}
	p = parseSyntheticReward(t, body)
	if p.Status == "success_reward" || p.RewardTransferSun == nil || p.RewardTransferSun.Sign() != 0 {
		t.Fatal("resource-only trace became payment", p)
	}
}

func TestRewardMissingMismatchedAndDuplicateEvidence(t *testing.T) {
	cases := []struct {
		name        string
		mutate      func(map[string]any)
		consistency string
	}{
		{"missing logs", func(b map[string]any) { delete(b, "logs") }, "incomplete"},
		{"missing transfers", func(b map[string]any) { delete(b, "internal_transactions") }, "incomplete"},
		{"null transfers", func(b map[string]any) { b["internal_transactions"] = nil }, "incomplete"},
		{"wrong cash", func(b map[string]any) {
			b["internal_transactions"].([]any)[0].(map[string]any)["callValueInfo"] = []any{map[string]any{"callValue": 1}}
		}, "mismatch"},
		{"rejected payment", func(b map[string]any) { b["internal_transactions"].([]any)[0].(map[string]any)["rejected"] = true }, "mismatch"},
		{"token payment", func(b map[string]any) {
			b["internal_transactions"].([]any)[0].(map[string]any)["callValueInfo"] = []any{map[string]any{"callValue": 20000000, "tokenId": "1000001"}}
		}, "mismatch"},
		{"duplicate logs", func(b map[string]any) { l := b["logs"].([]any); b["logs"] = append(l, l[0]) }, "mismatch"},
		{"unknown note", func(b map[string]any) { b["internal_transactions"].([]any)[0].(map[string]any)["note"] = "" }, "incomplete"},
		{"null rejection", func(b map[string]any) { b["internal_transactions"].([]any)[0].(map[string]any)["rejected"] = nil }, "incomplete"},
		{"wrong log caller", func(b map[string]any) {
			b["logs"].([]any)[0].(map[string]any)["topics"].([]string)[1] = strings.Repeat("0", 24) + strings.Repeat("33", 20)
		}, "mismatch"},
		{"wrong log renter", func(b map[string]any) {
			b["logs"].([]any)[0].(map[string]any)["topics"].([]string)[2] = strings.Repeat("0", 24) + strings.Repeat("33", 20)
		}, "mismatch"},
		{"wrong emitter", func(b map[string]any) {
			b["logs"].([]any)[0].(map[string]any)["address"] = "41" + strings.Repeat("33", 20)
		}, "incomplete"},
		{"wrong recipient", func(b map[string]any) {
			b["internal_transactions"].([]any)[0].(map[string]any)["transferTo_address"] = "41" + strings.Repeat("33", 20)
		}, "mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := syntheticRewardResponse(t)
			tc.mutate(b)
			p := parseSyntheticReward(t, b)
			if p.Status == "success_reward" || p.RewardConsistency != tc.consistency || p.RewardReturnSun == nil || p.RewardReturnSun.String() != "20000000" {
				t.Fatalf("unsafe or lost evidence: %+v", p)
			}
		})
	}
}

func TestRewardMalformedOrFailedExecutionCannotCertify(t *testing.T) {
	cases := []struct {
		name   string
		change func(map[string]any)
	}{
		{"API missing", func(b map[string]any) { b["result"] = map[string]any{} }},
		{"TVM null", func(b map[string]any) { b["transaction"] = map[string]any{"ret": []any{nil}} }},
		{"TVM out of energy", func(b map[string]any) {
			b["transaction"] = map[string]any{"ret": []any{map[string]any{"contractRet": "OUT_OF_ENERGY"}}}
		}},
		{"extra return", func(b map[string]any) {
			b["constant_result"] = []string{fmt.Sprintf("%064x", 20000000), fmt.Sprintf("%064x", 1)}
		}},
		{"257 bit return", func(b map[string]any) { b["constant_result"] = []string{"01" + strings.Repeat("0", 64)} }},
		{"API contradictory error", func(b map[string]any) {
			b["result"] = map[string]any{"result": true, "code": "OTHER_ERROR", "message": hex.EncodeToString([]byte("exception"))}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := syntheticRewardResponse(t)
			tc.change(b)
			p := parseSyntheticReward(t, b)
			if p.Status == "success_reward" {
				t.Fatal("invalid execution accepted")
			}
		})
	}
}

func TestMalformedRewardEvidencePreservesKnownAmounts(t *testing.T) {
	b := syntheticRewardResponse(t)
	b["internal_transactions"].([]any)[0].(map[string]any)["callValueInfo"] = []any{map[string]any{"callValue": json.RawMessage("1.5")}}
	p := parseSyntheticReward(t, b)
	if p.Status != "unknown" || p.ErrorCode != "reward_evidence_invalid" || p.RewardReturnSun == nil || p.RewardReturnSun.String() != "20000000" || p.RewardLogSun == nil || p.RewardLogSun.String() != "20000000" || p.RewardTransferSun != nil {
		t.Fatal("lost valid return/log or certified invalid cash", p)
	}
}

func TestPositiveProbeCommitValidation(t *testing.T) {
	base := parseSyntheticReward(t, syntheticRewardResponse(t))
	// Test the storage boundary independently of provisional parser classification.
	base.Status = "success_reward"
	base.RewardConsistency = "matched"
	now := *base.RequestStartedAt
	base.HeadBeforeNumber = Ptr(uint64(10))
	base.HeadAfterNumber = Ptr(uint64(11))
	base.HeadBeforeHash = Ptr(strings.Repeat("a", 32))
	base.HeadAfterHash = Ptr(strings.Repeat("b", 32))
	base.HeadBeforeTime = Ptr(now.Add(-time.Second))
	base.HeadAfterTime = Ptr(now.Add(time.Second))
	cases := []struct {
		name   string
		mutate func(*Probe)
	}{
		{"missing head", func(p *Probe) { p.HeadAfterHash = nil }},
		{"reversed heights", func(p *Probe) { p.HeadAfterNumber = Ptr(uint64(9)) }},
		{"same height conflict", func(p *Probe) { p.HeadAfterNumber = Ptr(uint64(10)) }},
		{"missing response hash", func(p *Probe) { p.ResponsePayloadHash = nil }},
		{"stale identity", func(p *Probe) { p.ImplementationCheckedAt = Ptr(now.Add(-11 * time.Minute)) }},
		{"wrong implementation", func(p *Probe) { p.ImplementationAddress = Ptr(strings.Repeat("x", 21)) }},
		{"wrong code", func(p *Probe) { p.ImplementationCodeHash = Ptr(strings.Repeat("x", 32)) }},
		{"missing tvm", func(p *Probe) { p.TvmResult = "" }},
		{"tvm failed", func(p *Probe) { p.TvmResult = `"FAILED"` }},
		{"api missing", func(p *Probe) { p.ApiSuccess = nil }},
		{"reward mismatch", func(p *Probe) { p.RewardTransferSun = big.NewInt(1) }},
		{"claimed match without log", func(p *Probe) { p.RewardLogSun = nil }},
		{"own order", func(p *Probe) { p.CallerAddress = p.Renter }},
		{"negative reward", func(p *Probe) { p.RewardReturnSun = big.NewInt(-1) }},
	}
	if err := validatePositiveProbe(base); err != nil {
		t.Fatal("complete observation rejected", err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.mutate(&p)
			if validatePositiveProbe(p) == nil {
				t.Fatal("invalid positive observation accepted")
			}
		})
	}
}
