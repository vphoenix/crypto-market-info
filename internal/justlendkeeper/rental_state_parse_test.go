package justlendkeeper

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
)

func TestRentalStateExactABIAndNulls(t *testing.T) {
	// Synthetic vectors exercise ABI boundaries; they are not observed rental states.
	max := strings.Repeat("f", 64)
	cases := []struct {
		name, words, status string
		liquidatable        *bool
	}{
		{"positive flag", fmt.Sprintf("%064x%064x%064x", 123, 456, 1), "ok", Ptr(true)},
		{"zero is a real value", strings.Repeat("0", 192), "ok", Ptr(false)},
		{"uint256 maximum", max + max + fmt.Sprintf("%064x", 1), "ok", Ptr(true)},
		{"number starts with error selector", "08c379a0" + strings.Repeat("0", 120) + fmt.Sprintf("%064x", 1), "ok", Ptr(true)},
		{"noncanonical bool", fmt.Sprintf("%064x%064x%064x", 123, 456, 2), "unknown", nil},
		{"bool high bit", strings.Repeat("0", 128) + "1" + strings.Repeat("0", 63), "unknown", nil},
		{"truncated", strings.Repeat("0", 190), "unknown", nil},
		{"trailing word", strings.Repeat("0", 256), "unknown", nil},
		{"bad hex", strings.Repeat("0", 191) + "g", "unknown", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"result": map[string]any{"result": true}, "transaction": map[string]any{"ret": []any{map[string]any{}}}, "constant_result": []string{tc.words}}
			raw, _ := json.Marshal(body)
			c, can, caller, ev, now := rewardParseContext()
			r, err := ParseRentalState(raw, ev, can, caller, now, c)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != tc.status || (r.Liquidatable == nil) != (tc.liquidatable == nil) {
				t.Fatalf("status=%s liquidatable=%v", r.Status, r.Liquidatable)
			}
			if tc.liquidatable == nil {
				if r.SecurityDepositSun != nil || r.RentIndex != nil {
					t.Fatal("invalid tuple fabricated numbers")
				}
				return
			}
			if *r.Liquidatable != *tc.liquidatable || r.TvmResult != "SUCCESS" {
				t.Fatal("tuple/TVM wrong", r)
			}
			if tc.name == "positive flag" && (r.SecurityDepositSun.Cmp(big.NewInt(123)) != 0 || r.RentIndex.Cmp(big.NewInt(456)) != 0) {
				t.Fatal("tuple order or units wrong")
			}
			if tc.name == "uint256 maximum" && r.SecurityDepositSun.BitLen() != 256 {
				t.Fatal("uint256 narrowed")
			}
		})
	}
}

func TestRentalStateExecutionFailuresKeepTupleUnknown(t *testing.T) {
	cases := []struct{ name, body, status string }{
		{"API absent", `{"transaction":{"ret":[{}]},"constant_result":["` + strings.Repeat("0", 192) + `"]}`, "rpc_error"},
		{"API false", `{"result":{"result":false}}`, "rpc_error"},
		{"TVM absent", `{"result":{"result":true},"constant_result":["` + strings.Repeat("0", 192) + `"]}`, "unknown"},
		{"TVM null", `{"result":{"result":true},"transaction":{"ret":[null]},"constant_result":["` + strings.Repeat("0", 192) + `"]}`, "unknown"},
		{"revert", `{"result":{"result":true,"message":"524556455254"},"transaction":{"ret":[{"ret":"FAILED"}]},"constant_result":[]}`, "revert"},
		{"out of energy", `{"result":{"result":true},"transaction":{"ret":[{"contractRet":"OUT_OF_ENERGY"}]}}`, "tvm_failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, can, caller, ev, now := rewardParseContext()
			r, err := ParseRentalState([]byte(tc.body), ev, can, caller, now, c)
			if err != nil || r.Status != tc.status || r.SecurityDepositSun != nil || r.RentIndex != nil || r.Liquidatable != nil {
				t.Fatalf("fabricated state/status: err=%v %+v", err, r)
			}
		})
	}
}

func TestArchivedRentalStateFalse(t *testing.T) {
	raw, err := os.ReadFile("testdata/keeper-rental-state/rent-info.raw.json")
	if err != nil {
		t.Fatal(err)
	}
	if Hex(Hash(raw)) != "ed2559f47f62da51db7a0d1b89dbc00ed69dd5357cd630848a96bfc8ad1874e9" {
		t.Fatal("archived fixture no longer matches provenance")
	}
	c, can, caller, ev, now := rewardParseContext()
	r, err := ParseRentalState(raw, ev, can, caller, now, c)
	if err != nil || r.Status != "ok" || r.Liquidatable == nil || *r.Liquidatable || r.SecurityDepositSun == nil || r.SecurityDepositSun.Sign() != 0 || r.RentIndex == nil || r.RentIndex.String() != "1054569691767344913" {
		t.Fatal("real 96-byte tuple changed", err, r)
	}
	c.State.IdentityStatus = "unknown"
	r, err = ParseRentalState(raw, ev, can, caller, now, c)
	if err != nil || r.Status == "ok" || r.RentIndex == nil {
		t.Fatal("unknown identity must preserve decoded values without trust", err, r)
	}
}
