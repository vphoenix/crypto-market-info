package across

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestBaseOperatorFeeExactParametersAndProof(t *testing.T) {
	for _, tc := range []struct {
		name, scalar, constant, oracle, want, rule, err string
		explicit                                        *big.Int
	}{
		{name: "omitted_zero_proved", oracle: "0", want: "0", rule: "base_block_oracle_components"},
		{name: "omitted_nonzero_proved", oracle: "9876543210123456789", want: "9876543210123456789", rule: "base_block_oracle_components"},
		{name: "isthmus_floors_after_multiply", scalar: "0x2", constant: "0x7", oracle: "9", want: "9", rule: "base_isthmus_parameters_oracle"},
		{name: "jovian_does_not_divide", scalar: "0x2", constant: "0x7", oracle: "200000207", want: "200000207", rule: "base_jovian_parameters_oracle"},
		{name: "constant_over_float_precision", scalar: "0x0", constant: "0xffffffffffffffff", oracle: "18446744073709551615", want: "18446744073709551615", rule: "base_receipt_parameters_oracle_equal"},
		{name: "parameters_do_not_match_oracle", scalar: "0x2", constant: "0x7", oracle: "8", err: "operator_fee_parameters_oracle_mismatch"},
		{name: "explicit_amount_contradiction", oracle: "0", explicit: big.NewInt(1), err: "operator_fee_amount_mismatch"},
		{name: "only_scalar", scalar: "0x0", oracle: "0", err: "incomplete_operator_fee_parameters"},
		{name: "only_constant", constant: "0x0", oracle: "0", err: "incomplete_operator_fee_parameters"},
		{name: "scalar_overflow", scalar: "0x100000000", constant: "0x0", oracle: "0", err: "invalid_operator_fee_scalar"},
		{name: "constant_overflow", scalar: "0x0", constant: "0x10000000000000000", oracle: "0", err: "invalid_operator_fee_constant"},
		{name: "noncanonical_quantity", scalar: "0x00", constant: "0x0", oracle: "0", err: "invalid_operator_fee_scalar"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const gas = uint64(1_000_001)
			blockHash := strings.Repeat("h", 32)
			oracle, _ := new(big.Int).SetString(tc.oracle, 10)
			r := mockReader(t, 8453, func(method string, p json.RawMessage) any {
				if method != "eth_call" {
					t.Fatal(method)
				}
				var params []json.RawMessage
				if err := json.Unmarshal(p, &params); err != nil || len(params) != 2 {
					t.Fatal("invalid call params", string(p))
				}
				var call map[string]string
				var anchor struct {
					BlockHash        string
					RequireCanonical bool
				}
				if json.Unmarshal(params[0], &call) != nil || json.Unmarshal(params[1], &anchor) != nil {
					t.Fatal("invalid call structure")
				}
				if call["to"] != baseGasPriceOracle || call["data"] != operatorFeeSelector+fmt.Sprintf("%064x", gas) || anchor.BlockHash != Hex(blockHash) || !anchor.RequireCanonical {
					t.Fatal("fee not bound to receipt gas and exact canonical block", string(p))
				}
				return fmt.Sprintf("0x%064x", oracle)
			})
			var scalar, constant *string
			if tc.scalar != "" {
				scalar = &tc.scalar
			}
			if tc.constant != "" {
				constant = &tc.constant
			}
			fee, rule, err := r.baseOperatorFee(context.Background(), Block{ChainID: 8453, Hash: blockHash}, gas, 2, tc.explicit, scalar, constant)
			if tc.err != "" {
				if err == nil || err.Error() != tc.err {
					t.Fatalf("fee=%v rule=%s err=%v want=%s", fee, rule, err, tc.err)
				}
				return
			}
			if err != nil || fee == nil || fee.String() != tc.want || rule != tc.rule {
				t.Fatalf("fee=%v rule=%s err=%v", fee, rule, err)
			}
			if len(r.Members) != 1 {
				t.Fatal("oracle evidence missing")
			}
			var hash dex.Hash
			copy(hash[:], r.Members[0])
			if _, err := r.RPC.Archive.Get(hash); err != nil {
				t.Fatal("oracle payload not archived", err)
			}
		})
	}
	// Independent known bounds from the specification; all intermediate values
	// exceed uint64 here, so accidental machine-integer arithmetic is caught.
	maxScalar := new(big.Int).SetUint64(1<<32 - 1)
	maxConstant := new(big.Int).SetUint64(1<<64 - 1)
	if got := operatorFeeFromParameters(1<<64-1, maxScalar, maxConstant, false).String(); got != "79246609239891303067154" {
		t.Fatal("isthmus max", got)
	}
	if got := operatorFeeFromParameters(1<<64-1, maxScalar, maxConstant, true).String(); got != "7922816249600206095627652694115" {
		t.Fatal("jovian max", got)
	}
}

func TestBaseOperatorFeeUnavailabilityAndTransportErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		result    any
		remote    *ethereum.RPCError
		transport error
		wantError bool
	}{
		{name: "empty_old_contract", result: "0x"},
		{name: "revert", remote: &ethereum.RPCError{Code: 3, Message: "execution reverted"}},
		{name: "method_missing", remote: &ethereum.RPCError{Code: -32601, Message: "method not found"}},
		{name: "null", result: nil, wantError: true},
		{name: "short_abi_result", result: "0x00", wantError: true},
		{name: "extra_abi_word", result: "0x" + strings.Repeat("0", 128), wantError: true},
		{name: "rate_limited", remote: &ethereum.RPCError{Code: -32016, Message: "over rate limit"}, wantError: true},
		{name: "state_pruned", remote: &ethereum.RPCError{Code: -32000, Message: "historical state unavailable"}, wantError: true},
		{name: "network", transport: errors.New("network failure"), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := mockReader(t, 8453, func(string, json.RawMessage) any { return tc.result })
			if tc.remote != nil || tc.transport != nil {
				r.RPC.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
					if tc.transport != nil {
						return nil, tc.transport
					}
					raw, _ := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": tc.remote.Code, "message": tc.remote.Message}}})
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
				})
			}
			fee, _, err := r.baseOperatorFee(context.Background(), Block{ChainID: 8453, Hash: strings.Repeat("h", 32)}, 120054, 2, nil, nil, nil)
			if fee != nil || (err != nil) != tc.wantError {
				t.Fatalf("unknown fee must stay unknown: %v %v", fee, err)
			}
		})
	}
	for _, typ := range []uint8{3, 0x7e, 0x7f, 0xff} {
		r := mockReader(t, 8453, func(string, json.RawMessage) any {
			t.Fatal("unsupported type must not use ordinary gas formula")
			return nil
		})
		fee, rule, err := r.baseOperatorFee(context.Background(), Block{}, 1, typ, nil, nil, nil)
		if fee != nil || err != nil || rule != "base_unsupported_transaction_type" {
			t.Fatalf("type %d: %v %s %v", typ, fee, rule, err)
		}
	}
}

func TestBaseReceiptOracleProofAndReorg(t *testing.T) {
	for _, reorg := range []bool{false, true} {
		t.Run(fmt.Sprintf("reorg=%v", reorg), func(t *testing.T) {
			hash, txHash := Hex(strings.Repeat("h", 32)), Hex(strings.Repeat("t", 32))
			address := Hex(strings.Repeat("a", 20))
			headers := 0
			r := mockReader(t, 8453, func(method string, p json.RawMessage) any {
				switch method {
				case "eth_getTransactionByHash":
					return map[string]any{"hash": txHash, "blockHash": hash, "blockNumber": "0x7b", "transactionIndex": "0x0", "from": address, "to": address, "type": "0x2", "input": "0x12345678", "value": "0x0", "chainId": "0x2105", "gas": "0xfffff"}
				case "eth_getTransactionReceipt":
					return map[string]any{"transactionHash": txHash, "blockHash": hash, "blockNumber": "0x7b", "transactionIndex": "0x0", "from": address, "to": address, "status": "0x0", "gasUsed": "0x1d4f6", "effectiveGasPrice": "0x7e6523", "l1Fee": "0x5aed120e", "logs": []any{}}
				case "eth_getBlockByNumber":
					headers++
					h := hash
					if reorg && headers == 2 {
						h = Hex(strings.Repeat("r", 32))
					}
					return map[string]any{"number": "0x7b", "hash": h, "parentHash": Hex(strings.Repeat("p", 32)), "timestamp": "0x3e8"}
				case "eth_call":
					if !strings.Contains(string(p), fmt.Sprintf("%064x", 120054)) {
						t.Fatal("gas limit or refund was incorrectly substituted", string(p))
					}
					return "0x" + strings.Repeat("0", 64)
				}
				t.Fatal(method)
				return nil
			})
			got, err := r.Receipt(context.Background(), strings.Repeat("t", 32))
			if reorg {
				if err == nil {
					t.Fatal("fee proof accepted after receipt reorg")
				}
				return
			}
			if err != nil || !got.FeeComplete || got.TotalFeeWei.String() != "995984031152" || got.OperatorFeeWei.Sign() != 0 || got.Success {
				t.Fatalf("failed tx still pays exact fee: %+v err=%v", got, err)
			}
			if headers != 2 || len(r.Members) != 5 {
				t.Fatalf("missing anchored evidence: headers=%d members=%d", headers, len(r.Members))
			}
		})
	}
}
