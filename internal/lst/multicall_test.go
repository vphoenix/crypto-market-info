package lst

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProtocolMulticallExactStateAndEvidence(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	b := Block{Number: 100, Hash: strings.Repeat("h", 32), Time: time.Unix(1790000000, 0).UTC(), BaseFee: big.NewInt(17)}
	contract, e := abi.JSON(strings.NewReader(multicallABIJSON))
	if e != nil {
		t.Fatal(e)
	}
	specs := protocolViewSpecs()
	results := make([]multicallResult, len(specs))
	values := make([]any, len(specs))
	for i, s := range specs {
		var value any = new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 80), big.NewInt(int64(i)))
		if s.output == "address" {
			role := "steth_impl"
			if i == 1 {
				role = "queue_impl"
			}
			value = common.HexToAddress(m.Addresses[role])
		}
		if s.output == "bool" {
			value = false
		}
		if i == 7 {
			value = new(big.Int).Set(values[6].(*big.Int))
		}
		if i == 13 {
			value = new(big.Int).SetUint64(b.Number)
		}
		if i == 14 {
			value = big.NewInt(b.Time.Unix())
		}
		args, _ := arguments([]string{s.output})
		raw, err := args.Pack(value)
		if err != nil {
			t.Fatal(err)
		}
		values[i] = value
		results[i] = multicallResult{true, raw}
	}
	outputs := contract.Methods["aggregate3"].Outputs
	cases := []struct {
		name   string
		mutate func([]multicallResult) []multicallResult
		tail   bool
		want   bool
	}{
		{"valid", nil, false, true},
		{"partial_failure", func(r []multicallResult) []multicallResult { r[3].Success = false; return r }, false, false},
		{"short_array", func(r []multicallResult) []multicallResult { return r[:14] }, false, false},
		{"bad_bool", func(r []multicallResult) []multicallResult {
			r[11].ReturnData = make([]byte, 32)
			r[11].ReturnData[31] = 2
			return r
		}, false, false},
		{"bad_address_padding", func(r []multicallResult) []multicallResult {
			r[0].ReturnData = append([]byte(nil), r[0].ReturnData...)
			r[0].ReturnData[0] = 1
			return r
		}, false, false},
		{"wrong_execution_block", func(r []multicallResult) []multicallResult { r[13].ReturnData = make([]byte, 32); return r }, false, false},
		{"outer_trailing_data", nil, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := append([]multicallResult(nil), results...)
			if tc.mutate != nil {
				rs = tc.mutate(rs)
			}
			payload, err := outputs.Pack(rs)
			if err != nil {
				t.Fatal(err)
			}
			if tc.tail {
				payload = append(payload, 0)
			}
			calls := 0
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					Id     uint64
					Method string
					Params []json.RawMessage
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				if call.Method != "eth_call" || len(call.Params) != 2 {
					t.Fatal("unexpected RPC", call)
				}
				var target map[string]string
				json.Unmarshal(call.Params[0], &target)
				if target["to"] != Hex(multicallAddress()) {
					t.Fatal("unexpected target", target)
				}
				var ref struct {
					BlockHash        string
					RequireCanonical bool
				}
				json.Unmarshal(call.Params[1], &ref)
				if ref.BlockHash != Hex(b.Hash) || !ref.RequireCanonical {
					t.Fatal("unpinned multicall", ref)
				}
				data, err := hex.DecodeString(strings.TrimPrefix(target["data"], "0x"))
				if err != nil {
					t.Fatal(err)
				}
				args, err := contract.Methods["aggregate3"].Inputs.Unpack(data[4:])
				if err != nil {
					t.Fatal(err)
				}
				batched := *abi.ConvertType(args[0], new([]multicallInput)).(*[]multicallInput)
				if len(batched) != 15 {
					t.Fatal("missing views", len(batched))
				}
				for i, c := range batched {
					if !c.AllowFailure {
						t.Fatal("diagnostics missing", i)
					}
				}
				return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%s"}`, call.Id, hex.EncodeToString(payload))), nil
			})
			rpc := RPC{Transport: tr, URL: "https://rpc.example", ProtocolMulticall: true}
			p, proof, err := rpc.Protocol(context.Background(), m, b)
			if (err == nil) != tc.want || calls != 1 || len(proof) != 2 || len(p.PayloadHashes) != 1 || p.RequestedAt == nil {
				t.Fatalf("invalid evidence/status calls=%d proofs=%d status=%s err=%v", calls, len(proof), p.StateStatus, err)
			}
			if tc.want {
				if p.StateStatus != "ok" || p.StethTotalSharesRaw.Cmp(values[3].(*big.Int)) != 0 || p.OneWstethToStethWei.Cmp(values[10].(*big.Int)) != 0 || *p.QueuePaused {
					t.Fatal("state not exact", p)
				}
			} else if p.StateStatus == "ok" || p.Reason == "" {
				t.Fatal("false success", p)
			}
		})
	}
}
