package lst

// Multicall3 only aggregates fixed, caller-independent protocol views. It is
// called through eth_call at the same requireCanonical blockHash as each quote.
// Runtime code is independently pinned without changing the log manifest/cursor.
import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const multicallCodeHash = "0xd5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891"

func multicallAddress() string {
	return string(common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11").Bytes())
}

const multicallABIJSON = `[{"type":"function","name":"aggregate3","stateMutability":"payable","inputs":[{"name":"calls","type":"tuple[]","components":[{"name":"target","type":"address"},{"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}]}],"outputs":[{"name":"returnData","type":"tuple[]","components":[{"name":"success","type":"bool"},{"name":"returnData","type":"bytes"}]}]}]`

type multicallInput struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}
type multicallResult struct {
	Success    bool
	ReturnData []byte
}
type protocolViewSpec struct {
	role, sig, output string
	value             *big.Int
}

func protocolViewSpecs() []protocolViewSpec {
	return []protocolViewSpec{
		{"steth", "implementation()", "address", nil}, {"queue", "proxy__getImplementation()", "address", nil},
		{"steth", "getTotalPooledEther()", "uint256", nil}, {"steth", "getTotalShares()", "uint256", nil},
		{"queue", "MIN_STETH_WITHDRAWAL_AMOUNT()", "uint256", nil}, {"queue", "MAX_STETH_WITHDRAWAL_AMOUNT()", "uint256", nil},
		{"queue", "getLastRequestId()", "uint256", nil}, {"queue", "getLastFinalizedRequestId()", "uint256", nil},
		{"queue", "unfinalizedStETH()", "uint256", nil}, {"queue", "getLockedEtherAmount()", "uint256", nil},
		{"wsteth", "getStETHByWstETH(uint256)", "uint256", big.NewInt(1e18)},
		{"queue", "isPaused()", "bool", nil}, {"queue", "isBunkerModeActive()", "bool", nil},
		{"multicall", "getBlockNumber()", "uint256", nil}, {"multicall", "getCurrentBlockTimestamp()", "uint256", nil},
	}
}
func (r *RPC) protocolMulticall(ctx context.Context, m Manifest, b Block) ([][]any, Response, error) {
	contract, err := abi.JSON(strings.NewReader(multicallABIJSON))
	if err != nil {
		return nil, Response{}, err
	}
	specs := protocolViewSpecs()
	calls := make([]multicallInput, len(specs))
	for i, s := range specs {
		address := m.Address(s.role)
		if s.role == "multicall" {
			address = multicallAddress()
		}
		data := append([]byte(nil), crypto.Keccak256([]byte(s.sig))[:4]...)
		if s.value != nil {
			args, _ := arguments([]string{"uint256"})
			encoded, e := args.Pack(s.value)
			if e != nil {
				return nil, Response{}, e
			}
			data = append(data, encoded...)
		}
		calls[i] = multicallInput{common.BytesToAddress([]byte(address)), true, data}
	}
	data, err := contract.Pack("aggregate3", calls)
	if err != nil {
		return nil, Response{}, err
	}
	raw, res, err := r.Call(ctx, "eth_call", []any{map[string]string{"to": Hex(multicallAddress()), "data": "0x" + hex.EncodeToString(data), "gas": "0x4c4b40"}, blockRef(b)}, "normal")
	if err != nil {
		return nil, res, err
	}
	var encoded string
	if err = exactJSON(raw, &encoded); err != nil {
		return nil, res, err
	}
	decoded, err := decodeBytes(encoded)
	if err != nil {
		return nil, res, err
	}
	outs := contract.Methods["aggregate3"].Outputs
	unpacked, err := outs.Unpack(decoded)
	if err != nil {
		return nil, res, fmt.Errorf("multicall_abi: %w", err)
	}
	canonical, err := outs.Pack(unpacked...)
	if err != nil || !bytes.Equal(canonical, decoded) {
		return nil, res, errors.New("multicall_noncanonical_abi")
	}
	results := *abi.ConvertType(unpacked[0], new([]multicallResult)).(*[]multicallResult)
	if len(results) != len(specs) {
		return nil, res, errors.New("multicall_result_count")
	}
	views := make([][]any, len(specs))
	for i, result := range results {
		spec := specs[i]
		if !result.Success {
			return nil, res, fmt.Errorf("multicall_view_failed_%s_%s", spec.role, spec.sig)
		}
		if len(result.ReturnData) != 32 {
			return nil, res, fmt.Errorf("multicall_view_length_%s_%s", spec.role, spec.sig)
		}
		args, _ := arguments([]string{spec.output})
		views[i], err = args.Unpack(result.ReturnData)
		if err != nil {
			return nil, res, fmt.Errorf("multicall_view_abi_%s_%s: %w", spec.role, spec.sig, err)
		}
		again, e := args.Pack(views[i]...)
		if e != nil || !bytes.Equal(again, result.ReturnData) {
			return nil, res, fmt.Errorf("multicall_view_noncanonical_%s_%s", spec.role, spec.sig)
		}
	}
	if views[13][0].(*big.Int).Cmp(new(big.Int).SetUint64(b.Number)) != 0 || views[14][0].(*big.Int).Cmp(big.NewInt(b.Time.Unix())) != 0 {
		return nil, res, errors.New("multicall_execution_block_mismatch")
	}
	return views[:13], res, nil
}
