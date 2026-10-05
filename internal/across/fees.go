package across

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

const baseGasPriceOracle = "0x420000000000000000000000000000000000000F"

var operatorFeeSelector = fmt.Sprintf("0x%x", crypto.Keccak256([]byte("getOperatorFee(uint256)"))[:4])

// Receipt gasUsed already includes the execution engine's gas refund and floor
// adjustments. Do not use the transaction gas limit or subtract refunds again.
func operatorFeeFromParameters(gas uint64, scalar, constant *big.Int, jovian bool) *big.Int {
	fee := new(big.Int).Mul(new(big.Int).SetUint64(gas), scalar)
	if jovian {
		fee.Mul(fee, big.NewInt(100))
	} else {
		fee.Quo(fee, big.NewInt(1_000_000))
	}
	return fee.Add(fee, constant)
}

// baseOperatorFee binds every inferred amount, including zero when receipt
// parameters are omitted, to the deployed oracle at this exact canonical block.
// The oracle evaluates the applicable historical rule. If receipt parameters
// exist, independently compute both documented rules and require agreement; no
// assumption about the current chain upgrade schedule enters historical rows.
func (r *Reader) baseOperatorFee(ctx context.Context, b Block, gas uint64, txType uint8, explicit *big.Int, scalarText, constantText *string) (*big.Int, string, error) {
	var scalar, constant *big.Int
	if (scalarText == nil) != (constantText == nil) {
		return nil, "", errors.New("incomplete_operator_fee_parameters")
	}
	if scalarText != nil {
		var err error
		scalar, err = Quantity(*scalarText)
		if err != nil || scalar.BitLen() > 32 {
			return nil, "", errors.New("invalid_operator_fee_scalar")
		}
		constant, err = Quantity(*constantText)
		if err != nil || constant.BitLen() > 64 {
			return nil, "", errors.New("invalid_operator_fee_constant")
		}
	}
	// OP deposit/system transactions are exempt from operator fees; their full
	// L1/L2 accounting is outside this ordinary-transaction path. Blob or future
	// transaction types likewise require their own fee-component specification.
	if txType != 0 && txType != 1 && txType != 2 && txType != 4 {
		return nil, "base_unsupported_transaction_type", nil
	}
	data := operatorFeeSelector + fmt.Sprintf("%064x", gas)
	v := r.one(ctx, "eth_call", []any{map[string]string{"to": baseGasPriceOracle, "data": data}, ref(b.Hash)})
	if v.Err != nil {
		var remote *ethereum.RPCError
		if errors.As(v.Err, &remote) && (remote.Code == -32601 || remote.Code == 3 || remote.Code == -32000 && strings.Contains(strings.ToLower(remote.Message), "execution reverted")) {
			// Historical deployments can lack this method. The archived failure
			// proves lack of a usable fee proof, never a zero fee.
			return nil, "base_operator_fee_proof_unavailable", nil
		}
		return nil, "", v.Err
	}
	word, err := resultBytes(v)
	if err != nil {
		return nil, "", err
	}
	if len(word) == 0 {
		return nil, "base_operator_fee_proof_unavailable", nil
	}
	if len(word) != 32 {
		return nil, "", errors.New("invalid_operator_fee_oracle_result")
	}
	fee := new(big.Int).SetBytes([]byte(word))
	rule := "base_block_oracle_components"
	if scalar != nil {
		isthmus := operatorFeeFromParameters(gas, scalar, constant, false)
		jovian := operatorFeeFromParameters(gas, scalar, constant, true)
		switch {
		case fee.Cmp(isthmus) == 0 && fee.Cmp(jovian) == 0:
			rule = "base_receipt_parameters_oracle_equal"
		case fee.Cmp(isthmus) == 0:
			rule = "base_isthmus_parameters_oracle"
		case fee.Cmp(jovian) == 0:
			rule = "base_jovian_parameters_oracle"
		default:
			return nil, "", errors.New("operator_fee_parameters_oracle_mismatch")
		}
	}
	if explicit != nil && explicit.Cmp(fee) != 0 {
		return nil, "", errors.New("operator_fee_amount_mismatch")
	}
	return fee, rule, nil
}
