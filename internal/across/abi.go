package across

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ABI is the union of events and read-only methods from the two verified deployed
// implementations, whose RPC bytecode was matched to the explorer bytecode.
//
//go:embed abi.json
var abiJSON string
var ABI = func() abi.ABI {
	a, e := abi.JSON(strings.NewReader(abiJSON))
	if e != nil {
		panic(e)
	}
	return a
}()

const ABIRevision = "across-deployed-2026-10-02"

type DecodeMeta struct {
	CaptureID, ABIRevision string
	AvailableAt            time.Time
	LiveReceivedAt         *time.Time
}
type Log struct {
	ChainID           uint64
	Address           string
	BlockNumber       uint64
	BlockHash, TxHash string
	TxIndex, LogIndex uint32
	Topics            []string
	Data              string
	PayloadHash       string
	Removed           bool
}

func WordAddress(a string) string { return strings.Repeat("\x00", 12) + a }
func AddressWord(w string) (string, error) {
	if len(w) != 32 || w[:12] != strings.Repeat("\x00", 12) {
		return "", errors.New("non_evm_address_word")
	}
	return w[12:], nil
}
func MessageHash(s string) string {
	if len(s) == 0 {
		return strings.Repeat("\x00", 32)
	}
	return string(crypto.Keccak256([]byte(s)))
}
func IsUpgradeLog(l Log) bool {
	e := ABI.Events["Upgraded"]
	return len(l.Topics) > 0 && l.Topics[0] == string(e.ID[:])
}
func number(v any) *big.Int {
	if n, ok := v.(*big.Int); ok {
		return new(big.Int).Set(n)
	}
	return new(big.Int).SetUint64(uint64(v.(uint32)))
}
func word(v any) string {
	if a, ok := v.(common.Address); ok {
		return WordAddress(string(a[:]))
	}
	a := v.([32]byte)
	return string(a[:])
}
func to64(v any) (uint64, error) {
	n := number(v)
	if !n.IsUint64() {
		return 0, errors.New("chain_id_overflow")
	}
	return n.Uint64(), nil
}
func hashWord(s string) [32]byte { var b [32]byte; copy(b[:], s); return b }

// RelayHash includes the dynamic message and complete tuple; messageHash cannot
// replace message in this hash. Current and legacy event fields are normalized
// to the bytes32/uint256 relay structure used by this verified implementation.
func RelayHash(d Deposit) (string, error) {
	if len(d.Depositor) != 32 || len(d.Recipient) != 32 || len(d.ExclusiveRelayer) != 32 || len(d.InputToken) != 32 || len(d.OutputToken) != 32 || d.DepositId == nil || d.InputAmountRaw == nil || d.OutputAmountRaw == nil {
		return "", errors.New("invalid_relay_tuple")
	}
	for _, n := range []*big.Int{d.DepositId, d.InputAmountRaw, d.OutputAmountRaw} {
		if n.Sign() < 0 || n.BitLen() > 256 {
			return "", errors.New("relay_uint256_range")
		}
	}
	type relay struct {
		Depositor, Recipient, ExclusiveRelayer, InputToken, OutputToken [32]byte
		InputAmount, OutputAmount, OriginChainId, DepositId             *big.Int
		FillDeadline, ExclusivityDeadline                               uint32
		Message                                                         []byte
	}
	v := relay{hashWord(d.Depositor), hashWord(d.Recipient), hashWord(d.ExclusiveRelayer), hashWord(d.InputToken), hashWord(d.OutputToken), d.InputAmountRaw, d.OutputAmountRaw, new(big.Int).SetUint64(d.ChainId), d.DepositId, d.FillDeadline, d.ExclusivityDeadline, []byte(d.Message)}
	typ, e := abi.NewType("uint256", "", nil)
	if e != nil {
		return "", e
	}
	args := append(abi.Arguments{}, ABI.Methods["getV3RelayHash"].Inputs...)
	args = append(args, abi.Argument{Type: typ})
	b, e := args.Pack(v, new(big.Int).SetUint64(d.DestinationChainId))
	if e != nil {
		return "", e
	}
	return string(crypto.Keccak256(b)), nil
}

func eventValues(l Log) (string, map[string]any, bool, error) {
	if len(l.Topics) == 0 {
		return "", nil, false, nil
	}
	var ev *abi.Event
	for _, e := range ABI.Events {
		if l.Topics[0] == string(e.ID[:]) {
			v := e
			ev = &v
			break
		}
	}
	if ev == nil {
		return "", nil, false, nil
	}
	non := ev.Inputs.NonIndexed()
	vals, e := non.Unpack([]byte(l.Data))
	if e != nil {
		return "", nil, true, e
	}
	packed, e := non.Pack(vals...)
	if e != nil || !bytes.Equal(packed, []byte(l.Data)) {
		return "", nil, true, errors.New("noncanonical_event_data")
	}
	out := map[string]any{}
	for i, a := range non {
		out[a.Name] = vals[i]
	}
	indexed := abi.Arguments{}
	for _, a := range ev.Inputs {
		if a.Indexed {
			indexed = append(indexed, a)
		}
	}
	if len(l.Topics) != len(indexed)+1 {
		return "", nil, true, errors.New("event_topic_count")
	}
	for i, a := range indexed {
		if len(l.Topics[i+1]) != 32 {
			return "", nil, true, errors.New("event_topic_width")
		}
		a.Indexed = false
		one := abi.Arguments{a}
		v, e := one.Unpack([]byte(l.Topics[i+1]))
		if e != nil {
			return "", nil, true, e
		}
		p, e := one.Pack(v...)
		if e != nil || string(p) != l.Topics[i+1] {
			return "", nil, true, errors.New("noncanonical_event_topic")
		}
		out[a.Name] = v[0]
	}
	return ev.Name, out, true, nil
}
func eventAnchor(v any, l Log, b Block, m DecodeMeta) {
	r := reflect.ValueOf(v).Elem()
	for k, x := range map[string]any{"CaptureId": m.CaptureID, "ChainId": l.ChainID, "SpokePool": l.Address, "BlockNumber": l.BlockNumber, "BlockHash": l.BlockHash, "BlockTime": b.Time, "TxHash": l.TxHash, "TxIndex": l.TxIndex, "LogIndex": l.LogIndex, "AbiRevision": m.ABIRevision, "AvailableAt": m.AvailableAt, "PayloadHash": l.PayloadHash} {
		r.FieldByName(k).Set(reflect.ValueOf(x))
	}
	if f := r.FieldByName("LiveReceivedAt"); f.IsValid() {
		f.Set(reflect.ValueOf(m.LiveReceivedAt))
	}
}
func DecodeLog(l Log, b Block, m DecodeMeta) (Batch, bool, error) {
	var out Batch
	if l.Removed || l.ChainID != b.ChainID || l.BlockNumber != b.Number || l.BlockHash != b.Hash {
		return out, false, errors.New("log_header_anchor_mismatch")
	}
	if m.ABIRevision != ABIRevision {
		return out, false, errors.New("unknown_abi_revision")
	}
	n, v, known, e := eventValues(l)
	if e != nil || !known {
		return out, known, e
	}
	switch n {
	case "FundsDeposited", "V3FundsDeposited":
		d := Deposit{}
		eventAnchor(&d, l, b, m)
		d.DestinationChainId, e = to64(v["destinationChainId"])
		if e != nil {
			return out, true, e
		}
		d.DepositId = number(v["depositId"])
		d.Depositor = word(v["depositor"])
		d.Recipient = word(v["recipient"])
		d.ExclusiveRelayer = word(v["exclusiveRelayer"])
		d.InputToken = word(v["inputToken"])
		d.OutputToken = word(v["outputToken"])
		d.InputAmountRaw = number(v["inputAmount"])
		d.OutputAmountRaw = number(v["outputAmount"])
		d.QuoteTimestamp = v["quoteTimestamp"].(uint32)
		d.FillDeadline = v["fillDeadline"].(uint32)
		d.ExclusivityDeadline = v["exclusivityDeadline"].(uint32)
		d.Message = string(v["message"].([]byte))
		d.MessageHash = MessageHash(d.Message)
		d.RelayHash, e = RelayHash(d)
		if e != nil {
			return out, true, e
		}
		out.Deposits = []Deposit{d}
	case "RequestedSpeedUpDeposit", "RequestedSpeedUpV3Deposit":
		d := DepositUpdate{}
		eventAnchor(&d, l, b, m)
		d.DepositId = number(v["depositId"])
		d.Depositor = word(v["depositor"])
		d.UpdatedOutputAmountRaw = number(v["updatedOutputAmount"])
		d.UpdatedRecipient = word(v["updatedRecipient"])
		d.UpdatedMessage = string(v["updatedMessage"].([]byte))
		d.UpdatedMessageHash = MessageHash(d.UpdatedMessage)
		d.DepositorSignature = string(v["depositorSignature"].([]byte))
		out.Updates = []DepositUpdate{d}
	case "FilledRelay", "FilledV3Relay":
		d := Fill{}
		eventAnchor(&d, l, b, m)
		d.OriginChainId, e = to64(v["originChainId"])
		if e != nil {
			return out, true, e
		}
		d.RepaymentChainId, e = to64(v["repaymentChainId"])
		if e != nil {
			return out, true, e
		}
		d.DepositId = number(v["depositId"])
		d.Depositor = word(v["depositor"])
		d.Recipient = word(v["recipient"])
		d.ExclusiveRelayer = word(v["exclusiveRelayer"])
		d.InputToken = word(v["inputToken"])
		d.OutputToken = word(v["outputToken"])
		d.InputAmountRaw = number(v["inputAmount"])
		d.OutputAmountRaw = number(v["outputAmount"])
		d.FillDeadline = v["fillDeadline"].(uint32)
		d.ExclusivityDeadline = v["exclusivityDeadline"].(uint32)
		d.RepaymentAddress = word(v["relayer"])
		info := reflect.ValueOf(v["relayExecutionInfo"])
		d.UpdatedRecipient = word(info.FieldByName("UpdatedRecipient").Interface())
		d.UpdatedOutputAmountRaw = number(info.FieldByName("UpdatedOutputAmount").Interface())
		d.FillType = info.FieldByName("FillType").Interface().(uint8)
		if d.FillType > 2 {
			return out, true, errors.New("invalid_fill_type")
		}
		if n == "FilledRelay" {
			d.MessageHash = word(v["messageHash"])
			d.UpdatedMessageHash = word(info.FieldByName("UpdatedMessageHash").Interface())
		} else {
			d.MessageHash = MessageHash(string(v["message"].([]byte)))
			d.UpdatedMessageHash = MessageHash(string(info.FieldByName("UpdatedMessage").Interface().([]byte)))
		}
		out.Fills = []Fill{d}
	case "ExecutedRelayerRefundRoot", "ClaimedRelayerRefund":
		d := Refund{}
		eventAnchor(&d, l, b, m)
		d.Token, e = AddressWord(word(v["l2TokenAddress"]))
		if e != nil {
			return out, true, e
		}
		d.Caller, e = AddressWord(word(v["caller"]))
		if e != nil {
			return out, true, e
		}
		if n == "ExecutedRelayerRefundRoot" {
			chain, e := to64(v["chainId"])
			if e != nil || chain != l.ChainID {
				return out, true, errors.New("refund_chain_mismatch")
			}
			d.EventKind = "leaf_execution"
			root, leaf, deferred := v["rootBundleId"].(uint32), v["leafId"].(uint32), v["deferredRefunds"].(bool)
			d.RootBundleId = &root
			d.LeafId = &leaf
			d.DeferredRefunds = &deferred
			d.AmountToReturnRaw = number(v["amountToReturn"])
			d.RefundAmountsRaw = v["refundAmounts"].([]*big.Int)
			for _, a := range v["refundAddresses"].([]common.Address) {
				d.RefundAddresses = append(d.RefundAddresses, string(a[:]))
			}
			if len(d.RefundAmountsRaw) != len(d.RefundAddresses) {
				return out, true, errors.New("refund_array_length_mismatch")
			}
		} else {
			d.EventKind = "deferred_claim"
			a, e := AddressWord(word(v["refundAddress"]))
			if e != nil {
				return out, true, e
			}
			d.RefundAddresses = []string{a}
			d.RefundAmountsRaw = []*big.Int{number(v["amount"])}
		}
		out.Refunds = []Refund{d}
	}
	return out, true, nil
}
func CallData(name string, args ...any) (string, error) {
	b, e := ABI.Pack(name, args...)
	if e != nil {
		return "", fmt.Errorf("pack_%s: %w", name, e)
	}
	return "0x" + common.Bytes2Hex(b), nil
}
