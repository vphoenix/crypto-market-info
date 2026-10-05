package across

import (
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"sort"
	"strings"
)

type receiptTransfer struct {
	from, to string
	amount   *big.Int
	index    uint32
}

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

func parseReceiptTransferResult(raw json.RawMessage, r TxReceipt, token string) ([]receiptTransfer, error) {
	var v struct {
		TransactionHash, BlockHash, Status string
		Logs                               []rpcLog
	}
	if len(token) != 20 || json.Unmarshal(raw, &v) != nil || v.Logs == nil || !strings.EqualFold(v.TransactionHash, Hex(r.TxHash)) || !strings.EqualFold(v.BlockHash, Hex(r.BlockHash)) {
		return nil, errors.New("invalid_transfer_receipt")
	}
	status, err := q64(v.Status)
	if err != nil || status > 1 || (status == 1) != r.Success || !r.Success && len(v.Logs) != 0 {
		return nil, errors.New("invalid_transfer_receipt_status")
	}
	out := []receiptTransfer{}
	seen := map[uint32]bool{}
	for _, rawLog := range v.Logs {
		l, err := parseLog(rawLog, r.ChainId, r.ReceiptPayloadHash)
		if err != nil {
			return nil, err
		}
		if l.Removed || l.BlockHash != r.BlockHash || l.BlockNumber != r.BlockNumber || l.TxHash != r.TxHash || l.TxIndex != r.TxIndex || seen[l.LogIndex] {
			return nil, errors.New("invalid_transfer_anchor")
		}
		seen[l.LogIndex] = true
		if l.Address != token || len(l.Topics) == 0 || Hex(l.Topics[0]) != transferTopic {
			continue
		}
		if len(l.Topics) != 3 || len(l.Data) != 32 || l.Topics[1][:12] != strings.Repeat("\x00", 12) || l.Topics[2][:12] != strings.Repeat("\x00", 12) {
			return nil, errors.New("invalid_erc20_transfer")
		}
		out = append(out, receiptTransfer{l.Topics[1][12:], l.Topics[2][12:], new(big.Int).SetBytes([]byte(l.Data)), l.LogIndex})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].index < out[j].index })
	return out, nil
}

func receiptTransferRow(r TxReceipt, token string, transfers []receiptTransfer) ReceiptTransfers {
	v := ReceiptTransfers{CaptureId: r.CaptureId, ChainId: r.ChainId, BlockNumber: r.BlockNumber, BlockHash: r.BlockHash, BlockTime: r.BlockTime, TxHash: r.TxHash, Token: token, ReceiptSuccess: r.Success, AvailableAt: Now(), PayloadHash: r.ReceiptPayloadHash,
		LogIndices: []uint32{}, Senders: []string{}, Recipients: []string{}, AmountsRaw: []*big.Int{}}
	sort.Slice(transfers, func(i, j int) bool { return transfers[i].index < transfers[j].index })
	for _, t := range transfers {
		v.LogIndices = append(v.LogIndices, t.index)
		v.Senders = append(v.Senders, t.from)
		v.Recipients = append(v.Recipients, t.to)
		v.AmountsRaw = append(v.AmountsRaw, new(big.Int).Set(t.amount))
	}
	return v
}

func ValidateReceiptTransfers(v ReceiptTransfers) error {
	if err := validateRow(reflect.ValueOf(v)); err != nil {
		return err
	}
	n := len(v.LogIndices)
	if v.ChainId == 0 || v.BlockTime.IsZero() || v.AvailableAt.IsZero() || len(v.Senders) != n || len(v.Recipients) != n || len(v.AmountsRaw) != n || !v.ReceiptSuccess && n != 0 {
		return errors.New("invalid_receipt_transfer_arrays")
	}
	for i := range v.LogIndices {
		if i > 0 && v.LogIndices[i-1] >= v.LogIndices[i] {
			return errors.New("unordered_receipt_transfers")
		}
	}
	return nil
}

func storedReceiptTransfers(v ReceiptTransfers, r TxReceipt, token string) ([]receiptTransfer, error) {
	if err := ValidateReceiptTransfers(v); err != nil {
		return nil, err
	}
	if v.ChainId != r.ChainId || v.BlockNumber != r.BlockNumber || v.BlockHash != r.BlockHash || v.TxHash != r.TxHash || !v.BlockTime.Equal(r.BlockTime) || v.Token != token || v.ReceiptSuccess != r.Success {
		return nil, errors.New("stored_receipt_transfer_anchor_mismatch")
	}
	out := make([]receiptTransfer, len(v.LogIndices))
	for i := range out {
		out[i] = receiptTransfer{v.Senders[i], v.Recipients[i], v.AmountsRaw[i], v.LogIndices[i]}
	}
	return out, nil
}
