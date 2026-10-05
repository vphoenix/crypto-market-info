package lst

import (
	"encoding/json"
	"errors"

	"github.com/ethereum/go-ethereum/crypto"
)

func bloomAdd(bloom []byte, value string) {
	h := crypto.Keccak256([]byte(value))
	for i := 0; i < 6; i += 2 {
		bit := (uint16(h[i])<<8 | uint16(h[i+1])) & 2047
		bloom[255-bit/8] |= 1 << (bit % 8)
	}
}

// Validate every receipt and every log before filtering the queue. Empty or
// truncated arrays cannot stand in for a block's complete transaction receipts.
func queueLogsFromReceipts(raw json.RawMessage, h Block, queue string) ([]chainLog, error) {
	var receipts []struct {
		TransactionHash, TransactionIndex, BlockHash, BlockNumber, Status string
		Logs                                                              []chainLog
	}
	if err := exactJSON(raw, &receipts); err != nil {
		return nil, err
	}
	if h.Transactions == nil || receipts == nil || len(receipts) != len(h.Transactions) || len(h.LogsBloom) != 256 {
		return nil, errors.New("block_receipts_incomplete")
	}
	allBloom := make([]byte, 256)
	var queueLogs []chainLog
	var nextLog uint64
	seenTx := map[string]bool{}
	for i, r := range receipts {
		ti, err := q64(r.TransactionIndex)
		n, ne := q64(r.BlockNumber)
		status, se := q64(r.Status)
		if err != nil || ne != nil || se != nil || status > 1 || ti != uint64(i) || n != h.Number || r.BlockHash != Hex(h.Hash) || r.TransactionHash != h.Transactions[i] || seenTx[r.TransactionHash] || r.Logs == nil || status == 0 && len(r.Logs) != 0 {
			return nil, errors.New("block_receipt_identity")
		}
		if _, err = ParseHex(r.TransactionHash, 32); err != nil {
			return nil, err
		}
		seenTx[r.TransactionHash] = true
		for _, v := range r.Logs {
			address, addressErr := ParseHex(v.Address, 20)
			vn, en := q64(v.BlockNumber)
			vti, eti := q64(v.TransactionIndex)
			li, eli := q64(v.LogIndex)
			if en != nil || eti != nil || eli != nil || vn != n || vti != ti || li != nextLog || v.Removed || v.BlockHash != r.BlockHash || v.TransactionHash != r.TransactionHash || addressErr != nil || len(v.Topics) > 4 {
				return nil, errors.New("receipt_log_identity_or_continuity")
			}
			if _, err = decodeBytes(v.Data); err != nil {
				return nil, err
			}
			bloomAdd(allBloom, address)
			for _, topic := range v.Topics {
				t, err := ParseHex(topic, 32)
				if err != nil {
					return nil, err
				}
				bloomAdd(allBloom, t)
			}
			nextLog++
			if address == queue {
				queueLogs = append(queueLogs, v)
			}
		}
	}
	if string(allBloom) != h.LogsBloom {
		return nil, errors.New("receipt_logs_header_bloom_mismatch")
	}
	return queueLogs, nil
}
