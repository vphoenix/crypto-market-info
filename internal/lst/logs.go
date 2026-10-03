package lst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var ErrLogRange = errors.New("log_range_too_large")

type chainLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockHash        string   `json:"blockHash"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

func eventTopic(signature string) string {
	return "0x" + fmt.Sprintf("%x", crypto.Keccak256([]byte(signature)))
}

var requestedTopic = eventTopic("WithdrawalRequested(uint256,address,address,uint256,uint256)")
var finalizedTopic = eventTopic("WithdrawalsFinalized(uint256,uint256,uint256,uint256,uint256)")
var claimedTopic = eventTopic("WithdrawalClaimed(uint256,address,address,uint256)")

func topicInt(s string) (*big.Int, error) {
	b, e := ParseHex(s, 32)
	if e != nil {
		return nil, e
	}
	return new(big.Int).SetBytes([]byte(b)), nil
}
func topicAddress(s string) (string, error) {
	b, e := ParseHex(s, 32)
	if e != nil {
		return "", e
	}
	if b[:12] != strings.Repeat("\x00", 12) {
		return "", errors.New("indexed_address_padding")
	}
	return b[12:], nil
}
func logWords(data string, count int) ([]*big.Int, error) {
	b, e := decodeBytes(data)
	if e != nil || len(b) != 32*count {
		return nil, errors.New("event_data_width")
	}
	out := []*big.Int{}
	for i := 0; i < count; i++ {
		out = append(out, new(big.Int).SetBytes(b[i*32:(i+1)*32]))
	}
	return out, nil
}
func logIdentity(v chainLog, b Block, m Manifest) (tx string, txindex, index uint32, err error) {
	n, e := q64(v.BlockNumber)
	if e != nil || n != b.Number || v.BlockHash != Hex(b.Hash) || common.HexToAddress(v.Address) != common.HexToAddress(m.Addresses["queue"]) || !common.IsHexAddress(v.Address) || v.Removed {
		return "", 0, 0, errors.New("event_anchor")
	}
	tx, e = ParseHex(v.TransactionHash, 32)
	if e != nil {
		return "", 0, 0, e
	}
	ti, e := q64(v.TransactionIndex)
	if e != nil || ti > 1<<32-1 {
		return "", 0, 0, errors.New("tx_index")
	}
	li, e := q64(v.LogIndex)
	if e != nil || li > 1<<32-1 {
		return "", 0, 0, errors.New("log_index")
	}
	return tx, uint32(ti), uint32(li), nil
}
func decodeEvent(v chainLog, b Block, m Manifest, batch *Batch, res Response) error {
	tx, ti, li, e := logIdentity(v, b, m)
	if e != nil {
		return e
	}
	if len(v.Topics) == 0 {
		return nil
	}
	switch v.Topics[0] {
	case requestedTopic:
		if len(v.Topics) != 4 {
			return errors.New("requested_topics")
		}
		id, e := topicInt(v.Topics[1])
		if e != nil {
			return e
		}
		sender, e := topicAddress(v.Topics[2])
		if e != nil {
			return e
		}
		owner, e := topicAddress(v.Topics[3])
		if e != nil {
			return e
		}
		w, e := logWords(v.Data, 2)
		if e != nil {
			return e
		}
		if id.Sign() == 0 || w[0].Sign() == 0 || w[1].Sign() == 0 {
			return errors.New("request_amount")
		}
		batch.Requests = append(batch.Requests, WithdrawalRequest{CaptureId: batch.Capture.CaptureId, ChainId: 1, QueueAddress: m.Address("queue"), BlockNumber: b.Number, BlockHash: b.Hash, BlockTime: b.Time, TransactionHash: tx, TransactionIndex: ti, LogIndex: li, AbiVersion: m.AbiVersion, RequestId: id, Sender: sender, InitialOwner: owner, AmountStethWei: w[0], AmountSharesRaw: w[1], EventPayloadHash: res.PayloadHash, AvailableAt: res.AvailableAt, GasSampleClass: "missing"})
	case finalizedTopic:
		if len(v.Topics) != 3 {
			return errors.New("finalization_topics")
		}
		from, e := topicInt(v.Topics[1])
		if e != nil {
			return e
		}
		to, e := topicInt(v.Topics[2])
		if e != nil {
			return e
		}
		w, e := logWords(v.Data, 3)
		if e != nil {
			return e
		}
		if from.Sign() <= 0 || from.Cmp(to) > 0 || !w[2].IsInt64() {
			return errors.New("finalization_range_or_time")
		}
		at := time.Unix(w[2].Int64(), 0).UTC()
		if !at.Equal(b.Time) {
			return errors.New("finalization_header_timestamp_mismatch")
		}
		batch.Finalizations = append(batch.Finalizations, WithdrawalFinalization{CaptureId: batch.Capture.CaptureId, ChainId: 1, QueueAddress: m.Address("queue"), BlockNumber: b.Number, BlockHash: b.Hash, BlockTime: b.Time, TransactionHash: tx, TransactionIndex: ti, LogIndex: li, AbiVersion: m.AbiVersion, FromRequestId: from, ToRequestId: to, EthLockedWei: w[0], SharesToBurnRaw: w[1], EventTimestamp: at, EventPayloadHash: res.PayloadHash, AvailableAt: res.AvailableAt})
	case claimedTopic:
		if len(v.Topics) != 4 {
			return errors.New("claim_topics")
		}
		id, e := topicInt(v.Topics[1])
		if e != nil {
			return e
		}
		owner, e := topicAddress(v.Topics[2])
		if e != nil {
			return e
		}
		receiver, e := topicAddress(v.Topics[3])
		if e != nil {
			return e
		}
		w, e := logWords(v.Data, 1)
		if e != nil {
			return e
		}
		batch.Claims = append(batch.Claims, WithdrawalClaim{CaptureId: batch.Capture.CaptureId, ChainId: 1, QueueAddress: m.Address("queue"), BlockNumber: b.Number, BlockHash: b.Hash, BlockTime: b.Time, TransactionHash: tx, TransactionIndex: ti, LogIndex: li, AbiVersion: m.AbiVersion, RequestId: id, Owner: owner, Receiver: receiver, AmountEthWei: w[0], EventPayloadHash: res.PayloadHash, AvailableAt: res.AvailableAt, GasSampleClass: "missing"})
	}
	return nil
}
func (c *Collector) queueIdentityAt(ctx context.Context, b Block) (Response, error) {
	v, res, e := c.RPC.View(ctx, c.Manifest.Address("queue"), b, "proxy__getImplementation()", []string{"address"}, "normal")
	if e != nil {
		return res, e
	}
	if v[0].(common.Address) != common.HexToAddress(c.Manifest.Addresses["queue_impl"]) {
		return res, errors.New("historical_abi_unknown")
	}
	return res, nil
}
func (c *Collector) Logs(ctx context.Context, from, to uint64, mode string) (Batch, error) {
	b := Batch{Capture: c.newCapture("logs", mode)}
	b.Capture.Finality = "finalized"
	b.Capture.SourceId = "ethereum:1:lido:withdrawal-queue"
	responses := []Response{}
	collect := func() error {
		if from > to || to-from >= c.Manifest.MaxLogBlocks {
			return errors.New("log_range_invalid")
		}
		end, e := c.RPC.Header(ctx, "finalized")
		responses = append(responses, end.Response)
		if e != nil {
			return e
		}
		if to > end.Number {
			return errors.New("log_range_not_finalized")
		}
		first, e := c.RPC.Header(ctx, fmt.Sprintf("0x%x", from))
		responses = append(responses, first.Response)
		if e != nil {
			return e
		}
		last := first
		if to != from {
			last, e = c.RPC.Header(ctx, fmt.Sprintf("0x%x", to))
			responses = append(responses, last.Response)
			if e != nil {
				return e
			}
		}
		setAnchor(&b.Capture, first, last)
		for _, h := range []Block{first, last} {
			res, e := c.queueIdentityAt(ctx, h)
			responses = append(responses, res)
			if e != nil {
				return e
			}
		}
		raw, res, e := c.RPC.Call(ctx, "eth_getLogs", []any{map[string]any{"address": c.Manifest.Addresses["queue"], "fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}}, "logs")
		responses = append(responses, res)
		if e != nil {
			var re *RPCError
			if errors.As(e, &re) {
				s := strings.ToLower(re.Message)
				if strings.Contains(s, "too many results") || strings.Contains(s, "block range") || strings.Contains(s, "response size") || strings.Contains(s, "query returned more") {
					return ErrLogRange
				}
			}
			return e
		}
		var logs []chainLog
		if e = exactJSON(raw, &logs); e != nil {
			return e
		}
		if len(logs) >= int(c.Manifest.MaxLogs) {
			return ErrLogRange
		}
		headers := map[uint64]Block{from: first, to: last}
		seen := map[string]bool{}
		sort.Slice(logs, func(i, j int) bool {
			a, _ := q64(logs[i].BlockNumber)
			z, _ := q64(logs[j].BlockNumber)
			if a != z {
				return a < z
			}
			a, _ = q64(logs[i].LogIndex)
			z, _ = q64(logs[j].LogIndex)
			return a < z
		})
		for _, v := range logs {
			n, e := q64(v.BlockNumber)
			if e != nil || n < from || n > to {
				return errors.New("log_outside_range")
			}
			if len(v.Topics) > 0 && (v.Topics[0] == eventTopic("Upgraded(address)") || v.Topics[0] == eventTopic("BeaconUpgraded(address)")) {
				return errors.New("historical_abi_transition_unknown")
			}
			key := v.BlockHash + ":" + v.LogIndex
			if seen[key] {
				return errors.New("duplicate_source_log")
			}
			seen[key] = true
			h, ok := headers[n]
			if !ok {
				h, e = c.RPC.Header(ctx, fmt.Sprintf("0x%x", n))
				responses = append(responses, h.Response)
				if e != nil {
					return e
				}
				headers[n] = h
			}
			if e = decodeEvent(v, h, c.Manifest, &b, res); e != nil {
				return e
			}
		}
		// Finalized data must still match canonical headers. Conflicts stop the range.
		ok, rs, e := c.RPC.Canonical(ctx, last)
		responses = append(responses, rs)
		if e != nil {
			return e
		}
		if !ok {
			return errors.New("finalized_hash_conflict")
		}
		b.Capture.Canonical = true
		return nil
	}
	e := collect()
	if e != nil {
		b.Capture.Status = "failed"
		b.Capture.Reason = e.Error()
		b.Requests = nil
		b.Finalizations = nil
		b.Claims = nil
	} else {
		b.Capture.Status = "complete"
	}
	if se := c.seal(&b, responses); se != nil {
		return b, se
	}
	return b, e
}

func (c *Collector) BlockAtTime(ctx context.Context, target time.Time, end Block) (uint64, error) {
	lo, hi := uint64(0), end.Number
	for lo < hi {
		mid := lo + (hi-lo)/2
		b, e := c.RPC.Header(ctx, fmt.Sprintf("0x%x", mid))
		if e != nil {
			return 0, e
		}
		if b.Time.Before(target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}

// EnrichGas limits selection to known transaction hashes supplied by the runner.
// Receipt failures leave the immutable events intact and never block log coverage.
func (c *Collector) EnrichGas(ctx context.Context, b *Batch, hashes []string) ([]Response, error) {
	responses := []Response{}
	for _, hash := range hashes {
		raw, res, e := c.RPC.Call(ctx, "eth_getTransactionReceipt", []any{Hex(hash)}, "normal")
		responses = append(responses, res)
		if e != nil {
			continue
		}
		var receipt struct {
			TransactionHash, BlockHash, BlockNumber, From, To, Status, GasUsed, EffectiveGasPrice string
			Logs                                                                                  []chainLog
		}
		if json.Unmarshal(raw, &receipt) != nil || receipt.TransactionHash != Hex(hash) {
			continue
		}
		blockHash, e := ParseHex(receipt.BlockHash, 32)
		if e != nil {
			continue
		}
		status, e := q64(receipt.Status)
		if e != nil || status != 1 {
			continue
		}
		gas, e := q64(receipt.GasUsed)
		if e != nil {
			continue
		}
		price, e := quantity(receipt.EffectiveGasPrice)
		if e != nil {
			continue
		}
		sender, e := ParseHex(receipt.From, 20)
		if e != nil {
			continue
		}
		to, e := ParseHex(receipt.To, 20)
		if e != nil {
			continue
		}
		txraw, tr, e := c.RPC.Call(ctx, "eth_getTransactionByHash", []any{Hex(hash)}, "normal")
		responses = append(responses, tr)
		if e != nil {
			continue
		}
		var tx struct{ Hash, BlockHash, To, Input string }
		if json.Unmarshal(txraw, &tx) != nil || tx.Hash != Hex(hash) || tx.BlockHash != receipt.BlockHash || tx.To != receipt.To {
			continue
		}
		input, e := decodeBytes(tx.Input)
		if e != nil || len(input) < 4 {
			continue
		}
		selector := string(input[:4])
		count := uint32(0)
		allSame := true
		var seenTopic string
		for _, l := range receipt.Logs {
			if common.HexToAddress(l.Address) != common.HexToAddress(c.Manifest.Addresses["queue"]) || len(l.Topics) == 0 {
				continue
			}
			if l.Topics[0] == requestedTopic || l.Topics[0] == claimedTopic {
				count++
				if seenTopic != "" && seenTopic != l.Topics[0] {
					allSame = false
				}
				seenTopic = l.Topics[0]
			}
		}
		class := "mixed"
		known := map[string]string{}
		for _, sig := range []string{"requestWithdrawals(uint256[],address)", "requestWithdrawalsWstETH(uint256[],address)"} {
			known[string(crypto.Keccak256([]byte(sig))[:4])] = requestedTopic
		}
		for _, sig := range []string{"claimWithdrawal(uint256)", "claimWithdrawals(uint256[],uint256[])", "claimWithdrawalsTo(uint256[],uint256[],address)"} {
			known[string(crypto.Keccak256([]byte(sig))[:4])] = claimedTopic
		}
		if to == c.Manifest.Address("queue") && allSame && count > 0 && known[selector] == seenTopic {
			class = "direct_single"
			if count > 1 {
				class = "direct_batch"
			}
		}
		for i := range b.Requests {
			r := &b.Requests[i]
			if r.TransactionHash == hash && r.BlockHash == blockHash {
				r.ReceiptStatus = Ptr(uint8(status))
				r.TransactionSender = &sender
				r.TransactionTo = &to
				r.InputSelector = &selector
				r.TransactionOperationCount = &count
				r.GasSampleClass = class
				r.GasUsed = &gas
				r.EffectiveGasPriceWei = price
				r.ReceiptPayloadHash = &res.PayloadHash
				r.ReceiptAvailableAt = &res.AvailableAt
			}
		}
		for i := range b.Claims {
			r := &b.Claims[i]
			if r.TransactionHash == hash && r.BlockHash == blockHash {
				r.ReceiptStatus = Ptr(uint8(status))
				r.TransactionSender = &sender
				r.TransactionTo = &to
				r.InputSelector = &selector
				r.TransactionOperationCount = &count
				r.GasSampleClass = class
				r.GasUsed = &gas
				r.EffectiveGasPriceWei = price
				r.ReceiptPayloadHash = &res.PayloadHash
				r.ReceiptAvailableAt = &res.AvailableAt
			}
		}
	}
	return responses, nil
}
