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
var ErrLogSourceUnsupported = errors.New("log_source_range_capability_rejected")

func classifyLogRPCError(err error, res Response) *RPCError {
	var re *RPCError
	if !errors.As(err, &re) {
		return nil
	}
	// Additional RPC diagnostics must never replace rate-limit/ban semantics.
	if res.HTTPStatus == 200 && err.Error() == re.Error() || res.HTTPStatus == 400 && strings.HasPrefix(err.Error(), "source_http_400\n") {
		return re
	}
	return nil
}

func (c *Collector) logRPC() *RPC {
	if c.LogRPC != nil {
		return c.LogRPC
	}
	return c.RPC
}

func (c *Collector) logSourceIdentity() string {
	// Bind the rejection to the exact endpoint without persisting credentials.
	return Hex(HashBytes([]byte(c.logRPC().URL + "|" + c.LogMode)))
}

func (c *Collector) liveLogBlocks() uint64 {
	if c.LogMode == "receipts" || c.LiveLogsFromBlock != 0 {
		return min(uint64(8), c.Manifest.MaxLogBlocks)
	}
	return c.Manifest.MaxLogBlocks
}

func (c *Collector) logInterval() time.Duration {
	if c.LogMode == "receipts" || c.LiveLogsFromBlock != 0 {
		return time.Minute
	}
	return 5 * time.Minute
}

// Ethereum's header bloom has no false negatives for an address. A positive
// bloom is only a candidate and must be resolved with logs at that exact hash.
func bloomContains(bloom, address string) (bool, error) {
	if len(bloom) != 256 || len(address) != 20 {
		return false, errors.New("logs_bloom_missing_or_invalid")
	}
	h := crypto.Keccak256([]byte(address))
	for i := 0; i < 6; i += 2 {
		bit := (uint16(h[i])<<8 | uint16(h[i+1])) & 2047
		if bloom[255-bit/8]&(1<<(bit%8)) == 0 {
			return false, nil
		}
	}
	return true, nil
}

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
	v, res, e := c.logRPC().View(ctx, c.Manifest.Address("queue"), b, "proxy__getImplementation()", []string{"address"}, "normal")
	if e != nil {
		return res, e
	}
	if v[0].(common.Address) != common.HexToAddress(c.Manifest.Addresses["queue_impl"]) {
		return res, errors.New("historical_abi_unknown")
	}
	return res, nil
}
func (c *Collector) Logs(ctx context.Context, from, to uint64, mode string) (Batch, error) {
	return c.logsWithFinalized(ctx, from, to, mode, nil)
}

// The runner may reuse the finalized response it just obtained for planning.
// It remains archived with its original timestamps; no cached market head is used.
func (c *Collector) logsWithFinalized(ctx context.Context, from, to uint64, mode string, finalized *Block) (Batch, error) {
	return c.logsRange(ctx, from, to, mode, finalized, false)
}
func (c *Collector) logsRange(ctx context.Context, from, to uint64, mode string, finalized *Block, liveBounded bool) (Batch, error) {
	liveBounded = liveBounded && c.LogMode == "receipts" && c.logRPC().URL == c.RPC.URL
	b := Batch{Capture: c.newCapture("logs", mode)}
	b.Capture.Finality = "finalized"
	b.Capture.SourceId = "ethereum:1:lido:withdrawal-queue"
	responses := []Response{}
	stage := "log_range"
	var lastResponse Response
	add := func(res Response) { responses = append(responses, res); lastResponse = res }
	collect := func() error {
		if from > to || to-from >= c.Manifest.MaxLogBlocks {
			return errors.New("log_range_invalid")
		}
		if c.LogMode == "receipts" && to-from >= c.liveLogBlocks() {
			return errors.New("receipt_scan_range_over_eight_blocks")
		}
		stage = "logs_finalized_head"
		var end Block
		var e error
		if finalized != nil {
			end = *finalized
			if end.Number == 0 || !goodHash(end.Hash, 32) || !timeValid(end.Time) || len(end.Response.PayloadHash) != 32 {
				return errors.New("logs_finalized_proof_invalid")
			}
		} else {
			end, e = c.RPC.Header(ctx, "finalized")
		}
		add(end.Response)
		if e != nil {
			return e
		}
		if to > end.Number {
			return errors.New("log_range_not_finalized")
		}
		stage = "logs_range_header"
		first, e := c.RPC.Header(ctx, fmt.Sprintf("0x%x", from))
		add(first.Response)
		if e != nil {
			return e
		}
		if first.Number != from {
			return errors.New("logs_from_header_height_mismatch")
		}
		last := first
		if to != from && !liveBounded {
			last, e = c.RPC.Header(ctx, fmt.Sprintf("0x%x", to))
			add(last.Response)
			if e != nil {
				return e
			}
			if last.Number != to {
				return errors.New("logs_to_header_height_mismatch")
			}
		}
		setAnchor(&b.Capture, first, last)
		// A separate log source must agree with the primary chain anchor. Empty
		// arrays from a wrong chain/provider cannot establish range coverage.
		if c.logRPC().URL != c.RPC.URL {
			stage = "logs_source_anchor"
			h, err := c.logRPC().Header(ctx, fmt.Sprintf("0x%x", to))
			add(h.Response)
			if err != nil {
				return err
			}
			if h.Number != last.Number || h.Hash != last.Hash {
				return errors.New("log_source_anchor_mismatch")
			}
		}
		stage = "logs_queue_identity"
		identityHeaders := []Block{first, last}
		if c.LogMode == "receipts" {
			identityHeaders = nil
		}
		for _, h := range identityHeaders {
			res, e := c.queueIdentityAt(ctx, h)
			add(res)
			if e != nil {
				return e
			}
		}
		headers := map[uint64]Block{from: first}
		if !liveBounded {
			headers[to] = last
		}
		logResponses := map[uint64]Response{}
		var logs []chainLog
		if c.LogMode == "receipts" {
			var verifiedLast Block
			budgetStopped := false
			// Only the live runner accepts a fully verified prefix. Public
			// Logs/backfill retain their explicit whole-range contract.
			room := func(calls int) (bool, error) {
				if !liveBounded {
					return true, nil
				}
				slots, err := c.RPC.Transport.AvailableRPCSlots(c.RPC.URL)
				if err != nil {
					return false, err
				}
				if slots < calls {
					return false, nil
				}
				if deadline, ok := ctx.Deadline(); ok {
					if time.Until(deadline) < time.Duration(calls)*c.RPC.Transport.spacing("rpc", Now())+5*time.Second {
						return false, nil
					}
				}
				return true, nil
			}
			var previousHash string
			for n := from; n <= to; n++ {
				h, ok := headers[n]
				remainingCalls := 2 // canonical end plus one maintenance reservation
				if !ok {
					remainingCalls++
				}
				enough, roomErr := room(remainingCalls)
				if roomErr != nil {
					return roomErr
				}
				if !enough {
					budgetStopped = true
					break
				}
				if !ok {
					stage = "logs_bloom_header"
					h, e = c.RPC.Header(ctx, fmt.Sprintf("0x%x", n))
					add(h.Response)
					if e != nil {
						return e
					}
					headers[n] = h
				}
				stage, lastResponse = "logs_header_identity", h.Response
				if h.Number != n {
					return errors.New("logs_middle_header_height_mismatch")
				}
				if n > from && h.ParentHash != previousHash {
					return errors.New("logs_header_parent_chain_mismatch")
				}
				previousHash = h.Hash
				stage, lastResponse = "logs_bloom", h.Response
				possible, err := bloomContains(h.LogsBloom, c.Manifest.Address("queue"))
				if err != nil {
					return err
				}
				if !possible {
					verifiedLast = h
					continue
				}
				// Receipt + possible ABI check + canonical + maintenance.
				enough, roomErr = room(4)
				if roomErr != nil {
					return roomErr
				}
				if !enough {
					budgetStopped = true
					break
				}
				stage = "logs_block_receipts"
				raw, res, err := c.logRPC().Call(ctx, "eth_getBlockReceipts", []any{Hex(h.Hash)}, "normal")
				add(res)
				if err != nil {
					if re := classifyLogRPCError(err, res); re != nil && re.Code == -32601 {
						return errors.Join(ErrLogSourceUnsupported, err)
					}
					return err
				}
				part, err := queueLogsFromReceipts(raw, h, c.Manifest.Address("queue"))
				if err != nil {
					return err
				}

				// Check ABI only when raw queue-address logs actually exist,
				// before filtering known topics. Empty proof needs no state read.
				if len(part) > 0 {
					stage = "logs_queue_identity"
					identity, identityErr := c.queueIdentityAt(ctx, h)
					add(identity)
					if identityErr != nil {
						return identityErr
					}
				}
				logResponses[n] = res
				logs = append(logs, part...)
				verifiedLast = h
			}
			if budgetStopped {
				if verifiedLast.Number == 0 {
					return errors.New("live_receipt_piece_budget_exhausted_before_first_block")
				}
				b.Capture.Reason = fmt.Sprintf("live_receipt_piece_bounded planned_to=%d covered_to=%d", to, verifiedLast.Number)
				to = verifiedLast.Number
			}
			last = verifiedLast
			setAnchor(&b.Capture, first, last)
		} else {
			stage = "logs_getLogs"
			raw, res, e := c.logRPC().Call(ctx, "eth_getLogs", []any{map[string]any{"address": c.Manifest.Addresses["queue"], "fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}}, "logs")
			add(res)
			if e != nil {
				if re := classifyLogRPCError(e, res); re != nil {
					s := strings.ToLower(re.Message)
					if re.Code == 35 && strings.Contains(s, "ranges over") && strings.Contains(s, "10000") && strings.Contains(s, "free plan") && to-from < 10000 {
						return errors.Join(ErrLogSourceUnsupported, e)
					}
					if strings.Contains(s, "too many results") || strings.Contains(s, "block range") || strings.Contains(s, "response size") || strings.Contains(s, "query returned more") {
						return errors.Join(ErrLogRange, e)
					}
				}
				return e
			}
			stage = "logs_parse"
			if e = exactJSON(raw, &logs); e != nil {
				return e
			}
			if logs == nil {
				return errors.New("logs_array_missing")
			}
			for _, v := range logs {
				n, _ := q64(v.BlockNumber)
				logResponses[n] = res
			}
		}
		if len(logs) >= int(c.Manifest.MaxLogs) {
			return ErrLogRange
		}
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
				stage = "logs_event_header"
				h, e = c.RPC.Header(ctx, fmt.Sprintf("0x%x", n))
				add(h.Response)
				if e != nil {
					return e
				}
				headers[n] = h
			}
			stage = "logs_decode_event"
			lastResponse = logResponses[n]
			if e = decodeEvent(v, h, c.Manifest, &b, logResponses[n]); e != nil {
				return e
			}
		}
		// Finalized data must still match canonical headers. Conflicts stop the range.
		stage = "logs_canonical_end"
		ok, rs, e := c.RPC.Canonical(ctx, last)
		add(rs)
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
		b.Capture.Reason = failureReason(stage, e, lastResponse)
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
