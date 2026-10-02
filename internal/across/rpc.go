package across

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Block struct {
	ChainID, Number  uint64
	Hash, ParentHash string
	Time             time.Time
	PayloadHash      string
	AvailableAt      time.Time
}
type Reader struct {
	RPC     *ethereum.Client
	Chain   ChainConfig
	Members []string
	Used    int
	MaxLogs uint32
	// RPCMinInterval is configured before use. Zero preserves normal batching.
	// Positive values serialize members with a cooldown after each response;
	// this conservatively bounds actual request starts even under slow transport.
	RPCMinInterval time.Duration
	memberMu       sync.Mutex
	rateInit       sync.Once
	rateGate       chan struct{}
	nextRequestAt  time.Time
}

func NewReader(rpc *ethereum.Client, c ChainConfig) *Reader {
	return &Reader{RPC: rpc, Chain: c, MaxLogs: 10000}
}
func (r *Reader) batch(ctx context.Context, calls []ethereum.Call) []ethereum.Result {
	var v []ethereum.Result
	used := len(calls)
	if r.RPCMinInterval > 0 {
		v, used = r.throttledBatch(ctx, calls)
	} else {
		// Base's public RPC rejects more than ten members (-32014). Group before
		// calling the shared client; its semaphore still caps all requests at two.
		if r.Chain.ChainID == 8453 && len(calls) > 10 {
			v = make([]ethereum.Result, len(calls))
			var wg sync.WaitGroup
			for start := 0; start < len(calls); start += 10 {
				end := min(start+10, len(calls))
				wg.Add(1)
				go func(start, end int) { defer wg.Done(); copy(v[start:end], r.RPC.Batch(ctx, calls[start:end])) }(start, end)
			}
			wg.Wait()
		} else {
			v = r.RPC.Batch(ctx, calls)
		}
	}
	r.memberMu.Lock()
	r.Used += used
	for _, x := range v {
		if x.Payload != (dex.Hash{}) {
			r.Members = append(r.Members, string(x.Payload[:]))
		}
	}
	r.memberMu.Unlock()
	return v
}

// The gate is a context-aware mutex. Keeping it through the response makes
// throttled requests strictly serial and lets canceled queued callers return
// immediately without waiting for an active request or consuming its cooldown.
func (r *Reader) throttledOne(ctx context.Context, call ethereum.Call) (ethereum.Result, bool) {
	r.rateInit.Do(func() { r.rateGate = make(chan struct{}, 1) })
	failure := func(e error) (ethereum.Result, bool) { return ethereum.Result{Err: e, At: Now()}, false }
	select {
	case r.rateGate <- struct{}{}:
	case <-ctx.Done():
		return failure(ctx.Err())
	}
	defer func() { <-r.rateGate }()
	if wait := time.Until(r.nextRequestAt); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return failure(ctx.Err())
		}
	}
	if e := ctx.Err(); e != nil {
		return failure(e)
	}
	v := r.RPC.One(ctx, call.Method, call.Params)
	r.nextRequestAt = time.Now().Add(r.RPCMinInterval)
	if v.Err == nil && bytes.Equal(bytes.TrimSpace(v.Raw), []byte("null")) {
		v.Err = errors.New("rpc_null_result")
	}
	return v, true
}
func (r *Reader) throttledBatch(ctx context.Context, calls []ethereum.Call) ([]ethereum.Result, int) {
	out := make([]ethereum.Result, len(calls))
	used := 0
	for i, call := range calls {
		v, sent := r.throttledOne(ctx, call)
		out[i] = v
		if sent {
			used++
		}
		if v.Err != nil {
			for j := i + 1; j < len(out); j++ {
				out[j] = ethereum.Result{Err: errors.New("rpc_batch_aborted_after_failure"), At: Now()}
			}
			break
		}
	}
	return out, used
}
func (r *Reader) headerBudget(count int) time.Duration {
	if r.RPCMinInterval <= 0 {
		return 5 * time.Second
	}
	// Saturate rather than wrapping an explicitly supplied very long interval.
	const maximum = time.Duration(1<<63 - 1)
	if r.RPCMinInterval > maximum-5*time.Second {
		return maximum
	}
	per := r.RPCMinInterval + 5*time.Second
	if count > 0 && per > maximum/time.Duration(count) {
		return maximum
	}
	return per * time.Duration(count)
}
func (r *Reader) one(ctx context.Context, method string, params any) ethereum.Result {
	return r.batch(ctx, []ethereum.Call{{Method: method, Params: params}})[0]
}
func ref(hash string) map[string]any {
	return map[string]any{"blockHash": Hex(hash), "requireCanonical": true}
}
func Quantity(s string) (*big.Int, error) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 || len(s) > 3 && s[2] == '0' {
		return nil, errors.New("invalid_rpc_quantity")
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return nil, errors.New("invalid_rpc_quantity")
		}
	}
	n, ok := new(big.Int).SetString(s[2:], 16)
	if !ok || n.Sign() < 0 || n.BitLen() > 256 {
		return nil, errors.New("invalid_rpc_quantity")
	}
	return n, nil
}
func q64(s string) (uint64, error) {
	n, e := Quantity(s)
	if e != nil {
		return 0, e
	}
	if !n.IsUint64() {
		return 0, errors.New("rpc_quantity_overflow")
	}
	return n.Uint64(), nil
}
func resultString(v ethereum.Result) (string, error) {
	if v.Err != nil {
		return "", v.Err
	}
	var s string
	if json.Unmarshal(v.Raw, &s) != nil {
		return "", errors.New("invalid_rpc_string")
	}
	return s, nil
}
func resultBytes(v ethereum.Result) (string, error) {
	s, e := resultString(v)
	if e != nil {
		return "", e
	}
	if !strings.HasPrefix(s, "0x") || len(s)%2 != 0 {
		return "", errors.New("invalid_rpc_bytes")
	}
	return ParseHex(s, (len(s)-2)/2)
}
func (r *Reader) Header(ctx context.Context, tag string) (Block, error) {
	v := r.one(ctx, "eth_getBlockByNumber", []any{tag, false})
	return r.parseHeader(tag, v)
}

// Headers preserves caller order while the shared transport groups at most 20
// members per request (ten on Base) and permits two requests in flight per chain. The entire
// batch has a five-second budget by default. Explicit throttling budgets each
// member's cooldown and five-second request timeout; an earlier caller deadline
// still wins. A failed member rejects the complete result; raw evidence remains.
func (r *Reader) Headers(ctx context.Context, numbers []uint64) ([]Block, error) {
	if len(numbers) > 512 {
		return nil, errors.New("header_batch_budget")
	}
	if len(numbers) == 0 {
		return []Block{}, nil
	}
	budget, cancel := context.WithTimeout(ctx, r.headerBudget(len(numbers)))
	defer cancel()
	calls := make([]ethereum.Call, len(numbers))
	tags := make([]string, len(numbers))
	for i, n := range numbers {
		tags[i] = fmt.Sprintf("0x%x", n)
		calls[i] = ethereum.Call{Method: "eth_getBlockByNumber", Params: []any{tags[i], false}}
	}
	results := r.batch(budget, calls)
	out := make([]Block, len(numbers))
	for i, v := range results {
		b, e := r.parseHeader(tags[i], v)
		if e != nil {
			return nil, e
		}
		out[i] = b
	}
	return out, nil
}
func (r *Reader) parseHeader(tag string, v ethereum.Result) (Block, error) {
	b := Block{ChainID: r.Chain.ChainID, PayloadHash: string(v.Payload[:]), AvailableAt: v.At}
	if v.Err != nil {
		return b, v.Err
	}
	var raw struct{ Number, Hash, ParentHash, Timestamp string }
	if json.Unmarshal(v.Raw, &raw) != nil || string(v.Raw) == "null" {
		return b, errors.New("missing_header")
	}
	var e error
	b.Number, e = q64(raw.Number)
	if e != nil {
		return b, e
	}
	b.Hash, e = ParseHex(raw.Hash, 32)
	if e != nil {
		return b, e
	}
	b.ParentHash, e = ParseHex(raw.ParentHash, 32)
	if e != nil {
		return b, e
	}
	t, e := q64(raw.Timestamp)
	if e != nil || t > uint64(1<<63-1) {
		return b, errors.New("invalid_header_time")
	}
	b.Time = time.Unix(int64(t), 0).UTC()
	if strings.HasPrefix(tag, "0x") {
		want, e := q64(tag)
		if e != nil || want != b.Number {
			return b, errors.New("header_number_mismatch")
		}
	}
	return b, nil
}
func (r *Reader) canonical(ctx context.Context, b Block) error {
	h, e := r.Header(ctx, fmt.Sprintf("0x%x", b.Number))
	if e != nil {
		return e
	}
	if h.Hash != b.Hash {
		return errors.New("noncanonical_block")
	}
	return nil
}
func (r *Reader) call(ctx context.Context, b Block, to, name string, args ...any) ([]any, error) {
	data, e := CallData(name, args...)
	if e != nil {
		return nil, e
	}
	v := r.one(ctx, "eth_call", []any{map[string]string{"to": Hex(to), "data": data}, ref(b.Hash)})
	return decodeCall(v, name)
}
func decodeCall(v ethereum.Result, name string) ([]any, error) {
	s, e := resultBytes(v)
	if e != nil {
		return nil, e
	}
	a := ABI.Methods[name].Outputs
	out, e := a.Unpack([]byte(s))
	if e != nil {
		return nil, e
	}
	back, e := a.Pack(out...)
	if e != nil || !bytes.Equal(back, []byte(s)) {
		return nil, errors.New("noncanonical_call_result")
	}
	return out, nil
}
func (r *Reader) VerifyImplementation(ctx context.Context, b Block) (string, error) {
	if b.ChainID != r.Chain.ChainID {
		return "", errors.New("wrong_chain_anchor")
	}
	slot := r.one(ctx, "eth_getStorageAt", []any{Hex(r.Chain.SpokePool), ImplementationSlot, ref(b.Hash)})
	raw, e := resultBytes(slot)
	if e != nil {
		return "", e
	}
	addr, e := AddressWord(raw)
	if e != nil {
		return "", e
	}
	var match *Implementation
	for _, v := range r.Chain.Implementations {
		if v.Address == addr {
			x := v
			match = &x
			break
		}
	}
	if match == nil {
		return "", errors.New("unknown_implementation")
	}
	code := r.one(ctx, "eth_getCode", []any{Hex(addr), ref(b.Hash)})
	s, e := resultBytes(code)
	if e != nil {
		return "", e
	}
	if len(s) == 0 || string(crypto.Keccak256([]byte(s))) != match.CodeHash {
		return "", errors.New("implementation_code_hash_mismatch")
	}
	return match.ABIRevision, nil
}
func (r *Reader) Preflight(ctx context.Context, b Block) error {
	if e := r.PreflightIdentity(ctx, b); e != nil {
		return e
	}
	if _, e := r.VerifyImplementation(ctx, b); e != nil {
		return e
	}
	return r.canonical(ctx, b)
}
func (r *Reader) PreflightIdentity(ctx context.Context, b Block) error {
	v := r.one(ctx, "eth_chainId", []any{})
	s, e := resultString(v)
	if e != nil {
		return e
	}
	chain, e := q64(s)
	if e != nil || chain != r.Chain.ChainID {
		return errors.New("wrong_rpc_chain")
	}
	v1, e := r.call(ctx, b, r.Chain.SpokePool, "chainId")
	if e != nil {
		return e
	}
	if v1[0].(*big.Int).Cmp(new(big.Int).SetUint64(chain)) != 0 {
		return errors.New("spoke_chain_mismatch")
	}
	v2, e := r.call(ctx, b, r.Chain.USDC, "decimals")
	if e != nil {
		return e
	}
	if v2[0].(uint8) != 6 {
		return errors.New("usdc_decimals_mismatch")
	}
	v3, e := r.call(ctx, b, r.Chain.USDC, "symbol")
	if e != nil {
		return e
	}
	if v3[0].(string) != "USDC" {
		return errors.New("usdc_symbol_mismatch")
	}
	return r.canonical(ctx, b)
}

type rpcLog struct {
	Address, BlockNumber, BlockHash, TransactionHash, TransactionIndex, LogIndex, Data string
	Topics                                                                             []string
	Removed                                                                            *bool
}

func parseLog(v rpcLog, chain uint64, payload string) (Log, error) {
	l := Log{ChainID: chain, PayloadHash: payload}
	var e error
	l.Address, e = ParseHex(v.Address, 20)
	if e != nil {
		return l, e
	}
	l.BlockNumber, e = q64(v.BlockNumber)
	if e != nil {
		return l, e
	}
	l.BlockHash, e = ParseHex(v.BlockHash, 32)
	if e != nil {
		return l, e
	}
	l.TxHash, e = ParseHex(v.TransactionHash, 32)
	if e != nil {
		return l, e
	}
	ti, e := q64(v.TransactionIndex)
	if e != nil || ti > 1<<32-1 {
		return l, errors.New("invalid_tx_index")
	}
	li, e := q64(v.LogIndex)
	if e != nil || li > 1<<32-1 {
		return l, errors.New("invalid_log_index")
	}
	l.TxIndex = uint32(ti)
	l.LogIndex = uint32(li)
	if !strings.HasPrefix(v.Data, "0x") || len(v.Data)%2 != 0 {
		return l, errors.New("invalid_log_data")
	}
	l.Data, e = ParseHex(v.Data, (len(v.Data)-2)/2)
	if e != nil {
		return l, e
	}
	if len(v.Topics) > 4 {
		return l, errors.New("too_many_log_topics")
	}
	for _, t := range v.Topics {
		s, e := ParseHex(t, 32)
		if e != nil {
			return l, e
		}
		l.Topics = append(l.Topics, s)
	}
	if v.Removed == nil {
		return l, errors.New("missing_removed_flag")
	}
	l.Removed = *v.Removed
	return l, nil
}
func (r *Reader) Logs(ctx context.Context, from, to uint64) ([]Log, error) {
	if to < from || to-from >= 512 {
		return nil, errors.New("log_range_budget")
	}
	v := r.one(ctx, "eth_getLogs", []any{map[string]string{"address": Hex(r.Chain.SpokePool), "fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}})
	if v.Err != nil {
		return nil, v.Err
	}
	var raw []rpcLog
	if json.Unmarshal(v.Raw, &raw) != nil || raw == nil {
		return nil, errors.New("invalid_logs_response")
	}
	if uint32(len(raw)) >= r.MaxLogs {
		return nil, errors.New("log_result_limit")
	}
	out := make([]Log, 0, len(raw))
	seen := map[string]bool{}
	for _, x := range raw {
		l, e := parseLog(x, r.Chain.ChainID, string(v.Payload[:]))
		if e != nil {
			return nil, e
		}
		if l.Removed || l.BlockNumber < from || l.BlockNumber > to || l.Address != r.Chain.SpokePool {
			return nil, errors.New("log_outside_request")
		}
		key := l.BlockHash + l.TxHash + strconv.FormatUint(uint64(l.LogIndex), 10)
		if seen[key] {
			return nil, errors.New("duplicate_rpc_log")
		}
		seen[key] = true
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockNumber != out[j].BlockNumber {
			return out[i].BlockNumber < out[j].BlockNumber
		}
		return out[i].LogIndex < out[j].LogIndex
	})
	return out, nil
}
func (r *Reader) proof() (string, error) {
	h, e := r.RPC.Archive.PutObject(struct {
		Members []string `json:"members_hex"`
	}{func() []string {
		r.memberMu.Lock()
		defer r.memberMu.Unlock()
		o := make([]string, len(r.Members))
		for i, s := range r.Members {
			o[i] = Hex(s)
		}
		return o
	}()})
	return string(h[:]), e
}
func (r *Reader) Probe(ctx context.Context, d Deposit, b Block) (p OrderProbe, err error) {
	if d.DepositId == nil || len(d.RelayHash) != 32 {
		return p, errors.New("invalid_probe_order")
	}
	p = OrderProbe{ChainId: r.Chain.ChainID, OriginChainId: d.ChainId, OriginSpokePool: d.SpokePool, OriginBlockHash: d.BlockHash, DepositId: new(big.Int).Set(d.DepositId), RelayHash: d.RelayHash, DestinationSpokePool: r.Chain.SpokePool, BlockNumber: &b.Number, BlockHash: &b.Hash, BlockTime: &b.Time, RequestedAt: Now(), ProbeStatus: "error"}
	defer func() {
		p.AvailableAt = Now()
		h, e := r.proof()
		p.PayloadHash = h
		if e != nil && err == nil {
			err = e
		}
		if err != nil {
			p.Reason = err.Error()
			p.ProbeStatus = "error"
		}
	}()
	if d.DestinationChainId != r.Chain.ChainID || b.ChainID != r.Chain.ChainID {
		return p, errors.New("probe_destination_mismatch")
	}
	if _, err = r.VerifyImplementation(ctx, b); err != nil {
		return p, err
	}
	calls := []ethereum.Call{}
	for _, name := range []string{"fillStatuses", "pausedFills", "getCurrentTime"} {
		args := []any{}
		if name == "fillStatuses" {
			args = []any{hashWord(d.RelayHash)}
		}
		data, e := CallData(name, args...)
		if e != nil {
			return p, e
		}
		calls = append(calls, ethereum.Call{Method: "eth_call", Params: []any{map[string]string{"to": Hex(r.Chain.SpokePool), "data": data}, ref(b.Hash)}})
	}
	vals := r.batch(ctx, calls)
	s, e := decodeCall(vals[0], "fillStatuses")
	if e != nil {
		return p, e
	}
	status := s[0].(*big.Int)
	if !status.IsUint64() || status.Uint64() > 2 {
		return p, errors.New("invalid_fill_status")
	}
	p.FillStatus = Ptr(uint8(status.Uint64()))
	paused, e := decodeCall(vals[1], "pausedFills")
	if e != nil {
		return p, e
	}
	p.PausedFills = Ptr(paused[0].(bool))
	clock, e := decodeCall(vals[2], "getCurrentTime")
	if e != nil {
		return p, e
	}
	n := clock[0].(*big.Int)
	if !n.IsUint64() {
		return p, errors.New("contract_clock_overflow")
	}
	p.ContractTime = Ptr(n.Uint64())
	if err = r.canonical(ctx, b); err != nil {
		return p, err
	}
	p.ProbeStatus = "ok"
	return p, nil
}

// Receipt obtains transaction and receipt together, checks all positions against
// an independently read canonical header, and retains the complete raw receipt.
func (r *Reader) Receipt(ctx context.Context, txHash string) (out TxReceipt, err error) {
	out = TxReceipt{ChainId: r.Chain.ChainID, TxHash: txHash, RequestedAt: Now(), FeeRule: "unknown", Reason: "fee_components_incomplete"}
	defer func() { out.AvailableAt = Now() }()
	if len(txHash) != 32 {
		return out, errors.New("invalid_tx_hash")
	}
	v := r.batch(ctx, []ethereum.Call{{Method: "eth_getTransactionByHash", Params: []any{Hex(txHash)}}, {Method: "eth_getTransactionReceipt", Params: []any{Hex(txHash)}}})
	for _, x := range v {
		if x.Err != nil {
			return out, x.Err
		}
	}
	var tx struct {
		Hash, BlockHash, BlockNumber, TransactionIndex, From string
		To                                                   *string
		Type, Input, Value, ChainID                          string
	}
	var receipt struct {
		TransactionHash, BlockHash, BlockNumber, TransactionIndex, From string
		To                                                              *string
		Status, GasUsed, EffectiveGasPrice                              string
		L1Fee, OperatorFee, GasUsedForL1                                *string
		Logs                                                            []rpcLog
	}
	if json.Unmarshal(v[0].Raw, &tx) != nil || string(v[0].Raw) == "null" || json.Unmarshal(v[1].Raw, &receipt) != nil || string(v[1].Raw) == "null" {
		return out, errors.New("transaction_or_receipt_missing")
	}
	bh, e := ParseHex(receipt.BlockHash, 32)
	if e != nil {
		return out, e
	}
	h, e := ParseHex(receipt.TransactionHash, 32)
	if e != nil || h != txHash {
		return out, errors.New("receipt_hash_mismatch")
	}
	if !strings.EqualFold(tx.Hash, receipt.TransactionHash) || !strings.EqualFold(tx.BlockHash, receipt.BlockHash) || tx.BlockNumber != receipt.BlockNumber || tx.TransactionIndex != receipt.TransactionIndex || !strings.EqualFold(tx.From, receipt.From) {
		return out, errors.New("transaction_receipt_mismatch")
	}
	if tx.To == nil != (receipt.To == nil) || tx.To != nil && !strings.EqualFold(*tx.To, *receipt.To) {
		return out, errors.New("transaction_receipt_recipient_mismatch")
	}
	n, e := q64(receipt.BlockNumber)
	if e != nil {
		return out, e
	}
	b, e := r.Header(ctx, fmt.Sprintf("0x%x", n))
	if e != nil {
		return out, e
	}
	if b.Hash != bh {
		return out, errors.New("receipt_noncanonical")
	}
	out.BlockNumber = n
	out.BlockHash = bh
	out.BlockTime = b.Time
	idx, e := q64(receipt.TransactionIndex)
	if e != nil || idx > 1<<32-1 {
		return out, errors.New("invalid_receipt_index")
	}
	out.TxIndex = uint32(idx)
	out.Sender, e = ParseHex(tx.From, 20)
	if e != nil {
		return out, e
	}
	if tx.To != nil {
		a, e := ParseHex(*tx.To, 20)
		if e != nil {
			return out, e
		}
		out.Recipient = &a
	}
	typ, e := q64(tx.Type)
	if e != nil || typ > 255 {
		return out, errors.New("invalid_transaction_type")
	}
	out.TxType = uint8(typ)
	if !strings.HasPrefix(tx.Input, "0x") || len(tx.Input)%2 != 0 {
		return out, errors.New("invalid_transaction_input")
	}
	input, e := ParseHex(tx.Input, (len(tx.Input)-2)/2)
	if e != nil {
		return out, e
	}
	out.CalldataHash = string(crypto.Keccak256([]byte(input)))
	if len(input) >= 4 {
		out.InputSelector = Ptr(input[:4])
	}
	out.TxValueWei, e = Quantity(tx.Value)
	if e != nil {
		return out, e
	}
	if tx.ChainID != "" {
		chain, e := q64(tx.ChainID)
		if e != nil || chain != r.Chain.ChainID {
			return out, errors.New("transaction_wrong_chain")
		}
	}
	status, e := q64(receipt.Status)
	if e != nil || status > 1 {
		return out, errors.New("invalid_receipt_status")
	}
	out.Success = status == 1
	out.GasUsed, e = q64(receipt.GasUsed)
	if e != nil {
		return out, e
	}
	out.EffectiveGasPriceWei, e = Quantity(receipt.EffectiveGasPrice)
	if e != nil {
		return out, e
	}
	out.ExecutionFeeWei = new(big.Int).Mul(new(big.Int).SetUint64(out.GasUsed), out.EffectiveGasPriceWei)
	if out.ExecutionFeeWei.BitLen() > 256 {
		return out, errors.New("execution_fee_overflow")
	}
	if receipt.L1Fee != nil {
		out.L1DataFeeWei, e = Quantity(*receipt.L1Fee)
		if e != nil {
			return out, e
		}
	}
	if receipt.OperatorFee != nil {
		out.OperatorFeeWei, e = Quantity(*receipt.OperatorFee)
		if e != nil {
			return out, e
		}
	}
	if receipt.GasUsedForL1 != nil {
		q, e := q64(*receipt.GasUsedForL1)
		if e != nil {
			return out, e
		}
		out.GasUsedForL1 = &q
	}
	switch r.Chain.ChainID {
	case 8453:
		out.FeeRule = "base_explicit_components"
		if out.L1DataFeeWei != nil && out.OperatorFeeWei != nil {
			out.TotalFeeWei = new(big.Int).Add(out.ExecutionFeeWei, out.L1DataFeeWei)
			out.TotalFeeWei.Add(out.TotalFeeWei, out.OperatorFeeWei)
			out.FeeComplete = true
			out.Reason = ""
		}
	case 42161:
		out.FeeRule = "arbitrum_nitro_gas_includes_l1"
		out.TotalFeeWei = new(big.Int).Set(out.ExecutionFeeWei)
		out.FeeComplete = true
		out.Reason = ""
	}
	if out.TotalFeeWei != nil && out.TotalFeeWei.BitLen() > 256 {
		return out, errors.New("total_fee_overflow")
	}
	if receipt.Logs == nil {
		return out, errors.New("missing_receipt_logs")
	}
	seen := map[uint32]bool{}
	for _, raw := range receipt.Logs {
		l, e := parseLog(raw, r.Chain.ChainID, string(v[1].Payload[:]))
		if e != nil {
			return out, e
		}
		if l.Removed || l.BlockHash != bh || l.BlockNumber != n || l.TxHash != txHash || l.TxIndex != out.TxIndex || seen[l.LogIndex] {
			return out, errors.New("receipt_log_anchor_mismatch")
		}
		seen[l.LogIndex] = true
	}
	out.TransactionPayloadHash = string(v[0].Payload[:])
	out.ReceiptPayloadHash = string(v[1].Payload[:])
	return out, r.canonical(ctx, b)
}
