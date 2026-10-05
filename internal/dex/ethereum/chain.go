package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"sort"
	"time"
)

func quantityResult(r Result, bits int) (*big.Int, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	var s string
	if json.Unmarshal(r.Raw, &s) != nil {
		return nil, errors.New("quantity_result")
	}
	return Quantity(s, bits)
}
func (c *Client) Header(ctx context.Context, tag string) (dex.Block, error) {
	return c.header(ctx, "eth_getBlockByNumber", tag)
}
func (c *Client) HeaderHash(ctx context.Context, h dex.Hash) (dex.Block, error) {
	return c.header(ctx, "eth_getBlockByHash", h.String())
}
func (c *Client) HeaderTags(ctx context.Context, tags ...string) ([]dex.Block, error) {
	calls := make([]Call, len(tags))
	for i, tag := range tags {
		calls[i] = Call{"eth_getBlockByNumber", []any{tag, false}}
	}
	rr := c.Batch(ctx, calls)
	out := make([]dex.Block, len(tags))
	for i, result := range rr {
		block, err := parseHeader(result, "eth_getBlockByNumber", tags[i])
		if err != nil {
			return nil, err
		}
		out[i] = block
	}
	return out, nil
}
func (c *Client) header(ctx context.Context, method, tag string) (dex.Block, error) {
	r := c.One(ctx, method, []any{tag, false})
	return parseHeader(r, method, tag)
}
func parseHeader(r Result, method, tag string) (dex.Block, error) {
	var b dex.Block
	if r.Err != nil {
		return b, r.Err
	}
	var v struct{ Number, Hash, ParentHash, Timestamp, BaseFeePerGas, Miner string }
	if json.Unmarshal(r.Raw, &v) != nil {
		return b, errors.New("header_encoding")
	}
	n, e := Quantity(v.Number, 64)
	if e != nil {
		return b, e
	}
	ts, e := Quantity(v.Timestamp, 63)
	if e != nil {
		return b, e
	}
	fee, e := Quantity(v.BaseFeePerGas, 256)
	if e != nil {
		return b, e
	}
	h, e := dex.ParseHash(v.Hash)
	if e != nil {
		return b, e
	}
	parent, e := dex.ParseHash(v.ParentHash)
	if e != nil {
		return b, e
	}
	miner, e := dex.ParseAddress(v.Miner)
	if e != nil {
		return b, e
	}
	b = dex.Block{Anchor: dex.Anchor{ChainID: 1, Number: n.Uint64(), Hash: h, Manifest: ManifestHash(), Payload: r.Payload, Time: time.Unix(ts.Int64(), 0).UTC()}, Parent: parent, BaseFee: fee, Miner: miner, ReceivedAt: r.At, Canonical: true, Finality: "head", LogCoverage: "missing", ReceiptCoverage: "missing", QuoteCoverage: "missing"}
	if method == "eth_getBlockByHash" && h.String() != tag {
		return b, errors.New("header_hash_mismatch")
	}
	if len(tag) > 2 && tag[:2] == "0x" && method == "eth_getBlockByNumber" {
		want, e := Quantity(tag, 64)
		if e != nil || want.Uint64() != b.Number {
			return b, errors.New("header_number_mismatch")
		}
	}
	return b, nil
}
func Height(n uint64) string { return fmt.Sprintf("0x%x", n) }
func (c *Client) Canonical(ctx context.Context, b dex.Block) error {
	now, e := c.Header(ctx, Height(b.Number))
	if e != nil {
		return e
	}
	if now.Hash != b.Hash {
		return errors.New("block_no_longer_canonical")
	}
	return nil
}

type rpcLog struct {
	BlockNumber, BlockHash, TransactionHash, TransactionIndex, LogIndex, Address, Data string
	Topics                                                                             []string
	Removed                                                                            *bool
}

func parseLog(v rpcLog, a dex.Anchor) (dex.Log, error) {
	l := dex.Log{Anchor: a}
	n, e := Quantity(v.BlockNumber, 64)
	if e != nil || n.Uint64() != a.Number {
		return l, errors.New("log_block_number")
	}
	h, e := dex.ParseHash(v.BlockHash)
	if e != nil || h != a.Hash {
		return l, errors.New("log_block_hash")
	}
	l.TxHash, e = dex.ParseHash(v.TransactionHash)
	if e != nil {
		return l, e
	}
	ix, e := Quantity(v.TransactionIndex, 32)
	if e != nil {
		return l, e
	}
	l.TxIndex = uint32(ix.Uint64())
	ix, e = Quantity(v.LogIndex, 32)
	if e != nil {
		return l, e
	}
	l.Index = uint32(ix.Uint64())
	l.Emitter, e = dex.ParseAddress(v.Address)
	if e != nil {
		return l, e
	}
	l.Data, e = Bytes(v.Data)
	if e != nil {
		return l, e
	}
	if v.Removed == nil || *v.Removed {
		return l, errors.New("removed_or_unknown_log")
	}
	if len(v.Topics) > 4 {
		return l, errors.New("too_many_topics")
	}
	for _, s := range v.Topics {
		h, e := dex.ParseHash(s)
		if e != nil {
			return l, e
		}
		l.Topics = append(l.Topics, h)
	}
	l.Event = "unknown"
	if len(l.Topics) > 0 {
		if name, ok := DefaultManifest().Events[l.Topics[0].String()]; ok {
			l.Event = name
		}
	}
	return l, nil
}
func (c *Client) Logs(ctx context.Context, a dex.Anchor) ([]dex.Log, dex.Hash, error) {
	m := DefaultManifest()
	addresses := m.LogAddresses()
	var calls []Call
	// Public endpoints may reject large address OR filters even for one block.
	// Six per request is bounded; all groups must succeed before coverage is full.
	for start := 0; start < len(addresses); start += 6 {
		end := min(start+6, len(addresses))
		calls = append(calls, Call{"eth_getLogs", []any{map[string]any{"blockHash": a.Hash.String(), "address": addresses[start:end]}}})
	}
	rr := c.Batch(ctx, calls)
	members := []dex.Hash{}
	for _, r := range rr {
		members = append(members, r.Payload)
	}
	proof, e := c.Archive.PutObject(struct {
		Anchor     dex.Anchor
		RPCMembers []dex.Hash
		At         time.Time
	}{a, members, dex.Now()})
	if e != nil {
		return nil, dex.Hash{}, e
	}
	seen := map[uint32]bool{}
	out := []dex.Log{}
	for group, r := range rr {
		allowed := map[string]bool{}
		for _, addr := range addresses[group*6 : min((group+1)*6, len(addresses))] {
			allowed[addr] = true
		}
		if r.Err != nil {
			return nil, proof, r.Err
		}
		var vals []rpcLog
		if string(r.Raw) == "null" || json.Unmarshal(r.Raw, &vals) != nil {
			return nil, proof, errors.New("logs_encoding")
		}
		for _, v := range vals {
			l, e := parseLog(v, a)
			if e != nil {
				return nil, proof, e
			}
			if !allowed[l.Emitter.String()] || seen[l.Index] {
				return nil, proof, errors.New("logs_wrong_emitter_or_duplicate")
			}
			seen[l.Index] = true
			// Immutable same-hash log facts have a deterministic payload identity,
			// independent of retry response ids/times. The aggregate proof retains the
			// exact original RPC envelopes and their actual observation times.
			l.Payload, e = c.Archive.PutObject(v)
			if e != nil {
				return nil, proof, e
			}
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, proof, nil
}

// Receipts are bounded to 20 transactions per call site; complete raw receipts
// include all external legs. A partial fetch never invents an empty receipt.
func (c *Client) Receipts(ctx context.Context, a dex.Anchor, logs []dex.Log) ([]dex.Receipt, error) {
	hashes := []dex.Hash{}
	indexes := map[dex.Hash]uint32{}
	for _, l := range logs {
		if ix, ok := indexes[l.TxHash]; ok && ix != l.TxIndex {
			return nil, errors.New("tx_index_conflict")
		}
		indexes[l.TxHash] = l.TxIndex
		hashes = append(hashes, l.TxHash)
	}
	hashes = uniqueHashes(hashes)
	if len(hashes) > 20 {
		hashes = hashes[:20]
	}
	calls := []Call{}
	for _, h := range hashes {
		calls = append(calls, Call{"eth_getTransactionByHash", []any{h.String()}}, Call{"eth_getTransactionReceipt", []any{h.String()}})
	}
	rr := c.Batch(ctx, calls)
	out := []dex.Receipt{}
	var failure error
	for i, h := range hashes {
		r, e := c.parseReceipt(a, h, indexes[h], rr[2*i], rr[2*i+1])
		if e != nil {
			failure = e
			continue
		}
		out = append(out, r)
	}
	if len(out) != len(indexes) {
		failure = errors.New("receipts_incomplete")
	}
	return out, failure
}
func (c *Client) parseReceipt(a dex.Anchor, h dex.Hash, index uint32, tx, receipt Result) (dex.Receipt, error) {
	r := dex.Receipt{Anchor: a, TxHash: h, TxIndex: index, AvailableAt: receipt.At}
	if tx.Err != nil {
		return r, tx.Err
	}
	if receipt.Err != nil {
		return r, receipt.Err
	}
	var t struct{ Hash, BlockHash, BlockNumber, TransactionIndex, From, To, Type, Value, Input string }
	var v struct {
		TransactionHash, BlockHash, BlockNumber, TransactionIndex, From, To, Type, Status, GasUsed, EffectiveGasPrice string
		Logs                                                                                                          []rpcLog
	}
	if json.Unmarshal(tx.Raw, &t) != nil || json.Unmarshal(receipt.Raw, &v) != nil {
		return r, errors.New("receipt_encoding")
	}
	if t.Hash != h.String() || v.TransactionHash != h.String() || t.BlockHash != a.Hash.String() || v.BlockHash != a.Hash.String() || t.From != v.From || t.To != v.To || t.Type != v.Type {
		return r, errors.New("receipt_identity_mismatch")
	}
	for _, s := range []string{t.BlockNumber, v.BlockNumber} {
		n, e := Quantity(s, 64)
		if e != nil || n.Uint64() != a.Number {
			return r, errors.New("receipt_height")
		}
	}
	for _, s := range []string{t.TransactionIndex, v.TransactionIndex} {
		n, e := Quantity(s, 32)
		if e != nil || n.Uint64() != uint64(index) {
			return r, errors.New("receipt_index")
		}
	}
	var e error
	r.From, e = dex.ParseAddress(t.From)
	if e != nil {
		return r, e
	}
	if t.To != "" {
		r.To, e = dex.ParseAddress(t.To)
		if e != nil {
			return r, e
		}
		r.HasTo = true
	}
	typ, e := Quantity(t.Type, 8)
	if e != nil {
		return r, e
	}
	r.Type = uint8(typ.Uint64())
	r.Value, e = Quantity(t.Value, 256)
	if e != nil {
		return r, e
	}
	status, e := Quantity(v.Status, 1)
	if e != nil {
		return r, e
	}
	r.Status = uint8(status.Uint64())
	used, e := Quantity(v.GasUsed, 64)
	if e != nil {
		return r, e
	}
	r.GasUsed = used.Uint64()
	r.GasPrice, e = Quantity(v.EffectiveGasPrice, 256)
	if e != nil {
		return r, e
	}
	input, e := Bytes(t.Input)
	if e != nil {
		return r, e
	}
	if len(input) >= 4 {
		r.Selector = append([]byte(nil), input[:4]...)
	}
	if v.Logs == nil {
		return r, errors.New("receipt_logs_missing")
	}
	seen := map[uint32]bool{}
	for _, raw := range v.Logs {
		l, e := parseLog(raw, a)
		if e != nil || l.TxHash != h || l.TxIndex != index || seen[l.Index] {
			return r, errors.New("receipt_log_identity")
		}
		seen[l.Index] = true
	}
	r.LogCount = uint32(len(v.Logs))
	r.CalldataHash, e = c.Archive.Put(input)
	if e != nil {
		return r, e
	}
	r.ReceiptHash, e = c.Archive.Put(receipt.Raw)
	if e != nil {
		return r, e
	}
	r.Payload, e = c.Archive.PutObject([]dex.Hash{tx.Payload, receipt.Payload, r.CalldataHash, r.ReceiptHash})
	r.Batch = dex.ObjectHash(struct{ Hash, Payload dex.Hash }{h, r.Payload})
	return r, e
}

// DecodeReceipt permits a collector-owned Reader to retain both successful and
// failed transaction/receipt envelopes in its capture proof.
func (c *Client) DecodeReceipt(a dex.Anchor, h dex.Hash, index uint32, tx, receipt Result) (dex.Receipt, error) {
	return c.parseReceipt(a, h, index, tx, receipt)
}

func (c *Client) Headers(ctx context.Context, numbers []uint64) (map[uint64]dex.Block, error) {
	calls := make([]Call, len(numbers))
	for i, n := range numbers {
		calls[i] = Call{"eth_getBlockByNumber", []any{Height(n), false}}
	}
	rr := c.Batch(ctx, calls)
	out := map[uint64]dex.Block{}
	for i, r := range rr {
		b, e := parseHeader(r, "eth_getBlockByNumber", Height(numbers[i]))
		if e != nil {
			return nil, e
		}
		out[b.Number] = b
	}
	return out, nil
}
