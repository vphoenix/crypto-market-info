package mysql

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func namedRecord(names string, values []any) (record, error) {
	fields := strings.Split(names, ",")
	if len(fields) != len(values) {
		return nil, fmt.Errorf("typed column count mismatch")
	}
	r := record{}
	for i, n := range fields {
		r[n] = values[i]
	}
	return r, nil
}
func (c *Client) putValues(ctx context.Context, q executor, table, names string, values []any) error {
	compactNames, compactRows, e := compactInsertValues(table, names, [][]any{values})
	if e != nil {
		return e
	}
	names = compactNames
	values = compactRows[0]
	t, e := c.mapping(table)
	if e != nil {
		return e
	}
	r, e := namedRecord(names, values)
	if e != nil {
		return e
	}
	return putImmutable(ctx, q, c.table(table), t, r)
}
func anchorFrom(r record) dex.Anchor {
	a := dex.Anchor{ChainID: r["chain_id"].(uint64), Number: r["block_number"].(uint64), Time: r["block_time"].(time.Time)}
	copy(a.Hash[:], r["block_hash"].(string))
	copy(a.Manifest[:], r["manifest_hash"].(string))
	copy(a.Batch[:], r["batch_id"].(string))
	copy(a.Payload[:], r["payload_hash"].(string))
	return a
}
func bigValue(r record, name string) *big.Int {
	if v := r[name]; v != nil {
		n := v.(big.Int)
		return &n
	}
	return nil
}
func hashValue(r record, name string) (h dex.Hash)       { copy(h[:], r[name].(string)); return h }
func addressValue(r record, name string) (a dex.Address) { copy(a[:], r[name].(string)); return a }
func logFrom(r record) dex.Log {
	l := dex.Log{Anchor: anchorFrom(r), TxHash: hashValue(r, "tx_hash"), TxIndex: r["tx_index"].(uint32), Index: r["log_index"].(uint32), Emitter: addressValue(r, "emitter"), Data: []byte(r["data"].(string)), Event: r["event_type"].(string), Removed: r["removed"].(bool)}
	for _, v := range r["topics"].([]any) {
		var h dex.Hash
		copy(h[:], v.(string))
		l.Topics = append(l.Topics, h)
	}
	return l
}
func receiptFrom(r record) dex.Receipt {
	v := dex.Receipt{Anchor: anchorFrom(r), TxHash: hashValue(r, "tx_hash"), TxIndex: r["tx_index"].(uint32), From: addressValue(r, "sender"), To: addressValue(r, "recipient"), HasTo: r["has_recipient"].(bool), Type: r["tx_type"].(uint8), Value: bigValue(r, "value_raw"), CalldataHash: hashValue(r, "calldata_hash"), Status: r["status"].(uint8), GasUsed: r["gas_used"].(uint64), GasPrice: bigValue(r, "effective_gas_price"), LogCount: r["receipt_log_count"].(uint32), ReceiptHash: hashValue(r, "receipt_hash"), AvailableAt: r["available_at"].(time.Time)}
	if s := r["input_selector"]; s != nil {
		v.Selector = []byte(s.(string))
	}
	return v
}
func (c *Client) dexReceipts(ctx context.Context, q executor, where string, args ...any) ([]dex.Receipt, error) {
	t, e := c.mapping("dex_tx_receipt")
	if e != nil {
		return nil, e
	}
	rows, e := readRecords(ctx, q, c.table(t.Name), t, where, args...)
	if e != nil {
		return nil, e
	}
	out := make([]dex.Receipt, len(rows))
	for i, r := range rows {
		out[i] = receiptFrom(r)
	}
	return out, nil
}
func (c *Client) dexLogs(ctx context.Context, q executor, where string, args ...any) ([]dex.Log, error) {
	t, e := c.mapping("dex_log")
	if e != nil {
		return nil, e
	}
	rows, e := readRecords(ctx, q, c.table(t.Name), t, where, args...)
	if e != nil {
		return nil, e
	}
	out := make([]dex.Log, len(rows))
	for i, r := range rows {
		out[i] = logFrom(r)
	}
	return out, nil
}
func (c *Client) putSharedReceipt(ctx context.Context, q executor, r dex.Receipt) error {
	if r.ChainID == 0 || r.Hash == (dex.Hash{}) || r.TxHash == (dex.Hash{}) || !dex.ValidUint(r.Value, 256) || !dex.ValidUint(r.GasPrice, 256) {
		return fmt.Errorf("invalid receipt anchor or values")
	}
	old, e := c.dexReceipts(ctx, q, "WHERE chain_id=? AND block_hash=? AND tx_hash=? FOR UPDATE", r.ChainID, r.Hash[:], r.TxHash[:])
	if e != nil {
		return e
	}
	if len(old) > 1 {
		return contracts.ErrConflict
	}
	if len(old) == 1 {
		if old[0].ReceiptHash != r.ReceiptHash || old[0].CalldataHash != r.CalldataHash {
			return contracts.ErrConflict
		}
		return nil
	}
	return c.putValues(ctx, q, "dex_tx_receipt", dexReceiptColumns, receiptValues(r))
}
