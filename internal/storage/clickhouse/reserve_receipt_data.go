package clickhouse

import (
	"context"
	"errors"
	"reflect"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

func (c *Client) ReserveReceiptData(ctx context.Context, a dex.Anchor, tx dex.Hash) (reserve.ReceiptData, bool, error) {
	rows, e := reserveRead[reserve.ReceiptData](ctx, c, "reserve_receipt_data", "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", rawHash(a.Hash), rawHash(tx))
	if e != nil {
		return reserve.ReceiptData{}, false, e
	}
	if len(rows) == 0 {
		return reserve.ReceiptData{}, false, nil
	}
	if len(rows) != 1 {
		return reserve.ReceiptData{}, false, errors.New("duplicate_receipt_data_identity")
	}
	r, ok, e := c.ReserveReceipt(ctx, a, tx)
	if e != nil {
		return rows[0], false, e
	}
	if !ok {
		return rows[0], false, errors.New("receipt_data_summary_missing")
	}
	if e = reserve.ValidateReceiptData(rows[0], r); e != nil {
		return rows[0], false, e
	}
	return rows[0], true, nil
}
func (c *Client) WriteReserveReceiptData(ctx context.Context, d reserve.ReceiptData, r dex.Receipt) error {
	if e := reserve.ValidateReceiptData(d, r); e != nil {
		return e
	}
	existing, ok, e := c.ReserveReceiptData(ctx, r.Anchor, r.TxHash)
	if e != nil {
		return e
	}
	if ok {
		if existing.DataHash != d.DataHash {
			return errors.New("conflicting_receipt_data")
		}
		return nil
	}
	return c.dexInsert(ctx, "reserve_receipt_data", reserveColumns(reflect.TypeOf(d)), reserveRows([]reserve.ReceiptData{d}))
}
func (c *Client) ReserveReceipts(ctx context.Context) ([]dex.Receipt, error) {
	rs, e := c.conn.Query(ctx, "SELECT block_hash,tx_hash FROM "+c.table("dex_tx_receipt")+" FINAL WHERE chain_id=1 ORDER BY block_number,tx_hash")
	if e != nil {
		return nil, e
	}
	type key struct {
		a  dex.Anchor
		tx dex.Hash
	}
	keys := []key{}
	for rs.Next() {
		var block, tx string
		if e = rs.Scan(&block, &tx); e != nil {
			rs.Close()
			return nil, e
		}
		k := key{}
		copy(k.a.Hash[:], block)
		copy(k.tx[:], tx)
		keys = append(keys, k)
	}
	e = rs.Err()
	rs.Close()
	if e != nil {
		return nil, e
	}
	rows := make([]dex.Receipt, 0, len(keys))
	for _, k := range keys {
		r, ok, e := c.ReserveReceipt(ctx, k.a, k.tx)
		if e != nil {
			return nil, e
		}
		if !ok {
			return nil, errors.New("receipt_disappeared_during_migration")
		}
		rows = append(rows, r)
	}
	return rows, nil
}
