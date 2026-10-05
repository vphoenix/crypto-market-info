package clickhouse

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

//go:embed reserve_schema.sql
var reserveDDL string

func ReserveSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, errors.New("invalid_database")
	}
	ss := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (reserve_\w+).*?;`).FindAllString(reserveDDL, -1)
	for i, s := range ss {
		ss[i] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (reserve_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	dexSS, e := DEXSchemaStatements(database)
	if e != nil {
		return nil, e
	}
	ss = append(ss, dexSS[3:]...)
	if len(ss) != 7 {
		return nil, errors.New("reserve_schema_expected_seven_tables")
	}
	return ss, nil
}
func (c *Client) InitReserveSchema(ctx context.Context) error {
	ss, e := ReserveSchemaStatements(c.database)
	if e != nil {
		return e
	}
	for _, s := range ss {
		if e = c.conn.Exec(ctx, s); e != nil {
			return e
		}
	}
	return nil
}
func reserveColumns(t reflect.Type) string {
	cols := []string{}
	for i := 0; i < t.NumField(); i++ {
		cols = append(cols, t.Field(i).Tag.Get("ch"))
	}
	return strings.Join(cols, ",")
}
func reserveRows[T any](in []T) [][]any {
	rows := make([][]any, 0, len(in))
	for _, row := range in {
		v := reflect.ValueOf(row)
		values := []any{}
		for i := 0; i < v.NumField(); i++ {
			values = append(values, v.Field(i).Interface())
		}
		rows = append(rows, values)
	}
	return rows
}
func reserveRead[T any](ctx context.Context, c *Client, table, where string, args ...any) ([]T, error) {
	var row T
	rs, e := c.conn.Query(ctx, "SELECT "+reserveColumns(reflect.TypeOf(row))+" FROM "+c.table(table)+" FINAL "+where, args...)
	if e != nil {
		return nil, e
	}
	defer rs.Close()
	out := []T{}
	for rs.Next() {
		var v T
		x := reflect.ValueOf(&v).Elem()
		dest := []any{}
		arrays := map[int]*[]map[string]any{}
		for i := 0; i < x.NumField(); i++ {
			field := x.Field(i)
			if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Struct {
				rows := new([]map[string]any)
				arrays[i] = rows
				dest = append(dest, rows)
			} else {
				dest = append(dest, field.Addr().Interface())
			}
		}
		if e = rs.Scan(dest...); e != nil {
			return nil, e
		}
		for i, rows := range arrays {
			if e = reserveAssign(x.Field(i), reflect.ValueOf(*rows)); e != nil {
				return nil, fmt.Errorf("tuple %s: %w", x.Type().Field(i).Name, e)
			}
		}
		out = append(out, v)
	}
	return out, rs.Err()
}

// The native driver exposes non-nullable UInt256 in a named Tuple as big.Int,
// while Nullable(UInt256) is *big.Int. Adapt those exact types without JSON or
// numeric conversion. ScanStruct alone cannot read our required *big.Int fields.
func reserveAssign(dst, src reflect.Value) error {
	if !src.IsValid() {
		dst.SetZero()
		return nil
	}
	for src.Kind() == reflect.Interface {
		if src.IsNil() {
			dst.SetZero()
			return nil
		}
		src = src.Elem()
	}
	if src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return nil
	}
	if dst.Kind() == reflect.Pointer {
		if src.Kind() == reflect.Pointer && src.IsNil() {
			dst.SetZero()
			return nil
		}
		dst.Set(reflect.New(dst.Type().Elem()))
		return reserveAssign(dst.Elem(), src)
	}
	if src.Kind() == reflect.Pointer {
		if src.IsNil() {
			return errors.New("unexpected_null_tuple_field")
		}
		return reserveAssign(dst, src.Elem())
	}
	if dst.Kind() == reflect.Struct && src.Kind() == reflect.Map {
		for i := 0; i < dst.NumField(); i++ {
			name := dst.Type().Field(i).Tag.Get("json")
			value := src.MapIndex(reflect.ValueOf(name))
			if !value.IsValid() {
				return fmt.Errorf("missing_tuple_field_%s", name)
			}
			if e := reserveAssign(dst.Field(i), value); e != nil {
				return e
			}
		}
		return nil
	}
	if dst.Kind() == reflect.Slice && src.Kind() == reflect.Slice {
		dst.Set(reflect.MakeSlice(dst.Type(), src.Len(), src.Len()))
		for i := 0; i < src.Len(); i++ {
			if e := reserveAssign(dst.Index(i), src.Index(i)); e != nil {
				return e
			}
		}
		return nil
	}
	return fmt.Errorf("unsupported exact tuple type %s -> %s", src.Type(), dst.Type())
}
func (c *Client) ReserveCaptures(ctx context.Context, manifest string) ([]reserve.Capture, error) {
	return reserveRead[reserve.Capture](ctx, c, "reserve_capture", "WHERE chain_id=1 AND manifest_hash=? ORDER BY to_block,batch_id", manifest)
}
func (c *Client) ReserveBatch(ctx context.Context, cap reserve.Capture) (reserve.Batch, error) {
	b := reserve.Batch{Capture: cap}
	var e error
	args := []any{cap.ManifestHash, cap.CaptureId, cap.BatchId}
	where := "WHERE chain_id=1 AND manifest_hash=? AND capture_id=? AND batch_id=?"
	b.States, e = reserveRead[reserve.State](ctx, c, "reserve_folio_state", where, args...)
	if e != nil {
		return b, e
	}
	b.Quotes, e = reserveRead[reserve.Quote](ctx, c, "reserve_route_quote", where, args...)
	if e != nil {
		return b, e
	}
	b.Logs, e = c.reserveLogs(ctx, "WHERE chain_id=1 AND manifest_hash=? AND batch_id=?", cap.ManifestHash, cap.BatchId)
	if e != nil {
		return b, e
	}
	for _, ref := range cap.ReceiptRefs {
		var anchor dex.Anchor
		var tx dex.Hash
		copy(anchor.Hash[:], ref.BlockHash)
		copy(tx[:], ref.TxHash)
		r, ok, e := c.ReserveReceipt(ctx, anchor, tx)
		if e != nil {
			return b, e
		}
		if !ok || rawHash(r.ReceiptHash) != ref.ReceiptHash || rawHash(r.CalldataHash) != ref.CalldataHash {
			return b, errors.New("capture_receipt_reference_mismatch")
		}
		b.Receipts = append(b.Receipts, r)
		d, ok, e := c.ReserveReceiptData(ctx, r.Anchor, r.TxHash)
		if e != nil {
			return b, e
		}
		if !ok {
			return b, errors.New("capture_receipt_data_missing")
		}
		b.ReceiptData = append(b.ReceiptData, d)
	}
	return b, reserve.Validate(b)
}
func (c *Client) ReserveReceipt(ctx context.Context, a dex.Anchor, tx dex.Hash) (dex.Receipt, bool, error) {
	var r dex.Receipt
	var hash, mh, bh, ph, txh, from, to, calldata, receipt string
	var selector *string
	rs, e := c.conn.Query(ctx, "SELECT "+dexReceiptColumns+" FROM "+c.table("dex_tx_receipt")+" FINAL WHERE chain_id=1 AND block_hash=? AND tx_hash=?", rawHash(a.Hash), rawHash(tx))
	if e != nil {
		return r, false, e
	}
	defer rs.Close()
	if !rs.Next() {
		return r, false, rs.Err()
	}
	e = rs.Scan(&r.ChainID, &r.Number, &hash, &mh, &bh, &ph, &r.Time, &txh, &r.TxIndex, &from, &to, &r.HasTo, &r.Type, &r.Value, &selector, &calldata, &r.Status, &r.GasUsed, &r.GasPrice, &r.LogCount, &receipt, &r.AvailableAt)
	if e != nil {
		return r, false, e
	}
	copy(r.Hash[:], hash)
	copy(r.Manifest[:], mh)
	copy(r.Batch[:], bh)
	copy(r.Payload[:], ph)
	copy(r.TxHash[:], txh)
	copy(r.From[:], from)
	copy(r.To[:], to)
	copy(r.CalldataHash[:], calldata)
	copy(r.ReceiptHash[:], receipt)
	if selector != nil {
		r.Selector = []byte(*selector)
	}
	if rs.Next() {
		return r, false, errors.New("duplicate_receipt_identity")
	}
	return r, true, rs.Err()
}
func (c *Client) WriteReserveBatch(ctx context.Context, b reserve.Batch) error {
	if e := reserve.Validate(b); e != nil {
		return e
	}
	if len(b.ReceiptData) != len(b.Receipts) {
		return errors.New("receipt_data_members_missing")
	}
	for i, d := range b.ReceiptData {
		if e := reserve.ReceiptDataContains(d, b.Receipts[i], receiptLogs(b.Logs, b.Receipts[i])); e != nil {
			return e
		}
	}
	newReceipts := []dex.Receipt{}
	for _, r := range b.Receipts {
		existing, ok, e := c.ReserveReceipt(ctx, r.Anchor, r.TxHash)
		if e != nil {
			return e
		}
		if ok {
			if existing.ReceiptHash != r.ReceiptHash || existing.CalldataHash != r.CalldataHash {
				return errors.New("conflicting_receipt")
			}
		} else {
			newReceipts = append(newReceipts, r)
		}
	}
	logs := [][]any{}
	for _, l := range b.Logs {
		rs, e := c.conn.Query(ctx, "SELECT block_number,tx_index,emitter,topics,data,removed FROM "+c.table("dex_log")+" FINAL WHERE chain_id=1 AND block_hash=? AND tx_hash=? AND log_index=?", rawHash(l.Hash), rawHash(l.TxHash), l.Index)
		if e != nil {
			return e
		}
		for rs.Next() {
			old := l
			var emitter, data string
			var topics []string
			if e = rs.Scan(&old.Number, &old.TxIndex, &emitter, &topics, &data, &old.Removed); e != nil {
				rs.Close()
				return e
			}
			copy(old.Emitter[:], emitter)
			old.Data = []byte(data)
			old.Topics = []dex.Hash{}
			for _, s := range topics {
				var h dex.Hash
				copy(h[:], s)
				old.Topics = append(old.Topics, h)
			}
			if reserve.LogFact(old) != reserve.LogFact(l) {
				rs.Close()
				return errors.New("conflicting_log_fact")
			}
		}
		e = rs.Err()
		rs.Close()
		if e != nil {
			return e
		}
		logs = append(logs, logValues(l))
	}
	receipts := [][]any{}
	for _, r := range newReceipts {
		receipts = append(receipts, receiptValues(r))
	}
	// Committed capture is always last. DB retries preserve every identity/time.
	for _, v := range []struct {
		table, cols string
		rows        [][]any
	}{{"reserve_folio_state", reserveColumns(reflect.TypeOf(reserve.State{})), reserveRows(b.States)}, {"reserve_route_quote", reserveColumns(reflect.TypeOf(reserve.Quote{})), reserveRows(b.Quotes)}, {"dex_log", dexLogColumns, logs}, {"dex_tx_receipt", dexReceiptColumns, receipts}, {"reserve_capture", reserveColumns(reflect.TypeOf(reserve.Capture{})), reserveRows([]reserve.Capture{b.Capture})}} {
		if v.table == "reserve_capture" {
			for i, d := range b.ReceiptData {
				if e := c.WriteReserveReceiptData(ctx, d, b.Receipts[i]); e != nil {
					return e
				}
			}
		}
		if e := c.dexInsert(ctx, v.table, v.cols, v.rows); e != nil {
			return fmt.Errorf("%s: %w", v.table, e)
		}
	}
	return nil
}

func receiptLogs(logs []dex.Log, r dex.Receipt) []dex.Log {
	out := []dex.Log{}
	for _, l := range logs {
		if l.Hash == r.Hash && l.TxHash == r.TxHash {
			out = append(out, l)
		}
	}
	return out
}
func (c *Client) WriteReserveRevision(ctx context.Context, v reserve.Capture) error {
	return c.dexInsert(ctx, "reserve_capture", reserveColumns(reflect.TypeOf(v)), reserveRows([]reserve.Capture{v}))
}

func (c *Client) reserveLogs(ctx context.Context, where string, args ...any) ([]dex.Log, error) {
	out := []dex.Log{}
	rs, e := c.conn.Query(ctx, "SELECT "+dexLogColumns+" FROM "+c.table("dex_log")+" FINAL "+where+" ORDER BY block_number,log_index", args...)
	if e != nil {
		return nil, e
	}
	for rs.Next() {
		var l dex.Log
		var hash, mh, bh, ph, tx, emitter string
		var topics []string
		var data string
		if e = rs.Scan(&l.ChainID, &l.Number, &hash, &mh, &bh, &ph, &l.Time, &tx, &l.TxIndex, &l.Index, &emitter, &topics, &data, &l.Event, &l.Removed); e != nil {
			rs.Close()
			return nil, e
		}
		copy(l.Hash[:], hash)
		copy(l.Manifest[:], mh)
		copy(l.Batch[:], bh)
		copy(l.Payload[:], ph)
		copy(l.TxHash[:], tx)
		copy(l.Emitter[:], emitter)
		l.Data = []byte(data)
		for _, s := range topics {
			var h dex.Hash
			copy(h[:], s)
			l.Topics = append(l.Topics, h)
		}
		out = append(out, l)
	}
	e = rs.Err()
	rs.Close()
	if e != nil {
		return nil, e
	}
	return out, nil
}
