package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
)

const dexAnchorColumns = "chain_id,block_number,block_hash,manifest_hash,batch_id,payload_hash,block_time"
const dexBlockColumns = dexAnchorColumns + ",parent_hash,base_fee_wei,fee_recipient,received_at,available_at,capture_mode,canonical,finality,revision,log_coverage,receipt_coverage,quote_coverage,expected_quotes,actual_quotes,log_count,receipt_count,quote_members,log_members,receipt_members,committed"
const dexQuoteColumns = dexAnchorColumns + ",quote_id,quote_role,route_id,quote_mode,token_in,token_out,requested_amount_raw,amount_in_raw,amount_out_raw,dust_dai,dust_usds,v3_input_token,v3_output_token,v3_input_raw,v3_output_raw,sqrt_price_after_x96,ticks_crossed,quoter_gas_estimate,status,reason,available_at"
const dexLogColumns = dexAnchorColumns + ",tx_hash,tx_index,log_index,emitter,topics,data,event_type,removed"
const dexReceiptColumns = dexAnchorColumns + ",tx_hash,tx_index,sender,recipient,has_recipient,tx_type,value_raw,input_selector,calldata_hash,status,gas_used,effective_gas_price,receipt_log_count,receipt_hash,available_at"

func rawHash(h dex.Hash) string       { return string(h[:]) }
func rawAddress(a dex.Address) string { return string(a[:]) }
func anchorValues(a dex.Anchor) []any {
	return []any{a.ChainID, a.Number, rawHash(a.Hash), rawHash(a.Manifest), rawHash(a.Batch), rawHash(a.Payload), a.Time}
}
func blockValues(b dex.Block) []any {
	return append(anchorValues(b.Anchor), rawHash(b.Parent), b.BaseFee, rawAddress(b.Miner), b.ReceivedAt, b.AvailableAt, b.Capture, b.Canonical, b.Finality, b.Revision, b.LogCoverage, b.ReceiptCoverage, b.QuoteCoverage, b.ExpectedQuotes, b.ActualQuotes, b.LogCount, b.ReceiptCount, rawHash(b.QuoteMembers), rawHash(b.LogMembers), rawHash(b.ReceiptMembers), b.Committed)
}
func quoteValues(q dex.Quote) []any {
	return append(anchorValues(q.Anchor), rawHash(q.ID), q.Role, q.Route, q.Mode, rawAddress(q.TokenIn), rawAddress(q.TokenOut), q.Requested, q.AmountIn, q.AmountOut, q.DustDAI, q.DustUSDS, rawAddress(q.V3InToken), rawAddress(q.V3OutToken), q.V3In, q.V3Out, q.SqrtAfter, q.TicksCrossed, q.GasEstimate, q.Status, q.Reason, q.AvailableAt)
}
func logValues(l dex.Log) []any {
	topics := []string{}
	for _, h := range l.Topics {
		topics = append(topics, rawHash(h))
	}
	return append(anchorValues(l.Anchor), rawHash(l.TxHash), l.TxIndex, l.Index, rawAddress(l.Emitter), topics, string(l.Data), l.Event, l.Removed)
}
func receiptValues(r dex.Receipt) []any {
	var selector *string
	if r.Selector != nil {
		s := string(r.Selector)
		selector = &s
	}
	return append(anchorValues(r.Anchor), rawHash(r.TxHash), r.TxIndex, rawAddress(r.From), rawAddress(r.To), r.HasTo, r.Type, r.Value, selector, rawHash(r.CalldataHash), r.Status, r.GasUsed, r.GasPrice, r.LogCount, rawHash(r.ReceiptHash), r.AvailableAt)
}
func (c *Client) dexInsert(ctx context.Context, table, cols string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	var err error
	cols, rows, err = c.compactOptionInsert(ctx, table, cols, rows)
	if err != nil {
		return err
	}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		b, e := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.table(table)+" ("+cols+")")
		if e != nil {
			return e
		}
		defer b.Abort()
		for _, r := range rows {
			if e = b.Append(r...); e != nil {
				return e
			}
		}
		return b.Send()
	})
}
func validAnchor(a dex.Anchor) bool {
	return a.ChainID == 1 && a.Number > 0 && a.Hash != (dex.Hash{}) && a.Manifest != (dex.Hash{}) && a.Batch != (dex.Hash{}) && a.Payload != (dex.Hash{}) && !a.Time.IsZero()
}
func sameBatch(a, b dex.Anchor) bool {
	return a.ChainID == b.ChainID && a.Number == b.Number && a.Hash == b.Hash && a.Manifest == b.Manifest && a.Batch == b.Batch && a.Time.Equal(b.Time)
}
func validateNumbers(nums ...*big.Int) error {
	for _, n := range nums {
		if n != nil && !dex.ValidUint(n, 256) {
			return errors.New("DEX UInt256 overflow")
		}
	}
	return nil
}

// WriteDEXBatch exposes no facts until the final block marker succeeds. The
// caller retains this immutable batch on errors, including ambiguous timeouts.
func (c *Client) WriteDEXBatch(ctx context.Context, b dex.Batch) error {
	if !validAnchor(b.Block.Anchor) || !b.Block.Committed || !dex.ValidUint(b.Block.BaseFee, 256) {
		return errors.New("invalid DEX block batch")
	}
	if b.Block.ActualQuotes != uint32(len(b.Quotes)) || b.Block.LogCount != uint32(len(b.Logs)) || b.Block.ReceiptCount != uint32(len(b.Receipts)) || b.Block.QuoteMembers != dex.QuoteDigest(b.Quotes) || b.Block.LogMembers != dex.LogDigest(b.Logs) || b.Block.ReceiptMembers != dex.ReceiptDigest(b.Receipts) {
		return errors.New("DEX member digest mismatch")
	}
	var skyRows, quoteRows, logRows, receiptRows [][]any
	if s := b.Sky; s != nil {
		if !sameBatch(s.Anchor, b.Block.Anchor) || !validAnchor(s.Anchor) {
			return errors.New("DEX sky anchor mismatch")
		}
		if e := validateNumbers(s.Tin, s.Tout, s.Buf, s.DAICash, s.USDCCash, s.Allowance); e != nil {
			return e
		}
		if s.Complete {
			for _, n := range []*big.Int{s.Tin, s.Tout, s.Buf, s.DAICash, s.USDCCash, s.Allowance} {
				if n == nil {
					return errors.New("DEX complete state missing value")
				}
			}
		}
		skyRows = append(skyRows, append(anchorValues(s.Anchor), s.Module, s.Tin, s.Tout, s.Buf, s.DAICash, s.USDCCash, s.Allowance, s.VatLive, s.DAIJoinLive, s.DAIJoinWard, s.USDSJoinWard, s.IdentityOK, s.Complete, s.Reason))
	}
	seen := map[dex.Hash]bool{}
	for _, q := range b.Quotes {
		if !sameBatch(q.Anchor, b.Block.Anchor) || !validAnchor(q.Anchor) || !dex.ValidUint(q.Requested, 256) || seen[q.ID] {
			return errors.New("DEX quote identity mismatch")
		}
		want := q
		want.SetID()
		if want.ID != q.ID {
			return errors.New("DEX quote id mismatch")
		}
		seen[q.ID] = true
		if e := validateNumbers(q.AmountIn, q.AmountOut, q.DustDAI, q.DustUSDS, q.V3In, q.V3Out, q.SqrtAfter, q.GasEstimate); e != nil {
			return e
		}
		if q.Status == "ok" && (q.AmountIn == nil || q.AmountOut == nil || q.V3In == nil || q.V3Out == nil || q.SqrtAfter == nil || q.SqrtAfter.BitLen() > 160 || q.GasEstimate == nil) {
			return errors.New("DEX successful quote missing fields")
		}
		quoteRows = append(quoteRows, quoteValues(q))
	}
	for _, l := range b.Logs {
		if !sameBatch(l.Anchor, b.Block.Anchor) || !validAnchor(l.Anchor) || l.Removed {
			return errors.New("DEX log identity mismatch")
		}
		logRows = append(logRows, logValues(l))
	}
	for _, r := range b.Receipts {
		if r.Hash != b.Block.Hash || r.Number != b.Block.Number || !validAnchor(r.Anchor) || !dex.ValidUint(r.Value, 256) || !dex.ValidUint(r.GasPrice, 256) {
			return errors.New("DEX receipt identity mismatch")
		}
		receiptRows = append(receiptRows, receiptValues(r))
	}
	for _, op := range []struct {
		table, cols string
		rows        [][]any
	}{
		{"dex_sky_state", dexAnchorColumns + ",module_id,tin,tout,buf,dai_cash,usdc_pocket_cash,pocket_allowance,vat_live,dai_join_live,dai_join_ward,usds_join_ward,identity_ok,state_complete,reason", skyRows},
		{"dex_route_quote", dexQuoteColumns, quoteRows}, {"dex_log", dexLogColumns, logRows}, {"dex_tx_receipt", dexReceiptColumns, receiptRows},
	} {
		if e := c.dexInsert(ctx, op.table, op.cols, op.rows); e != nil {
			return fmt.Errorf("%s: %w", op.table, e)
		}
	}
	return c.WriteDEXBlock(ctx, b.Block)
}
func (c *Client) WriteDEXBlock(ctx context.Context, b dex.Block) error {
	if !validAnchor(b.Anchor) || b.Revision == 0 {
		return errors.New("invalid DEX block revision")
	}
	return c.dexInsert(ctx, "dex_block", dexBlockColumns, [][]any{blockValues(b)})
}
func (c *Client) WriteDEXReceipts(ctx context.Context, rr []dex.Receipt) error {
	var rows [][]any
	for _, r := range rr {
		if !validAnchor(r.Anchor) || !dex.ValidUint(r.Value, 256) || !dex.ValidUint(r.GasPrice, 256) {
			return errors.New("invalid DEX receipt")
		}
		rows = append(rows, receiptValues(r))
	}
	return c.dexInsert(ctx, "dex_tx_receipt", dexReceiptColumns, rows)
}
func (c *Client) CompleteDEXLogs(ctx context.Context, b dex.Block, logs []dex.Log) error {
	if b.LogCoverage != "complete" || b.LogCount != uint32(len(logs)) || b.LogMembers != dex.LogDigest(logs) {
		return errors.New("invalid log completion")
	}
	var rows [][]any
	for _, l := range logs {
		if !sameBatch(l.Anchor, b.Anchor) || !validAnchor(l.Anchor) || l.Removed {
			return errors.New("invalid recovered log")
		}
		rows = append(rows, logValues(l))
	}
	if e := c.dexInsert(ctx, "dex_log", dexLogColumns, rows); e != nil {
		return e
	}
	return c.WriteDEXBlock(ctx, b)
}
