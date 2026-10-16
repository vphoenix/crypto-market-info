package mysql

import (
	"github.com/vphoenix/crypto-market-info/internal/dex"
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
