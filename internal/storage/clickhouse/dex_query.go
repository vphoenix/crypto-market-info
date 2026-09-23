package clickhouse

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"strings"
	"time"
)

// OpenDEXReader never bootstraps a database or executes DDL. Session readonly=1
// also protects optional report commands against accidental writes.
func OpenDEXReader(ctx context.Context, cfg Config) (*Client, error) { return openDEXReader(ctx, cfg) }
func anchorDest(a *dex.Anchor) []any {
	return []any{&a.ChainID, &a.Number, &a.Hash, &a.Manifest, &a.Batch, &a.Payload, &a.Time}
}
func blockDest(b *dex.Block) []any {
	return append(anchorDest(&b.Anchor), &b.Parent, &b.BaseFee, &b.Miner, &b.ReceivedAt, &b.AvailableAt, &b.Capture, &b.Canonical, &b.Finality, &b.Revision, &b.LogCoverage, &b.ReceiptCoverage, &b.QuoteCoverage, &b.ExpectedQuotes, &b.ActualQuotes, &b.LogCount, &b.ReceiptCount, &b.QuoteMembers, &b.LogMembers, &b.ReceiptMembers, &b.Committed)
}
func quoteDest(q *dex.Quote) []any {
	return append(anchorDest(&q.Anchor), &q.ID, &q.Role, &q.Route, &q.Mode, &q.TokenIn, &q.TokenOut, &q.Requested, &q.AmountIn, &q.AmountOut, &q.DustDAI, &q.DustUSDS, &q.V3InToken, &q.V3OutToken, &q.V3In, &q.V3Out, &q.SqrtAfter, &q.TicksCrossed, &q.GasEstimate, &q.Status, &q.Reason, &q.AvailableAt)
}

// State predicates belong OUTSIDE argMax: filtering canonical/finalized before
// aggregation resurrects old completed versions of orphaned blocks.
func (c *Client) dexLatestSQL(keyFilter string) string {
	cols := strings.Split(dexBlockColumns, ",")
	projection := make([]string, len(cols))
	for i, col := range cols {
		projection[i] = fmt.Sprintf("tupleElement(latest,%d) AS %s", i+1, col)
	}
	return "SELECT " + strings.Join(projection, ",") + " FROM (SELECT argMax(tuple(" + dexBlockColumns + "),revision) AS latest FROM " + c.table("dex_block") + " WHERE " + keyFilter + " GROUP BY chain_id,manifest_hash,block_number,block_hash)"
}
func (c *Client) dexBlocks(ctx context.Context, sql string, args ...any) ([]dex.Block, error) {
	rows, e := c.conn.Query(ctx, sql, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []dex.Block{}
	for rows.Next() {
		var b dex.Block
		if e = rows.Scan(blockDest(&b)...); e != nil {
			return nil, e
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
func (c *Client) DEXBlocks(ctx context.Context, from, to time.Time) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.dexLatestSQL("block_time >= ? AND block_time < ?")+" ORDER BY block_number,manifest_hash,block_hash", from, to)
}
func (c *Client) DEXBlockRange(ctx context.Context, m dex.Hash, from, to uint64) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.dexLatestSQL("manifest_hash = ? AND block_number >= ? AND block_number <= ?")+" ORDER BY block_number,block_hash", rawHash(m), from, to)
}
func (c *Client) DEXLastBlock(ctx context.Context, m dex.Hash, finalized bool) (*dex.Block, error) {
	sql := c.dexLatestSQL("manifest_hash = ?") + " WHERE canonical AND committed"
	if finalized {
		sql += " AND finality = 'finalized'"
	}
	sql += " ORDER BY block_number DESC LIMIT 1"
	bs, e := c.dexBlocks(ctx, sql, rawHash(m))
	if e != nil || len(bs) == 0 {
		return nil, e
	}
	return &bs[0], nil
}
func (c *Client) DEXQuotes(ctx context.Context, blocks []dex.Block) (map[dex.Hash][]dex.Quote, error) {
	out := map[dex.Hash][]dex.Quote{}
	if len(blocks) == 0 {
		return out, nil
	}
	ids := make([]string, 0, len(blocks))
	manifests := []string{}
	seen := map[dex.Hash]bool{}
	low, high := blocks[0].Number, blocks[0].Number
	for _, b := range blocks {
		ids = append(ids, rawHash(b.Batch))
		low = min(low, b.Number)
		high = max(high, b.Number)
		if !seen[b.Manifest] {
			seen[b.Manifest] = true
			manifests = append(manifests, rawHash(b.Manifest))
		}
	}
	// Use the existing sorting-key prefix. A batch-only predicate scans all
	// historical hash columns even when reading just one known block.
	rows, e := c.conn.Query(ctx, "SELECT "+dexQuoteColumns+" FROM "+c.table("dex_route_quote")+" FINAL WHERE chain_id = 1 AND manifest_hash IN (?) AND block_number >= ? AND block_number <= ? AND batch_id IN (?) ORDER BY block_number,route_id,requested_amount_raw", manifests, low, high, ids)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var q dex.Quote
		if e = rows.Scan(quoteDest(&q)...); e != nil {
			return nil, e
		}
		out[q.Batch] = append(out[q.Batch], q)
	}
	return out, rows.Err()
}
func (c *Client) DEXLogs(ctx context.Context, b dex.Block) ([]dex.Log, error) {
	rows, e := c.conn.Query(ctx, "SELECT "+dexLogColumns+" FROM "+c.table("dex_log")+" FINAL WHERE batch_id = ? ORDER BY log_index", rawHash(b.Batch))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []dex.Log{}
	for rows.Next() {
		var l dex.Log
		var topics []string
		var data string
		dest := append(anchorDest(&l.Anchor), &l.TxHash, &l.TxIndex, &l.Index, &l.Emitter, &topics, &data, &l.Event, &l.Removed)
		if e = rows.Scan(dest...); e != nil {
			return nil, e
		}
		for _, s := range topics {
			var h dex.Hash
			copy(h[:], s)
			l.Topics = append(l.Topics, h)
		}
		l.Data = []byte(data)
		out = append(out, l)
	}
	return out, rows.Err()
}

func (c *Client) DEXGapStart(ctx context.Context, m dex.Hash) (uint64, error) {
	sql := "SELECT ifNull(min(gap),0) FROM (SELECT block_number + 1 AS gap, leadInFrame(block_number,1,block_number+1) OVER (ORDER BY block_number ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next FROM (" + c.dexLatestSQL("manifest_hash = ?") + ") WHERE canonical AND committed) WHERE next > gap"
	var n uint64
	e := c.conn.QueryRow(ctx, sql, rawHash(m)).Scan(&n)
	return n, e
}
func (c *Client) DEXReceiptHashes(ctx context.Context, b dex.Block) (map[dex.Hash]bool, error) {
	rows, e := c.conn.Query(ctx, "SELECT tx_hash FROM "+c.table("dex_tx_receipt")+" FINAL WHERE chain_id=1 AND block_hash = ?", rawHash(b.Hash))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[dex.Hash]bool{}
	for rows.Next() {
		var h dex.Hash
		if e = rows.Scan(&h); e != nil {
			return nil, e
		}
		out[h] = true
	}
	return out, rows.Err()
}
func (c *Client) DEXPendingReceiptBlocks(ctx context.Context, m dex.Hash) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.dexLatestSQL("manifest_hash = ?")+" WHERE canonical AND committed AND log_coverage = 'complete' AND receipt_coverage != 'complete' ORDER BY block_number DESC LIMIT 100", rawHash(m))
}
func (c *Client) DEXUnfinalizedBlocks(ctx context.Context, m dex.Hash, before uint64) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.dexLatestSQL("manifest_hash = ? AND block_number < ?")+" WHERE committed AND finality NOT IN ('finalized','orphaned') ORDER BY block_number LIMIT 100", rawHash(m), before)
}
func (c *Client) DEXPendingLogBlocks(ctx context.Context, m dex.Hash) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.dexLatestSQL("manifest_hash = ?")+" WHERE canonical AND committed AND log_coverage != 'complete' ORDER BY block_number LIMIT 100", rawHash(m))
}
func (c *Client) DEXReceipts(ctx context.Context, b dex.Block) ([]dex.Receipt, error) {
	rows, e := c.conn.Query(ctx, "SELECT "+dexReceiptColumns+" FROM "+c.table("dex_tx_receipt")+" FINAL WHERE chain_id=1 AND block_hash = ? ORDER BY tx_hash", rawHash(b.Hash))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []dex.Receipt{}
	for rows.Next() {
		var r dex.Receipt
		var selector *string
		dest := append(anchorDest(&r.Anchor), &r.TxHash, &r.TxIndex, &r.From, &r.To, &r.HasTo, &r.Type, &r.Value, &selector, &r.CalldataHash, &r.Status, &r.GasUsed, &r.GasPrice, &r.LogCount, &r.ReceiptHash, &r.AvailableAt)
		if e = rows.Scan(dest...); e != nil {
			return nil, e
		}
		if selector != nil {
			r.Selector = []byte(*selector)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
