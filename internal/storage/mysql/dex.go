package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func blockFrom(r record) dex.Block {
	return dex.Block{Anchor: anchorFrom(r), Parent: hashValue(r, "parent_hash"), BaseFee: bigValue(r, "base_fee_wei"), Miner: addressValue(r, "fee_recipient"), ReceivedAt: r["received_at"].(time.Time), AvailableAt: r["available_at"].(time.Time), Capture: r["capture_mode"].(string), Canonical: r["canonical"].(bool), Finality: r["finality"].(string), Revision: r["revision"].(uint64), LogCoverage: r["log_coverage"].(string), ReceiptCoverage: r["receipt_coverage"].(string), QuoteCoverage: r["quote_coverage"].(string), ExpectedQuotes: r["expected_quotes"].(uint32), ActualQuotes: r["actual_quotes"].(uint32), LogCount: r["log_count"].(uint32), ReceiptCount: r["receipt_count"].(uint32), QuoteMembers: hashValue(r, "quote_members"), LogMembers: hashValue(r, "log_members"), ReceiptMembers: hashValue(r, "receipt_members"), Committed: r["committed"].(bool)}
}
func quoteFrom(r record) dex.Quote {
	return dex.Quote{Anchor: anchorFrom(r), ID: hashValue(r, "quote_id"), Role: r["quote_role"].(string), Route: r["route_id"].(string), Mode: r["quote_mode"].(string), TokenIn: addressValue(r, "token_in"), TokenOut: addressValue(r, "token_out"), Requested: bigValue(r, "requested_amount_raw"), AmountIn: bigValue(r, "amount_in_raw"), AmountOut: bigValue(r, "amount_out_raw"), DustDAI: bigValue(r, "dust_dai"), DustUSDS: bigValue(r, "dust_usds"), V3InToken: addressValue(r, "v3_input_token"), V3OutToken: addressValue(r, "v3_output_token"), V3In: bigValue(r, "v3_input_raw"), V3Out: bigValue(r, "v3_output_raw"), SqrtAfter: bigValue(r, "sqrt_price_after_x96"), TicksCrossed: r["ticks_crossed"].(uint32), GasEstimate: bigValue(r, "quoter_gas_estimate"), Status: r["status"].(string), Reason: r["reason"].(string), AvailableAt: r["available_at"].(time.Time)}
}
func (c *Client) dexBlocks(ctx context.Context, q executor, where string, args ...any) ([]dex.Block, error) {
	return c.dexBlocksUsingIndex(ctx, q, "", where, args...)
}
func (c *Client) dexBlocksUsingIndex(ctx context.Context, q executor, index, where string, args ...any) ([]dex.Block, error) {
	t, e := c.mapping("dex_block")
	if e != nil {
		return nil, e
	}
	qualified := c.table(t.Name)
	if index != "" {
		if !identifier.MatchString(index) {
			return nil, fmt.Errorf("invalid DEX query index")
		}
		qualified += " FORCE INDEX (`" + index + "`)"
	}
	rs, e := readRecords(ctx, q, qualified, t, where, args...)
	if e != nil {
		return nil, e
	}
	out := make([]dex.Block, len(rs))
	for i, r := range rs {
		out[i] = blockFrom(r)
	}
	return out, nil
}
func (c *Client) DEXBlocks(ctx context.Context, from, to time.Time) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.db, "WHERE block_time>=? AND block_time<? ORDER BY block_number,manifest_hash,block_hash", from.UTC(), to.UTC())
}
func (c *Client) DEXBlockRange(ctx context.Context, m dex.Hash, from, to uint64) ([]dex.Block, error) {
	return c.dexBlocks(ctx, c.db, "WHERE chain_id=1 AND manifest_hash=? AND block_number>=? AND block_number<=? ORDER BY block_number,block_hash", m[:], from, to)
}
func (c *Client) DEXLastBlock(ctx context.Context, m dex.Hash, finalized bool) (*dex.Block, error) {
	where := "WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1"
	if finalized {
		where += " AND finality='finalized'"
	}
	index := "cmi_dex_committed"
	if finalized {
		index = "cmi_dex_finality"
	}
	rows, e := c.dexBlocksUsingIndex(ctx, c.db, index, where+" ORDER BY block_number DESC LIMIT 1", m[:])
	if e != nil || len(rows) == 0 {
		return nil, e
	}
	if e = c.verifyDEXMembers(ctx, c.db, rows[0]); e != nil {
		return nil, e
	}
	return &rows[0], nil
}
func (c *Client) DEXPendingReceiptBlocks(ctx context.Context, m dex.Hash) ([]dex.Block, error) {
	return c.dexBlocksUsingIndex(ctx, c.db, "cmi_dex_pending", "WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1 AND log_coverage='complete' AND receipt_coverage!='complete' ORDER BY block_number DESC LIMIT 100", m[:])
}
func (c *Client) DEXUnfinalizedBlocks(ctx context.Context, m dex.Hash, before uint64) ([]dex.Block, error) {
	return c.dexBlocksUsingIndex(ctx, c.db, "cmi_dex_finality", "WHERE chain_id=1 AND manifest_hash=? AND block_number<? AND committed=1 AND finality NOT IN ('finalized','orphaned') ORDER BY block_number LIMIT 100", m[:], before)
}
func (c *Client) DEXPendingLogBlocks(ctx context.Context, m dex.Hash) ([]dex.Block, error) {
	return c.dexBlocksUsingIndex(ctx, c.db, "cmi_dex_pending", "WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1 AND log_coverage!='complete' ORDER BY block_number LIMIT 100", m[:])
}
func (c *Client) DEXGapStart(ctx context.Context, m dex.Hash) (uint64, error) {
	// One ordered, covering-index read retains a single statement snapshot.
	// Window functions materialize/sort the whole history on each live poll;
	// streaming just heights avoids that temporary work and bounds Go memory.
	rows, err := c.db.QueryContext(ctx, "SELECT block_number FROM "+c.table("dex_block")+" FORCE INDEX (`cmi_dex_committed`) WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1 ORDER BY block_number", m[:])
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var previous, gap uint64
	havePrevious := false
	for rows.Next() {
		var height uint64
		if err = rows.Scan(&height); err != nil {
			return 0, err
		}
		if havePrevious && height < previous {
			return 0, fmt.Errorf("unstable DEX coverage ordering")
		}
		// Subtraction avoids overflow at the UInt64 upper boundary; duplicate
		// canonical heights retain the original query's adjacency semantics.
		if havePrevious && gap == 0 && height > previous && height-previous > 1 {
			gap = previous + 1
		}
		previous, havePrevious = height, true
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	return gap, nil
}
func (c *Client) DEXLogs(ctx context.Context, b dex.Block) ([]dex.Log, error) {
	if !b.Committed {
		return nil, fmt.Errorf("DEX block not committed")
	}
	return c.dexLogs(ctx, c.db, "WHERE chain_id=1 AND manifest_hash=? AND block_number=? AND block_hash=? AND batch_id=? ORDER BY log_index", b.Manifest[:], b.Number, b.Hash[:], b.Batch[:])
}
func (c *Client) DEXReceipts(ctx context.Context, b dex.Block) ([]dex.Receipt, error) {
	return c.dexReceipts(ctx, c.db, "WHERE chain_id=1 AND block_hash=? ORDER BY tx_hash", b.Hash[:])
}
func (c *Client) DEXReceiptHashes(ctx context.Context, b dex.Block) (map[dex.Hash]bool, error) {
	rows, e := c.DEXReceipts(ctx, b)
	if e != nil {
		return nil, e
	}
	out := map[dex.Hash]bool{}
	for _, r := range rows {
		out[r.TxHash] = true
	}
	return out, nil
}
func (c *Client) DEXQuotes(ctx context.Context, blocks []dex.Block) (map[dex.Hash][]dex.Quote, error) {
	t, e := c.mapping("dex_route_quote")
	if e != nil {
		return nil, e
	}
	out := map[dex.Hash][]dex.Quote{}
	for _, b := range blocks {
		if !b.Committed {
			return nil, fmt.Errorf("DEX block not committed")
		}
		rs, e := readRecords(ctx, c.db, c.table(t.Name), t, "WHERE chain_id=1 AND manifest_hash=? AND block_number=? AND block_hash=? AND batch_id=? ORDER BY route_id", b.Manifest[:], b.Number, b.Hash[:], b.Batch[:])
		if e != nil {
			return nil, e
		}
		qs := make([]dex.Quote, len(rs))
		for i, r := range rs {
			qs[i] = quoteFrom(r)
		}
		if uint32(len(qs)) != b.ActualQuotes || dex.QuoteDigest(qs) != b.QuoteMembers {
			return nil, fmt.Errorf("DEX quote members mismatch")
		}
		out[b.Batch] = qs
	}
	return out, nil
}

func validAnchor(a dex.Anchor) bool {
	return a.ChainID == 1 && a.Number > 0 && a.Hash != (dex.Hash{}) && a.Manifest != (dex.Hash{}) && a.Batch != (dex.Hash{}) && a.Payload != (dex.Hash{}) && !a.Time.IsZero()
}
func sameBatch(a, b dex.Anchor) bool {
	return a.ChainID == b.ChainID && a.Number == b.Number && a.Hash == b.Hash && a.Manifest == b.Manifest && a.Batch == b.Batch && a.Time.Equal(b.Time)
}
func (c *Client) putDEXBlock(ctx context.Context, tx *sql.Tx, b dex.Block) error {
	if !validAnchor(b.Anchor) || b.Revision == 0 {
		return fmt.Errorf("invalid DEX block revision")
	}
	t, e := c.mapping("dex_block")
	if e != nil {
		return e
	}
	r, e := namedRecord(dexBlockColumns, blockValues(b))
	if e != nil {
		return e
	}
	if _, e = rowHash(t, r); e != nil {
		return e
	}
	where, args, e := rowPredicate(t, r)
	if e != nil {
		return e
	}
	old, e := readRecords(ctx, tx, c.table(t.Name), t, "WHERE "+where+" FOR UPDATE", args...)
	if e != nil {
		return e
	}
	if len(old) > 1 {
		return contracts.ErrConflict
	}
	if len(old) == 1 {
		prev := blockFrom(old[0])
		if b.Revision < prev.Revision {
			return nil
		}
		if b.Revision == prev.Revision {
			a, _ := rowHash(t, old[0])
			z, _ := rowHash(t, r)
			if a == z {
				return nil
			}
			return contracts.ErrConflict
		}
		if prev.Finality == "finalized" && (!b.Canonical || b.Finality != "finalized") {
			return contracts.ErrConflict
		}
		if prev.Hash != b.Hash || prev.Number != b.Number || prev.Time != b.Time || prev.Manifest != b.Manifest {
			return contracts.ErrConflict
		}
		if _, e = tx.ExecContext(ctx, "DELETE FROM "+c.table(t.Name)+" WHERE "+where, args...); e != nil {
			return e
		}
	}
	return insertRecord(ctx, tx, c.table(t.Name), t, r)
}
func (c *Client) WriteDEXBlock(ctx context.Context, b dex.Block) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		if e := c.verifyDEXMembers(ctx, tx, b); e != nil {
			return e
		}
		return c.putDEXBlock(ctx, tx, b)
	})
}
func (c *Client) WriteDEXReceipts(ctx context.Context, rows []dex.Receipt) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, r := range rows {
			if e := c.putSharedReceipt(ctx, tx, r); e != nil {
				return e
			}
		}
		return nil
	})
}
func (c *Client) CompleteDEXLogs(ctx context.Context, b dex.Block, logs []dex.Log) error {
	if b.LogCoverage != "complete" || uint32(len(logs)) != b.LogCount || dex.LogDigest(logs) != b.LogMembers {
		return fmt.Errorf("DEX log completion invalid")
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, l := range logs {
			if !sameBatch(l.Anchor, b.Anchor) || !validAnchor(l.Anchor) || l.Removed {
				return fmt.Errorf("DEX log anchor invalid")
			}
			if e := c.putValues(ctx, tx, "dex_log", dexLogColumns, logValues(l)); e != nil {
				return e
			}
		}
		if e := c.verifyDEXMembers(ctx, tx, b); e != nil {
			return e
		}
		return c.putDEXBlock(ctx, tx, b)
	})
}
func (c *Client) WriteDEXBatch(ctx context.Context, b dex.Batch) error {
	if !validAnchor(b.Block.Anchor) || !b.Block.Committed || !dex.ValidUint(b.Block.BaseFee, 256) || b.Block.ActualQuotes != uint32(len(b.Quotes)) || b.Block.LogCount != uint32(len(b.Logs)) || b.Block.ReceiptCount != uint32(len(b.Receipts)) || dex.QuoteDigest(b.Quotes) != b.Block.QuoteMembers || dex.LogDigest(b.Logs) != b.Block.LogMembers || dex.ReceiptDigest(b.Receipts) != b.Block.ReceiptMembers {
		return fmt.Errorf("DEX envelope invalid")
	}
	prepared, err := prepareDEXFactRows(b)
	if err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, group := range []struct {
			name string
			rows []record
		}{
			{"dex_sky_state", prepared.sky},
			{"dex_route_quote", prepared.quotes},
			{"dex_log", prepared.logs},
		} {
			if len(group.rows) == 0 {
				continue
			}
			mapping, e := c.mapping(group.name)
			if e != nil {
				return e
			}
			if e = putImmutableRows(ctx, tx, c.table(group.name), mapping, group.rows); e != nil {
				return e
			}
		}
		// Shared receipts retain their cross-batch conflict semantics.
		for _, receipt := range b.Receipts {
			if e := c.putSharedReceipt(ctx, tx, receipt); e != nil {
				return e
			}
		}
		if e := c.verifyDEXMembers(ctx, tx, b.Block); e != nil {
			return e
		}
		return c.putDEXBlock(ctx, tx, b.Block)
	})
}

func (c *Client) verifyDEXMembers(ctx context.Context, q executor, b dex.Block) error {
	if !b.Committed {
		return fmt.Errorf("DEX block uncommitted")
	}
	t, e := c.mapping("dex_route_quote")
	if e != nil {
		return e
	}
	records, e := readRecords(ctx, q, c.table(t.Name), t, "WHERE chain_id=1 AND manifest_hash=? AND block_number=? AND block_hash=? AND batch_id=? ORDER BY route_id", b.Manifest[:], b.Number, b.Hash[:], b.Batch[:])
	if e != nil {
		return e
	}
	quotes := []dex.Quote{}
	for _, r := range records {
		quotes = append(quotes, quoteFrom(r))
	}
	if uint32(len(quotes)) != b.ActualQuotes || dex.QuoteDigest(quotes) != b.QuoteMembers {
		return fmt.Errorf("DEX quote membership incomplete")
	}
	logs, e := c.dexLogs(ctx, q, "WHERE chain_id=1 AND manifest_hash=? AND block_number=? AND block_hash=? AND batch_id=? ORDER BY log_index", b.Manifest[:], b.Number, b.Hash[:], b.Batch[:])
	if e != nil {
		return e
	}
	if uint32(len(logs)) != b.LogCount || dex.LogDigest(logs) != b.LogMembers {
		return fmt.Errorf("DEX log membership incomplete")
	}
	if b.ReceiptCoverage == "complete" {
		txIDs := map[dex.Hash]bool{}
		for _, l := range logs {
			txIDs[l.TxHash] = true
		}
		all, e := c.dexReceipts(ctx, q, "WHERE chain_id=1 AND block_hash=? ORDER BY tx_hash", b.Hash[:])
		if e != nil {
			return e
		}
		receipts := []dex.Receipt{}
		for _, r := range all {
			if txIDs[r.TxHash] {
				receipts = append(receipts, r)
			}
		}
		if uint32(len(receipts)) != b.ReceiptCount || dex.ReceiptDigest(receipts) != b.ReceiptMembers {
			return fmt.Errorf("DEX receipt membership incomplete")
		}
	}
	return nil
}

// DEXRuntimeBlock is a bounded, exact-key recovery read. It does not claim that
// a missing row proves a timed-out transaction has finished.
func (c *Client) DEXRuntimeBlock(ctx context.Context, want dex.Block) (dex.Block, bool, error) {
	if want.ChainID != 1 || want.Manifest == (dex.Hash{}) || want.Number == 0 || want.Hash == (dex.Hash{}) {
		return dex.Block{}, false, contracts.ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rows, err := c.dexBlocks(ctx, c.db, "WHERE chain_id=? AND manifest_hash=? AND block_number=? AND block_hash=?", want.ChainID, want.Manifest[:], want.Number, want.Hash[:])
	if err != nil || len(rows) == 0 {
		return dex.Block{}, false, err
	}
	if len(rows) != 1 || !rows[0].Committed {
		return dex.Block{}, false, contracts.ErrConflict
	}
	if err = c.verifyDEXMembers(ctx, c.db, rows[0]); err != nil {
		return dex.Block{}, false, err
	}
	return rows[0], true, nil
}

// Conversion alone pages the complete applicable registration range. The
// cursor carries the numeric height and exact binary hash, without timestamp
// formatting or a whole-history read on normal startup.
func (c *Client) DEXRuntimePage(ctx context.Context, m dex.Hash, after []byte, limit int) ([]dex.Block, []byte, error) {
	if m == (dex.Hash{}) || len(after) != 0 && len(after) != 40 || limit < 1 || limit > 500 {
		return nil, nil, fmt.Errorf("invalid DEX conversion page")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	where := "WHERE chain_id=1 AND manifest_hash=? AND committed=1"
	args := []any{m[:]}
	if len(after) > 0 {
		n := binary.BigEndian.Uint64(after[:8])
		where += " AND (block_number>? OR (block_number=? AND block_hash>?))"
		args = append(args, n, n, after[8:])
	}
	where += fmt.Sprintf(" ORDER BY block_number,block_hash LIMIT %d", limit)
	rows, err := c.dexBlocksUsingIndex(ctx, c.db, "PRIMARY", where, args...)
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	last := rows[len(rows)-1]
	cursor := make([]byte, 40)
	binary.BigEndian.PutUint64(cursor[:8], last.Number)
	copy(cursor[8:], last.Hash[:])
	return rows, cursor, nil
}
