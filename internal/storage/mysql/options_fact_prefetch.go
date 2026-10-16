package mysql

import (
	"context"

	"github.com/vphoenix/crypto-market-info/internal/options"
)

// Warm only the covering primary keys in the preparation pool. Cold backlog
// INSERTs otherwise read these pages while holding the generation fence. This
// returns no existence/content proof: CommitPrepared retains the full INSERT,
// rollback, conflict and publication protocol.
func (c *Client) prefetchOptionsFacts(ctx context.Context, live options.LiveEnvelope, catalog *options.CatalogEnvelope) error {
	if !c.prefetchMinuteIndexes {
		return nil
	}
	id := live.ID()
	if catalog != nil {
		id = catalog.ID()
	}
	compactID := digestBytes(id)
	names := []string{"derivative_book_minute", "derivative_book_quality_minute"}
	if catalog != nil {
		names = append(names, "options_catalog_quality_evidence_minute")
	}
	for _, name := range names {
		for offset := 0; offset < len(live.Books); offset += 500 {
			ids := make([]any, 0, min(500, len(live.Books)-offset))
			for _, book := range live.Books[offset:min(offset+500, len(live.Books))] {
				if name != "derivative_book_minute" || book.Minute != nil {
					ids = append(ids, book.InstrumentID)
				}
			}
			if len(ids) == 0 {
				continue
			}
			predicate := "`minute_time`=? AND `compact_batch_id`=?"
			args := []any{live.MinuteTime, compactID}
			if name == "derivative_book_minute" {
				// The anchor table retains the original 64-byte ASCII digest.
				predicate = "`minute_time`=? AND `batch_id`=?"
				args = []any{live.MinuteTime, id}
			} else if name == "options_catalog_quality_evidence_minute" {
				predicate = "`run_id`=? AND " + predicate
				args = []any{live.RunID[:], live.MinuteTime, compactID}
			}
			args = append(args, ids...)
			rows, err := c.db.QueryContext(ctx, "SELECT `instrument_id` FROM "+c.table(name)+" FORCE INDEX (PRIMARY) WHERE "+predicate+" AND `instrument_id` IN ("+placeholders(len(ids))+") ORDER BY `instrument_id` LIMIT 500", args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var instrument uint32
				if err = rows.Scan(&instrument); err != nil {
					rows.Close()
					return err
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
