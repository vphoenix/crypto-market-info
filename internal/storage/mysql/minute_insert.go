package mysql

import (
	"context"
	"errors"
)

type minuteInsertKey struct{}

var errMinuteInsertCollision = errors.New("minute fact insert needs complete retry checks")

// These tables have a unique full logical primary key and cannot represent
// historical multiplicity. An ordinary INSERT checks new identities; a 1062
// requires a full transaction rollback followed by the original content checks.
// Publication and metadata tables always retain their complete occurrence scan.
func insertNewMinuteFacts(ctx context.Context, table Table) bool {
	if table.Multiplicity || ctx.Value(minuteInsertKey{}) != true {
		return false
	}
	switch table.Name {
	case "order_book_minute", "order_book_second_delta",
		"derivative_book_minute", "derivative_book_second_delta",
		"derivative_book_quality_minute", "options_catalog_quality_evidence_minute", qualityMinuteV2, qualityMember:
		return true
	}
	return false
}
