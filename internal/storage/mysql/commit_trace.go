package mysql

import (
	"context"
	"time"
)

type CommitTrace struct {
	ConnectionID      uint64 `json:"connection_id"`
	RollbackConfirmed bool
	Committed         bool
	Lock, SQL, Commit time.Duration
	Bulk              map[string]*BulkTrace
}

type BulkTrace struct {
	Rows                        int `json:"rows"`
	PrepareMS, ReadMS, InsertMS float64
}

func bulkTrace(ctx context.Context, name string) *BulkTrace {
	t, _ := ctx.Value(commitTraceKey{}).(*CommitTrace)
	if t == nil {
		return nil
	}
	if t.Bulk == nil {
		t.Bulk = map[string]*BulkTrace{}
	}
	if t.Bulk[name] == nil {
		t.Bulk[name] = &BulkTrace{}
	}
	return t.Bulk[name]
}

type commitTraceKey struct{}

func WithCommitTrace(ctx context.Context, t *CommitTrace) context.Context {
	return context.WithValue(ctx, commitTraceKey{}, t)
}
func (c *Client) AttemptGone(ctx context.Context, id uint64) (bool, error) {
	if id == 0 {
		return true, nil
	}
	var count int
	e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID=?", id).Scan(&count)
	return count == 0, e
}
