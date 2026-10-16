package across

import (
	"context"
	"fmt"
)

func PreparedStartupRecovery(ctx context.Context, store Store, m Manifest) (map[string]any, error) {
	c := Collector{Manifest: m, Store: store}
	w, e := c.restore(ctx)
	if e != nil {
		return nil, e
	}
	if len(c.captures) == 0 || len(w.Cursors) != len(m.Chains) {
		return nil, fmt.Errorf("startup dependency/cursor set incomplete")
	}
	return map[string]any{"captures": len(c.captures), "pending_orders": len(w.Orders), "pending_receipts": len(c.receiptQueue), "covered_through": w.Cursors, "network_readers": 0, "production_writes": 0}, nil
}
