package lst

import (
	"context"
	"fmt"
)

func PreparedStartupRecovery(ctx context.Context, store Store, m Manifest, state string) (map[string]any, error) {
	c := Collector{Manifest: m, Store: store, StateDir: state, LiveLogsFromBlock: 26113044}
	p, e := c.logProgress(ctx)
	if e != nil {
		return nil, e
	}
	if p.Start != 26113044 || p.Next <= p.Start {
		return nil, fmt.Errorf("database continuous prefix missing")
	}
	s, e := c.restoreFollowups(ctx)
	if e != nil {
		return nil, e
	}
	return map[string]any{"start": p.Start, "next": p.Next, "range_size": p.RangeSize, "followup_seeds": len(s.Seeds), "copied_state_only": true, "network_readers": 0, "production_writes": 0}, nil
}
