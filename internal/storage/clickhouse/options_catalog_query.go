package clickhouse

import (
	"context"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"time"
)

func (c *Client) HasOptionsCatalog(ctx context.Context) (bool, error) {
	var n uint64
	err := c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name='options_collection_plan'", c.database).Scan(&n)
	return n == 1, err
}

type CatalogPlanSnapshot struct {
	Plan                                    options.CollectionPlan
	MinuteTime                              time.Time
	ExpectedBooks, ValidBooks, MissingBooks int
	MissingRuns                             []uuid.UUID
	Runs                                    []options.LiveEnvelope
}

// Every expected owner is queried. A missing shard is an explicit gap rather
// than being hidden by selecting the most recently created run.
func (c *Client) LoadOptionsPlanSnapshot(ctx context.Context, at time.Time) (CatalogPlanSnapshot, error) {
	var out CatalogPlanSnapshot
	out.MinuteTime = at.UTC().Truncate(time.Minute)
	p, err := c.OptionsPlanAt(ctx, out.MinuteTime)
	if err != nil {
		return out, err
	}
	out.Plan = p
	out.ExpectedBooks = len(p.InstrumentIDs)
	seen := map[uuid.UUID]bool{}
	for _, id := range p.RunIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		e, err := c.LoadOptionsMinute(ctx, id, out.MinuteTime)
		if err != nil {
			out.MissingRuns = append(out.MissingRuns, id)
			for _, owner := range p.RunIDs {
				if owner == id {
					out.MissingBooks++
				}
			}
			continue
		}
		out.Runs = append(out.Runs, e)
		for _, b := range e.Books {
			if b.Quality[at.Second()].ReplayValid {
				out.ValidBooks++
			}
		}
	}
	return out, nil
}
