package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

const legacyKeeperEnrichmentQuery = "SELECT p.capture_id FROM %s p LEFT JOIN (SELECT parent_capture_id FROM %s WHERE committed=1 AND capture_kind='enrichment' AND config_hash=? AND parent_capture_id IS NOT NULL GROUP BY parent_capture_id HAVING SUM(status='complete')>0 OR COUNT(*)>=3 OR MAX(available_at)>?) done ON done.parent_capture_id=p.capture_id WHERE p.committed=1 AND p.capture_kind='event_index' AND p.status='complete' AND p.config_hash=? AND p.capture_mode!='bootstrap' AND done.parent_capture_id IS NULL ORDER BY p.capture_mode IN ('live','catchup') DESC,p.requested_from,p.capture_started_at,p.capture_id LIMIT 1"

func TestKeeperEnrichmentPriorityMatchesLegacyWithoutGlobalSort(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	base := keeperFixture(t).Capture
	now := time.Now().UTC().Truncate(time.Microsecond)
	config := base.ConfigHash
	otherConfig := keeper.Hash([]byte("other enrichment config"))
	rows := []keeper.Capture{}
	parents := map[uuid.UUID]keeper.Capture{}
	modes := []string{"live", "catchup", "backfill", "bootstrap", "other", ""}
	for n := range 768 {
		p := base
		p.CaptureId = uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("enrichment-parent-%d", n)))
		p.CaptureKind = "event_index"
		p.CaptureMode = modes[n%len(modes)]
		p.EventKind = "RentResource"
		p.CaptureStartedAt = now.Add(-time.Hour).Add(time.Duration(n%19) * time.Microsecond)
		lo := now.Add(-2 * time.Hour).Add(time.Duration(n%23) * time.Microsecond)
		hi := lo.Add(time.Minute)
		p.RequestedFrom = &lo
		p.RequestedTo = &hi
		if n%13 == 0 {
			p.RequestedFrom = nil
		} // Preserve legacy NULL-first ordering.
		if n%17 == 0 {
			p.Committed = false
		}
		if n%31 == 0 {
			p.Status = "partial"
		}
		if n >= 750 {
			p.ConfigHash = otherConfig
			p.CaptureMode = "backfill"
		}
		parents[p.CaptureId] = p
		rows = append(rows, p)
		for j := range n % 6 {
			child := p
			child.CaptureId = uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("enrichment-child-%d-%d", n, j)))
			child.CaptureKind = "enrichment"
			child.ParentCaptureId = &p.CaptureId
			child.Status = "partial"
			child.Committed = true
			child.AvailableAt = now.Add(-30 * time.Minute).Add(time.Duration(n%3-1) * time.Microsecond)
			if n%7 == 0 && j == 0 {
				child.Status = "complete"
			}
			if n%11 == 0 {
				child.ConfigHash = keeper.Hash([]byte("irrelevant child config"))
			}
			if n%19 == 0 {
				child.Committed = false
			}
			if n%29 == 0 {
				child.CaptureKind = "events"
			}
			rows = append(rows, child)
		}
	}
	insertCaptureFixtures(t, c, "jl_keeper_capture", rows)
	oldQuery := fmt.Sprintf(legacyKeeperEnrichmentQuery, c.table("jl_keeper_capture"), c.table("jl_keeper_capture"))
	for _, cfg := range []string{config, otherConfig, keeper.Hash([]byte("missing config"))} {
		for _, at := range []time.Time{now.Add(-time.Hour), now.Add(-time.Microsecond), now, now.Add(time.Microsecond), now.Add(30 * time.Minute)} {
			var raw []byte
			err := c.db.QueryRowContext(ctx, oldQuery, []byte(cfg), at.Add(-30*time.Minute), []byte(cfg)).Scan(&raw)
			if err != nil && err != sql.ErrNoRows {
				t.Fatal(err)
			}
			got, errNew := c.KeeperNextEnrichment(ctx, cfg, at)
			if errNew != nil {
				t.Fatal(errNew)
			}
			if err == sql.ErrNoRows {
				if got != nil {
					t.Fatal("empty result changed")
				}
				continue
			}
			want, err := uuid.FromBytes(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || got.CaptureId != want || !reflect.DeepEqual(*got, parents[want]) {
				t.Fatalf("selection/content changed: got=%v want=%v", got, want)
			}
		}
	}
	// Query order must be supplied by the existing administrative index, rather
	// than an external sort whose cost grows with all historical parents.
	for _, priority := range []bool{true, false} {
		rs, err := c.db.QueryContext(ctx, "EXPLAIN "+c.keeperNextEnrichmentQuery(priority), []byte(config), now.Add(-30*time.Minute), []byte(config))
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rs.Columns()
		for rs.Next() {
			values := make([]sql.NullString, len(cols))
			args := make([]any, len(cols))
			for n := range args {
				args[n] = &values[n]
			}
			if err = rs.Scan(args...); err != nil {
				rs.Close()
				t.Fatal(err)
			}
			for n, col := range cols {
				if col == "Extra" && strings.Contains(values[n].String, "Using filesort") {
					rs.Close()
					t.Fatal("priority query still sorts history", values[n].String)
				}
			}
		}
		err = rs.Err()
		rs.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
