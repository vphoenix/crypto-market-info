package mysql

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

func insertCaptureFixtures[T any](t *testing.T, c *Client, name string, models []T) {
	t.Helper()
	mapping, e := c.mapping(name)
	if e != nil {
		t.Fatal(e)
	}
	for start := 0; start < len(models); start += 500 {
		end := min(start+500, len(models))
		rows := make([]MigrationRow, end-start)
		for i, model := range models[start:end] {
			r, e := modelRecord(model)
			if e != nil {
				t.Fatal(e)
			}
			rows[i] = MigrationRow(r)
		}
		tx, e := c.db.BeginTx(context.Background(), nil)
		if e != nil {
			t.Fatal(e)
		}
		e = insertMigrationBatch(context.Background(), tx, c.table(name), mapping, rows, make([]uint64, len(rows)))
		if e != nil {
			tx.Rollback()
			t.Fatal(e)
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestMySQLCapturePagingPreservesAllRowsFiltersAndOrder(t *testing.T) {
	ctx := context.Background()
	t.Run("hashed_across", func(t *testing.T) {
		c := familyClient(t, "crypto_market_info_across")
		base := acrossFixture(t).Capture
		all, want := []across.Capture{}, []across.Capture{}
		for i := range 1050 {
			cap := base
			cap.CaptureId = across.ID(i)
			cap.StartedAt = base.StartedAt.Add(time.Duration(i%11) * time.Microsecond)
			cap.Committed = i%9 != 0
			if i%3 == 0 {
				cap.ManifestHash = across.ID("other_manifest")
			} else {
				want = append(want, cap)
			}
			all = append(all, cap)
		}
		insertCaptureFixtures(t, c, "across_capture", all)
		sort.Slice(want, func(i, j int) bool {
			if want[i].StartedAt.Equal(want[j].StartedAt) {
				return want[i].CaptureId < want[j].CaptureId
			}
			return want[i].StartedAt.Before(want[j].StartedAt)
		})
		got, e := c.AcrossCaptures(ctx, base.ManifestHash)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("paged captures changed fields, filter, count or order: rows=%d want=%d error=%v", len(got), len(want), e)
		}
	})
	t.Run("uuid_lst", func(t *testing.T) {
		c := familyClient(t, "crypto_market_info_lst")
		batch := lstStorageMarket()
		if e := batch.Seal(); e != nil {
			t.Fatal(e)
		}
		base := batch.Capture
		all, want := []lst.Capture{}, []lst.Capture{}
		for i := range 1050 {
			cap := base
			cap.CaptureId = uuid.New()
			cap.StartedAt = base.StartedAt.Add(time.Duration(i%7) * time.Microsecond)
			cap.Committed = i%10 != 0
			if i%3 == 0 {
				cap.ManifestHash = across.ID("other_manifest")
			} else if cap.Committed {
				want = append(want, cap)
			}
			all = append(all, cap)
		}
		insertCaptureFixtures(t, c, "lst_capture", all)
		sort.Slice(want, func(i, j int) bool {
			if want[i].StartedAt.Equal(want[j].StartedAt) {
				return want[i].CaptureId.String() < want[j].CaptureId.String()
			}
			return want[i].StartedAt.Before(want[j].StartedAt)
		})
		got, e := c.LSTCaptures(ctx, base.ManifestHash)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("paged captures changed fields, filter, count or order: rows=%d want=%d error=%v", len(got), len(want), e)
		}
	})
	t.Run("composite_reserve", func(t *testing.T) {
		c := familyClient(t, "crypto_market_info_reserve")
		base := reserveFixture(7).Capture
		all, want := []reserve.Capture{}, []reserve.Capture{}
		for i := range 1050 {
			cap := base
			cap.CaptureId = across.ID(i / 3)
			cap.BatchId = across.ID(i)
			cap.ToBlock = uint64(i % 13)
			if i%3 == 0 {
				cap.ManifestHash = across.ID("other_manifest")
			} else {
				want = append(want, cap)
			}
			all = append(all, cap)
		}
		insertCaptureFixtures(t, c, "reserve_capture", all)
		sort.Slice(want, func(i, j int) bool {
			if want[i].ToBlock == want[j].ToBlock {
				return want[i].BatchId < want[j].BatchId
			}
			return want[i].ToBlock < want[j].ToBlock
		})
		var readsBefore uint64
		if e := c.db.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM performance_schema.session_status WHERE VARIABLE_NAME='Handler_read_next'").Scan(&readsBefore); e != nil {
			t.Fatal(e)
		}
		got, e := c.ReserveCaptures(ctx, base.ManifestHash)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("paged captures changed fields, filter, count or order: rows=%d want=%d error=%v", len(got), len(want), e)
		}
		var readsAfter uint64
		if e := c.db.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM performance_schema.session_status WHERE VARIABLE_NAME='Handler_read_next'").Scan(&readsAfter); e != nil {
			t.Fatal(e)
		}
		if read := readsAfter - readsBefore; read > uint64(len(want)+10) {
			t.Fatalf("capture cursor rescanned earlier pages: read=%d returned=%d", read, len(want))
		}
	})
}
