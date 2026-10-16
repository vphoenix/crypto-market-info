package mysql

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func TestMySQLKeeperCaptureTimeIndexAndMicrosecondBounds(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	if e := c.InitKeeperSchema(ctx); e != nil {
		t.Fatal(e)
	}
	// Reinstalling an already installed schema must also verify the additive
	// index, without changing the pinned mapping hash.
	if e := InitSchema(ctx, c.db, c.family, c.database); e != nil {
		t.Fatal(e)
	}
	base := keeperFixture(t).Capture
	lo := base.CaptureStartedAt.UTC().Truncate(time.Microsecond)
	hi := lo.Add(2 * time.Microsecond)
	all, want := []keeper.Capture{}, []keeper.Capture{}
	for i := range 24 {
		cap := base
		cap.CaptureId = uuid.New()
		cap.CaptureStartedAt = lo.Add(time.Duration(i%6-2) * time.Microsecond)
		cap.Committed = i%3 != 0 // Time queries also retain uncommitted declarations.
		all = append(all, cap)
		if !cap.CaptureStartedAt.Before(lo) && cap.CaptureStartedAt.Before(hi) {
			want = append(want, cap)
		}
	}
	insertCaptureFixtures(t, c, "jl_keeper_capture", all)
	sort.Slice(want, func(i, j int) bool {
		if want[i].CaptureStartedAt.Equal(want[j].CaptureStartedAt) {
			return want[i].CaptureId.String() < want[j].CaptureId.String()
		}
		return want[i].CaptureStartedAt.Before(want[j].CaptureStartedAt)
	})
	got, e := c.KeeperCaptures(ctx, lo, hi)
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("time index changed fields, time bounds or order: got=%d want=%d error=%v", len(got), len(want), e)
	}
	if _, e = c.db.ExecContext(ctx, "ALTER TABLE "+c.table("jl_keeper_capture")+" DROP INDEX "+keeperCaptureTimeIndex); e != nil {
		t.Fatal(e)
	}
	if e = c.InitKeeperSchema(ctx); e == nil {
		t.Fatal("collector accepted missing administrative index")
	}
	if e = InitSchema(ctx, c.db, c.family, c.database); e != nil {
		t.Fatal(e)
	}
	if _, e = c.db.ExecContext(ctx, "ALTER TABLE "+c.table("jl_keeper_capture")+" DROP INDEX "+keeperCaptureTimeIndex+", ADD INDEX "+keeperCaptureTimeIndex+" (capture_id)"); e != nil {
		t.Fatal(e)
	}
	if e = InitSchema(ctx, c.db, c.family, c.database); e == nil {
		t.Fatal("administration accepted a conflicting index shape")
	}
}
