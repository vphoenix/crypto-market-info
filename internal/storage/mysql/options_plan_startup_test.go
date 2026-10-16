package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMySQLLatestPlanStartupAndSuccessorPreserveBoundaries(t *testing.T) {
	c := minimalOptionsPlanClient(t)
	ctx := context.Background()
	if _, err := c.LatestOptionsPlan(ctx); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("empty plan store", err)
	}
	_, envelope := catalogDBFixture(t, c)
	first, err := c.LoadOptionsPlan(ctx, envelope.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	// The newer plan deliberately sorts before the older random UUID.
	second.ID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	second.PreviousID, second.PreviousHash = first.ID, first.Hash()
	second.Revision++
	second.CreatedAt = first.CreatedAt.Add(time.Minute)
	second.EffectiveMinute = first.EffectiveMinute.Add(time.Minute)
	if err = c.WriteOptionsPlan(ctx, second); err != nil {
		t.Fatal("write successor", err)
	}
	latest, err := c.LatestOptionsPlan(ctx)
	if err != nil || latest.ID != second.ID {
		t.Fatal("latest plan must use effective time", latest.ID, err)
	}
	older, err := c.OptionsPlanAt(ctx, first.EffectiveMinute)
	if err != nil || older.ID != first.ID {
		t.Fatal("bounded plan lookup", older.ID, err)
	}
	duplicate := second
	duplicate.ID = uuid.New()
	duplicate.SessionID = uuid.New()
	duplicate.Revision = 1
	duplicate.RowHash = duplicate.Hash()
	// Reproduce an ambiguous historical boundary without the live writer's
	// successor check. The startup reader must keep rejecting it.
	if err = c.transaction(ctx, func(tx *sql.Tx) error {
		return putModels(ctx, c, tx, "options_collection_plan", []options.CollectionPlan{duplicate})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LatestOptionsPlan(ctx); err == nil || !strings.Contains(err.Error(), "ambiguous plan boundary") {
		t.Fatal("ambiguous boundary became valid", err)
	}
}
