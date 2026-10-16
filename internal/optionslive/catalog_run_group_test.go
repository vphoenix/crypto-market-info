package optionslive

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type runGroupSink struct {
	*catalogMemorySink
	group []options.LiveRun
	err   error
}

func (s *runGroupSink) WriteOptionsRuns(_ context.Context, r []options.LiveRun) error {
	s.group = append([]options.LiveRun(nil), r...)
	return s.err
}

func TestPlanRunGroupKeepsFrozenIdentitiesAndLegacySink(t *testing.T) {
	runs := make([]options.LiveRun, 5)
	for n := range runs {
		runs[n] = options.LiveRun{ID: uuid.New(), StartedAt: time.Now().UTC().Truncate(time.Microsecond), RESTURL: "https://example.invalid", WSURL: "wss://example.invalid", Selection: options.CatalogSelection, Members: []options.LiveMember{{InstrumentID: 1, Symbol: "BTC-fixture", DefinitionHash: strings.Repeat("a", 64), IndexID: "btc_usd"}}, Indexes: []string{"btc_usd"}}
	}
	groups := []catalogGroup{{run: runs[0]}, {run: runs[1], retire: true}, {run: runs[2], worker: &catalogWorker{run: runs[2], persisted: true}}, {run: runs[3], worker: &catalogWorker{run: runs[2], persisted: true}}, {run: runs[4]}}
	want := []options.LiveRun{runs[0], runs[3], runs[4]}
	failed := errors.New("bounded storage failure")
	bulk := &runGroupSink{catalogMemorySink: &catalogMemorySink{}, err: failed}
	if err := persistCatalogPlanRuns(context.Background(), bulk, groups); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bulk.group, want) || len(bulk.runs) != 0 {
		t.Fatal("batch identities changed or legacy fallback used on failure")
	}
	bulk.err = nil
	if err := persistCatalogPlanRuns(context.Background(), bulk, groups); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bulk.group, want) {
		t.Fatal("retry changed frozen run identities")
	}
	legacy := &catalogMemorySink{}
	if err := persistCatalogPlanRuns(context.Background(), legacy, groups); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy.runs, want) {
		t.Fatal("legacy sink scope changed")
	}
}
