package clickhouse

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"os"
	"sort"
	"testing"
	"time"
)

func TestCatalogClickHouseWindowMeasurement(t *testing.T) {
	if os.Getenv("OPTIONS_CATALOG_MEASUREMENT") != "1" {
		t.Skip("opt-in R5 active/quiet measurement")
	}
	for _, active := range []bool{true, false} {
		name := "quiet"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			c := derivativeIntegrationClient(t)
			ctx := context.Background()
			r, template := catalogDBFixture(t, c)
			const minutes = 12
			for n := 0; n < minutes; n++ {
				at := template.Live.MinuteTime.Add(time.Duration(n) * time.Minute)
				e := options.CatalogEnvelope{PlanID: template.PlanID, PlanHash: template.PlanHash, Live: options.LiveEnvelope{RunID: r.ID, RunHash: r.Hash(), MinuteTime: at, PreparedAt: at.Add(time.Minute)}}
				for j, book := range template.Live.Books {
					b := derivativeSyntheticMinute(t, book.InstrumentID, at, false, active, true)
					for sec := range b.Quality {
						b.Quality[sec].TradingRuleID = book.Quality[0].TradingRuleID
						b.Quality[sec].RulePublishedAt = book.Quality[0].RulePublishedAt
						b.Quality[sec].MarketStateAt = book.Quality[0].MarketStateAt
					}
					e.Live.Books = append(e.Live.Books, b)
					proof := template.Evidence[j]
					proof.MinuteTime = at
					proof.LifecycleConfirmed = make([]*time.Time, 60)
					for sec := range proof.LifecycleConfirmed {
						confirmed := at.Add(time.Duration(sec) * time.Second)
						proof.LifecycleConfirmed[sec] = &confirmed
					}
					e.Evidence = append(e.Evidence, proof)
				}
				for _, index := range template.Live.Indexes {
					for sec := range index.Samples {
						index.Samples[sec].SourceTime = at.Add(-time.Second)
						index.Samples[sec].ReceivedAt = at
					}
					e.Live.Indexes = append(e.Live.Indexes, index)
				}
				if err := c.WriteOptionsCatalogMinute(ctx, e); err != nil {
					t.Fatal(n, err)
				}
			}
			var total uint64
			for _, table := range []string{"derivative_book_minute", "derivative_book_second_delta", "derivative_book_quality_minute", "options_index_minute", "options_catalog_quality_evidence_minute", "options_catalog_live_minute_commit"} {
				if err := c.conn.Exec(ctx, "OPTIMIZE TABLE "+c.table(table)+" FINAL"); err != nil {
					t.Fatal(err)
				}
				var bytes, rows uint64
				if err := c.conn.QueryRow(ctx, "SELECT sum(data_compressed_bytes),sum(rows) FROM system.parts WHERE active AND database=? AND table=?", c.database, table).Scan(&bytes, &rows); err != nil {
					t.Fatal(err)
				}
				t.Logf("R5 synthetic window table=%s compressed_bytes=%d rows=%d", table, bytes, rows)
				total += bytes
			}
			t.Logf("R5 minutes=%d books=%d compressed_bytes=%d estimated_bytes_per_book_day=%d excludes static catalog/definitions, not a full day", minutes, len(r.Members), total, total*1440/minutes/uint64(len(r.Members)))
			latencies := make([]time.Duration, 20)
			for n := range latencies {
				started := time.Now()
				e, err := c.LoadOptionsCatalogMinute(ctx, r.ID, template.Live.MinuteTime.Add(time.Duration(n%minutes)*time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				for _, b := range e.Live.Books {
					result, err := replay.ReplayDerivative(b, uint8(n%60))
					if err != nil || !result.Quality.ReplayValid {
						t.Fatal("invalid measured replay", err)
					}
				}
				latencies[n] = time.Since(started)
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			t.Logf("R5 complete-load+evidence-validation+replay p50=%s p95=%s", latencies[9], latencies[18])
		})
	}
}

func catalogDBFixture(t *testing.T, c *Client) (options.LiveRun, options.CatalogEnvelope) {
	t.Helper()
	ctx := context.Background()
	r, v := liveDBFixture(t, c)
	if err := c.InitOptionsCatalogSchema(ctx); err != nil {
		t.Fatal(err)
	}
	r.ID = uuid.New()
	r.Selection = options.CatalogSelection
	v.RunID = r.ID
	v.RunHash = r.Hash()
	if err := c.WriteOptionsRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	o := options.CatalogObservation{ID: uuid.New(), Scope: "BTC:option", Kind: "catalog", URL: "https://www.deribit.com/api/v2/public/get_instruments", RequestedAt: v.MinuteTime.Add(-2 * time.Second), ObservedAt: v.MinuteTime.Add(-time.Second), PayloadHash: options.PayloadHash([]byte("catalog fixture")), Status: "complete", RawCount: uint32(len(specs))}
	for n, s := range specs {
		o.NativeIDs = append(o.NativeIDs, s.NativeID)
		o.Symbols = append(o.Symbols, s.Instrument.ExchangeSymbol)
		o.InstrumentIDs = append(o.InstrumentIDs, s.Instrument.ID)
		o.Definitions = append(o.Definitions, s.DefinitionHash())
		o.Rules = append(o.Rules, v.Books[n].Quality[0].TradingRuleID)
		o.States = append(o.States, "open")
		o.Active = append(o.Active, true)
	}
	if err = c.WriteOptionsCatalog(ctx, o); err != nil {
		t.Fatal(err)
	}
	epoch := uuid.New()
	g := options.LifecycleObservation{ID: uuid.New(), Kind: "status", Channel: "public/status", Epoch: epoch, Sequence: 1, ReceivedAt: v.MinuteTime.Add(-time.Second), PayloadHash: options.PayloadHash([]byte("platform fixture")), LockMode: "false"}
	if err = c.WriteOptionsLifecycle(ctx, g); err != nil {
		t.Fatal(err)
	}
	p := options.CollectionPlan{ID: uuid.New(), SessionID: uuid.New(), Revision: 1, CreatedAt: v.MinuteTime.Add(-time.Second), EffectiveMinute: v.MinuteTime, ConfigHash: options.PayloadHash([]byte("config")), ObservationIDs: []uuid.UUID{o.ID}}
	for _, m := range r.Members {
		p.InstrumentIDs = append(p.InstrumentIDs, m.InstrumentID)
		p.Definitions = append(p.Definitions, m.DefinitionHash)
		p.RunIDs = append(p.RunIDs, r.ID)
	}
	if err = c.WriteOptionsPlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	e := options.CatalogEnvelope{PlanID: p.ID, PlanHash: p.Hash(), Live: v}
	for _, m := range r.Members {
		q := options.QualityEvidence{RunID: r.ID, InstrumentID: m.InstrumentID, MinuteTime: v.MinuteTime, StateIDs: make([]*uuid.UUID, 60), StateKinds: make([]uint8, 60), RuleObservationIDs: make([]*uuid.UUID, 60), PlatformIDs: make([]*uuid.UUID, 60), MaintenanceIDs: make([]*uuid.UUID, 60), LockIDs: make([]*uuid.UUID, 60), LifecycleEpochs: make([]uuid.UUID, 60), LifecycleConfirmed: make([]*time.Time, 60)}
		for sec := 0; sec < 60; sec++ {
			oid, gid := o.ID, g.ID
			confirmed := v.MinuteTime.Add(time.Duration(sec) * time.Second)
			q.StateIDs[sec] = &oid
			q.StateKinds[sec] = 2
			q.RuleObservationIDs[sec] = &oid
			q.PlatformIDs[sec] = &gid
			q.LifecycleEpochs[sec] = epoch
			q.LifecycleConfirmed[sec] = &confirmed
		}
		e.Evidence = append(e.Evidence, q)
	}
	return r, e
}
func TestCatalogClickHouseCommitRetryOwnershipAndReplay(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	r, e := catalogDBFixture(t, c)
	first := true
	c.derivativeAfterInsert = func(stage string) error {
		if stage == "catalog_evidence" && first {
			first = false
			return errors.New("ambiguous evidence insert")
		}
		return nil
	}
	if err := c.WriteOptionsCatalogMinute(ctx, e); err == nil {
		t.Fatal("fault did not fire")
	}
	if _, err := c.LoadOptionsMinute(ctx, r.ID, e.Live.MinuteTime); err == nil {
		t.Fatal("orphan accepted")
	}
	c.derivativeAfterInsert = nil
	if err := c.WriteOptionsCatalogMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteOptionsCatalogMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	loaded, err := c.LoadOptionsMinute(ctx, r.ID, e.Live.MinuteTime)
	if err != nil || loaded.ID() != e.Live.ID() {
		t.Fatal("load mismatch", err)
	}
	for _, b := range loaded.Books {
		for sec := 0; sec < 60; sec++ {
			result, err := replay.ReplayDerivative(b, uint8(sec))
			if err != nil || !result.Quality.ReplayValid {
				t.Fatal("replay", sec, err)
			}
		}
	}
	snapshot, err := c.LoadOptionsPlanSnapshot(ctx, e.Live.MinuteTime.Add(59*time.Second))
	if err != nil || snapshot.ValidBooks != len(r.Members) || snapshot.MissingBooks != 0 {
		t.Fatal("plan query", snapshot, err)
	}
	p, err := c.LoadOptionsPlan(ctx, e.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	p.ID = uuid.New()
	p.SessionID = uuid.New()
	p.Revision = 1
	p.PreviousID = e.PlanID
	p.PreviousHash = e.PlanHash
	p.CreatedAt = e.Live.MinuteTime.Add(30 * time.Second)
	p.EffectiveMinute = e.Live.MinuteTime.Add(time.Minute)
	if err = c.WriteOptionsPlan(ctx, p); err != nil {
		t.Fatal("successor", err)
	}
	fork := p
	fork.ID = uuid.New()
	if err = c.WriteOptionsPlan(ctx, fork); err == nil {
		t.Fatal("fork accepted")
	}
	if err = c.conn.Exec(ctx, "ALTER TABLE "+c.table("options_catalog_quality_evidence_minute")+" DELETE WHERE run_id=? SETTINGS mutations_sync=2", r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadOptionsMinute(ctx, r.ID, e.Live.MinuteTime); err == nil {
		t.Fatal("missing evidence accepted")
	}
}
func TestCatalogFixedLocksCancelable(t *testing.T) {
	c := &Client{}
	unlock, err := c.catalogLock(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.catalogLock(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal("lock not cancellable", err)
	}
	unlock()
	for n := 0; n < 10000; n++ {
		u, err := c.catalogLock(context.Background(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		u()
	}
	if len(c.catalogLocks) != 1024 {
		t.Fatal("unbounded locks")
	}
}
