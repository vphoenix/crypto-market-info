package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/optionslive"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

func liveDBFixture(t *testing.T, c *Client) (options.LiveRun, options.LiveEnvelope) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	if err := c.InitOptionsLiveSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.InitOptionsLiveSchema(ctx); err != nil {
		t.Fatal(err)
	}
	var parsed []deribit.ParsedInstrument
	for _, kind := range []string{"option", "future"} {
		raw, err := os.ReadFile("../../exchange/deribit/testdata/metadata-BTC-" + kind + ".json")
		if err != nil {
			t.Fatal(err)
		}
		items, _, err := deribit.DecodeInstruments(raw, "BTC", kind, at.Add(-time.Second))
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, items...)
	}
	var definitions []options.ContractSpec
	for _, p := range parsed {
		definitions = append(definitions, p.Spec)
	}
	specs, err := c.RegisterDerivativeSpecs(ctx, definitions)
	if err != nil {
		t.Fatal(err)
	}
	r := options.LiveRun{ID: uuid.New(), StartedAt: at.Add(-time.Second), RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2", Selection: "explicit", Indexes: []string{"btc_usd"}}
	e := options.LiveEnvelope{RunID: r.ID, MinuteTime: at, PreparedAt: at.Add(time.Minute)}
	for n, s := range specs {
		r.Members = append(r.Members, options.LiveMember{InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, DefinitionHash: s.DefinitionHash(), IndexID: s.IndexID})
		rule := parsed[n].Rule
		rule.InstrumentID = s.Instrument.ID
		if err = c.WriteDerivativeTradingRule(ctx, rule); err != nil {
			t.Fatal(err)
		}
		o := options.MetadataObservation{AttemptID: uuid.New(), RunID: r.ID, InstrumentID: s.Instrument.ID, Symbol: s.Instrument.ExchangeSymbol, Scope: "BTC:fixture", SourceURL: rule.SourceURL, RequestedAt: at.Add(-2 * time.Second), ObservedAt: at.Add(-time.Second), PayloadHash: rule.PayloadHash, Status: "complete", DefinitionHash: s.DefinitionHash(), TradingRuleID: rule.ID(), State: "open", Active: true, ScopeComplete: true, ScopeRawCount: 3, ScopeAcceptedCount: 3}
		if err = c.WriteOptionsMetadata(ctx, o); err != nil {
			t.Fatal(err)
		}
		b := derivativeSyntheticMinute(t, s.Instrument.ID, at, false, n == 0, true)
		for sec := range b.Quality {
			b.Quality[sec].TradingRuleID = rule.ID()
			b.Quality[sec].RulePublishedAt = at
			b.Quality[sec].MarketStateAt = at
		}
		e.Books = append(e.Books, b)
	}
	sort.Slice(r.Members, func(i, j int) bool { return r.Members[i].InstrumentID < r.Members[j].InstrumentID })
	sort.Slice(e.Books, func(i, j int) bool { return e.Books[i].InstrumentID < e.Books[j].InstrumentID })
	if err = c.WriteOptionsRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	e.RunHash = r.Hash()
	m := options.IndexMinute{IndexID: "btc_usd"}
	p := decimal.RequireFromString("81000.123456789012345678")
	for sec := range m.Samples {
		m.Samples[sec] = options.IndexSample{Price: &p, SourceTime: at.Add(-time.Second), ReceivedAt: at, Epoch: uuid.MustParse("00000000-0000-4000-8000-000000000001"), State: options.IndexHeld}
	}
	e.Indexes = []options.IndexMinute{m}
	return r, e
}
func TestOptionsLiveStorageCommitAndReferences(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	r, e := liveDBFixture(t, c)
	bad := e.Clone()
	bad.Books = bad.Books[:len(bad.Books)-1]
	if err := c.WriteOptionsMinute(ctx, bad); err == nil {
		t.Fatal("missing run member accepted")
	}
	once := true
	commitOnce := true
	c.derivativeAfterInsert = func(stage string) error {
		if stage == "live_index" && once {
			once = false
			return fmt.Errorf("injected failure")
		}
		if stage == "live_commit" && commitOnce {
			commitOnce = false
			return fmt.Errorf("commit acknowledgement lost")
		}
		return nil
	}
	if err := c.WriteOptionsMinute(ctx, e); err == nil {
		t.Fatal("injection ignored")
	}
	if _, err := c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan visible: %v", err)
	}
	if err := c.WriteOptionsMinute(ctx, e); err == nil {
		t.Fatal("commit acknowledgement failure ignored")
	}
	if err := c.WriteOptionsMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != e.ID() {
		t.Fatal("changed minute")
	}
	for _, b := range got.Books {
		rule, err := c.LoadDerivativeTradingRule(ctx, b.Quality[0].TradingRuleID)
		if err != nil || rule.InstrumentID != b.InstrumentID || rule.ID() != b.Quality[0].TradingRuleID {
			t.Fatalf("rule round trip: %v", err)
		}
		for sec := 0; sec < 60; sec++ {
			snap, err := replay.ReplayDerivative(b, uint8(sec))
			if err != nil || !snap.Quality.ReplayValid {
				t.Fatalf("replay %d: %v", sec, err)
			}
		}
	}
	if !got.Indexes[0].Samples[0].Price.Equal(*e.Indexes[0].Samples[0].Price) {
		t.Fatal("index decimal lost precision")
	}
	conflict := e.Clone()
	conflict.PreparedAt = conflict.PreparedAt.Add(time.Microsecond)
	if err = c.WriteOptionsMinute(ctx, conflict); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	if err = c.conn.Exec(ctx, `ALTER TABLE `+c.table("options_index_minute")+` DELETE WHERE batch_id=? SETTINGS mutations_sync=2`, e.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime); !errors.Is(err, replay.ErrIncompleteDerivativeBatch) {
		t.Fatalf("missing index not incomplete: %v", err)
	}
}

func TestOptionsLiveMissingMetadataIsIncomplete(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	r, e := liveDBFixture(t, c)
	if err := c.WriteOptionsMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := c.conn.Exec(ctx, `ALTER TABLE `+c.table("options_metadata_observation")+` DELETE WHERE run_id=? AND instrument_id=? SETTINGS mutations_sync=2`, r.ID, r.Members[0].InstrumentID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime); !errors.Is(err, replay.ErrIncompleteDerivativeBatch) {
		t.Fatalf("missing source evidence not incomplete: %v", err)
	}
}

type observedLiveSink struct {
	*Client
	cancel  context.CancelFunc
	written chan options.LiveEnvelope
}

func (s *observedLiveSink) WriteOptionsMinute(ctx context.Context, e options.LiveEnvelope) error {
	if err := s.Client.WriteOptionsMinute(ctx, e); err != nil {
		return err
	}
	valid := true
	for _, b := range e.Books {
		if b.Minute == nil || b.Minute.ValidBitmap == 0 {
			valid = false
		}
	}
	for _, m := range e.Indexes {
		found := false
		for _, v := range m.Samples {
			if v.Price != nil {
				found = true
			}
		}
		if !found {
			valid = false
		}
	}
	if valid {
		select {
		case s.written <- e:
			s.cancel()
		default:
		}
	}
	return nil
}
func TestOptionsLivePublicCollection(t *testing.T) {
	if os.Getenv("OPTIONS_PUBLIC_INTEGRATION") != "1" {
		t.Skip("opt-in bounded public Deribit capture")
	}
	c := derivativeIntegrationClient(t)
	cfg := optionslive.Config{Enabled: true, RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2"}
	client := deribit.NewClient(cfg.RESTURL, cfg.WSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// Exercise the legacy explicit API with a current public C/P/future triplet.
	// Automatic catalog collection has its own independent full-universe probe.
	plan, err := optionslive.Discover(ctx, client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, item := range plan.Selected {
		names[item.Spec.Instrument.ExchangeSymbol] = true
	}
	for _, future := range plan.Selected {
		if future.Spec.OptionType != "" {
			continue
		}
		for _, call := range plan.Selected {
			if call.Spec.OptionType != "call" || call.Spec.IndexID != future.Spec.IndexID || !call.Spec.Instrument.ExpiryTime.Equal(*future.Spec.Instrument.ExpiryTime) {
				continue
			}
			name := call.Spec.Instrument.ExchangeSymbol
			put := name[:len(name)-1] + "P"
			if names[put] {
				cfg.Symbols = []string{future.Spec.Instrument.ExchangeSymbol, name, put}
				break
			}
		}
		if len(cfg.Symbols) > 0 {
			break
		}
	}
	if len(cfg.Symbols) == 0 {
		t.Fatal("no public paired explicit triplet")
	}
	p, err := optionslive.Prepare(ctx, client, cfg, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("public selection run=%s members=%d indexes=%v", p.Run.ID, len(p.Run.Members), p.Run.Indexes)
	sink := &observedLiveSink{Client: c, cancel: cancel, written: make(chan options.LiveEnvelope, 1)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err = optionslive.Collect(ctx, client, p, sink, logger); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case e := <-sink.written:
		readCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		loaded, err := c.LoadOptionsMinute(readCtx, e.RunID, e.MinuteTime)
		if err != nil {
			t.Fatal(err)
		}
		for n, b := range loaded.Books {
			valid := 0
			for _, q := range b.Quality {
				if q.ReplayValid {
					valid++
				}
			}
			t.Logf("public member=%s valid_seconds=%d source=%s", p.Run.Members[n].Symbol, valid, b.Quality[59].SourceTime)
		}
		t.Logf("public stored minute=%s batch=%s database=%s", loaded.MinuteTime, loaded.ID(), c.database)
		measureOptionsLiveMinute(t, c, loaded)
	default:
		t.Fatal("no complete public minute with books and indices")
	}
}

// A short public capture reports its actual window; daily extrapolations are
// deliberately labelled estimates, not a replacement for a full-day sample.
func measureOptionsLiveMinute(t *testing.T, c *Client, e options.LiveEnvelope) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var total uint64
	for _, table := range []string{"derivative_book_minute", "derivative_book_second_delta", "derivative_book_quality_minute", "options_index_minute", "options_live_minute_commit"} {
		if err := c.conn.Exec(ctx, `OPTIMIZE TABLE `+c.table(table)+` FINAL`); err != nil {
			t.Fatal(err)
		}
		var bytes, rows uint64
		if err := c.conn.QueryRow(ctx, `SELECT sum(data_compressed_bytes),sum(rows) FROM system.parts WHERE active AND database=? AND table=?`, c.database, table).Scan(&bytes, &rows); err != nil {
			t.Fatal(err)
		}
		total += bytes
		t.Logf("public window table=%s compressed_bytes=%d rows=%d", table, bytes, rows)
	}
	var minutes uint64
	if err := c.conn.QueryRow(ctx, `SELECT count() FROM `+c.table("options_live_minute_commit")+` FINAL`).Scan(&minutes); err != nil || minutes == 0 {
		t.Fatalf("missing measurement window: %v", err)
	}
	t.Logf("public compressed window minutes=%d bytes=%d estimated_bytes_per_day=%d excludes catalog/run metadata", minutes, total, total*1440/minutes)
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		start := time.Now()
		loaded, err := c.LoadOptionsMinute(ctx, e.RunID, e.MinuteTime)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range loaded.Books {
			if _, err := replay.ReplayDerivative(b, uint8(i%60)); err != nil {
				t.Fatal(err)
			}
		}
		latencies[i] = time.Since(start)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("public 100 full-member loads/replays p50=%s p95=%s p99=%s", latencies[49], latencies[94], latencies[98])
}
