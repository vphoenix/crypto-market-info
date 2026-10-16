package mysql

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"os"
	"sort"
	"testing"
	"time"
)

func derivativeTestSpec(symbol string, native uint64) options.ContractSpec {
	settle := "BTC"
	expiry := time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)
	s := options.ContractSpec{Instrument: model.Instrument{Exchange: "Deribit", MarketType: model.MarketOption, ExchangeSymbol: symbol, BaseAsset: "BTC", QuoteAsset: "BTC", SettleAsset: &settle, ExpiryTime: &expiry, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: options.StorageUnit(), QuantityStepSize: options.StorageUnit()}, NativeID: native, CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), SourceInstrumentType: "reversed", NativeAmountKind: "base", NativeAmountCurrency: "BTC", ContractSize: decimal.NewFromInt(1), IndexID: "btc_usd", SettlementSemanticsID: "deribit-option-reversed-v1", OptionType: "call", Strike: decimal.NewFromInt(80000), StrikeCurrency: "USD", EvidenceHash: options.PayloadHash([]byte("synthetic definition"))}
	s.Instrument.VenueContractVersion = s.Version()
	return s
}
func derivativeSyntheticMinute(t *testing.T, id uint32, at time.Time, signed, active, anchor bool) model.DerivativeMinuteBatch {
	t.Helper()
	return derivativeSyntheticMinuteDepth(t, id, at, signed, active, anchor, 10)
}
func derivativeSyntheticMinuteDepth(t *testing.T, id uint32, at time.Time, signed, active, anchor bool, depth int) model.DerivativeMinuteBatch {
	t.Helper()
	buf, err := sampler.NewDerivativeMinuteBufferWithDepth(id, at, signed, depth)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	for second := 0; second < 60; second++ {
		tm := at.Add(time.Duration(second) * time.Second)
		received := at
		if active {
			received = tm
		}
		qty := uint64(7)
		if active {
			qty += uint64(second)
		}
		bid := int64(100)
		if signed {
			bid = 0
		}
		sequence := uint64(1)
		if active {
			sequence = uint64(second + 1)
		}
		s := model.DerivativeSnapshot{InstrumentID: id, SourceTime: received.Add(-time.Millisecond), ReceivedAt: received, Epoch: epoch, ChangeID: sequence}
		for level := int64(0); level < int64(depth); level++ {
			s.Bids = append(s.Bids, model.Level{PriceTick: bid - level, QtyLot: 7})
			s.Asks = append(s.Asks, model.Level{PriceTick: 101 + level, QtyLot: 9})
		}
		s.Bids[0].QtyLot = qty
		q := model.DerivativeQuality{Sampled: true, StreamValid: true, MarketKnown: true, MarketOpen: true, SourceTime: s.SourceTime, ReceivedAt: received, CapturedAt: tm, Epoch: epoch, ChangeID: s.ChangeID, LastSnapshotAt: at, MarketStateAt: at, MarketStateBasis: 2, BidLevels: uint8(depth), AskLevels: uint8(depth)}
		if !anchor && second == 0 {
			s = model.DerivativeSnapshot{}
			q.StreamValid = false
			q.Reason = model.DerivativeNotReady
		}
		if err := buf.Sample(tm, s, q); err != nil {
			t.Fatal(err)
		}
	}
	b, err := buf.Complete()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
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
func TestMySQLOptionsCatalogCommitReplayAndMissingEvidence(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	r, e := catalogDBFixture(t, c)
	raw, err := codec.EncodeOptionsCatalog(e)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodeOptionsCatalog(raw)
	if err != nil || decoded.ID() != e.ID() {
		t.Fatal("typed catalog codec", err)
	}
	meta, parts, err := codec.OptionsParts("options_catalog", raw)
	if err != nil {
		t.Fatal(err)
	}
	again, members, err := codec.JoinOptionsParts("options_catalog", meta, parts)
	if err != nil || string(again) != string(raw) || int(members) != len(e.Live.Books) {
		t.Fatal("typed option parts", err)
	}
	if os.Getenv("REDIS_INTEGRATION") == "1" {
		buffer, err := redisbuffer.Open(ctx, redisbuffer.Config{Socket: os.Getenv("CMI_REDIS_TEST_SOCKET"), Prefix: "test-option-" + uuid.NewString(), WaitTimeout: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer buffer.Close()
		d := c.Delivery("options_catalog", r.ID.String(), e.Live.MinuteTime, members, raw)
		if err = buffer.Accept(ctx, d); err != nil {
			t.Fatal(err)
		}
		loaded, err := buffer.Load(ctx, c.database, d.ID)
		if err != nil || string(loaded.Payload) != string(raw) {
			t.Fatal("redis typed option delivery", err)
		}
		if err = c.Commit(ctx, d); err != nil {
			t.Fatal(err)
		}
		if err = buffer.Ack(ctx, d, c.Committed); err != nil {
			t.Fatal(err)
		}
		if err = buffer.Cleanup(ctx, c.database, e.Live.MinuteTime.Add(3*time.Minute), 100); err != nil {
			t.Fatal(err)
		}
	}

	bad := e
	bad.Live = e.Live.Clone()
	bad.Live.Books = bad.Live.Books[:len(bad.Live.Books)-1]
	if err := c.WriteOptionsCatalogMinute(ctx, bad); err == nil {
		t.Fatal("missing member accepted")
	}
	for n := 0; n < 2; n++ {
		if err := c.WriteOptionsCatalogMinute(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.LoadOptionsCatalogMinute(ctx, r.ID, e.Live.MinuteTime)
	if err != nil || got.ID() != e.ID() {
		t.Fatal("catalog roundtrip", err)
	}
	for _, b := range got.Live.Books {
		for sec := 0; sec < 60; sec++ {
			snap, err := replay.ReplayDerivative(b, uint8(sec))
			if err != nil || !snap.Quality.ReplayValid {
				t.Fatal("replay", sec, err)
			}
		}
	}
	snapshot, err := c.LoadOptionsPlanSnapshot(ctx, e.Live.MinuteTime.Add(59*time.Second))
	if err != nil || snapshot.ValidBooks != len(r.Members) {
		t.Fatal("plan snapshot", err)
	}
	if _, err = c.db.ExecContext(ctx, "DELETE FROM "+c.table("options_catalog_quality_evidence_minute")+" WHERE run_id=?", r.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadOptionsMinute(ctx, r.ID, e.Live.MinuteTime); err == nil {
		t.Fatal("missing evidence visible")
	}
}
func TestMySQLOptionsLiveMinuteImmutableAndMetadata(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	r, e := liveDBFixture(t, c)
	if err := c.WriteOptionsMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteOptionsMinute(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime)
	if err != nil || got.ID() != e.ID() {
		t.Fatal("live roundtrip", err)
	}
	bad := e.Clone()
	bad.PreparedAt = bad.PreparedAt.Add(time.Microsecond)
	if err := c.WriteOptionsMinute(ctx, bad); err == nil {
		t.Fatal("mutable retry accepted")
	}
	if _, err = c.db.ExecContext(ctx, "DELETE FROM "+c.table("options_metadata_observation")+" WHERE run_id=?", r.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LoadOptionsMinute(ctx, r.ID, e.MinuteTime); err == nil {
		t.Fatal("missing metadata visible")
	}
}
