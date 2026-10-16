package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"net/url"
	"sort"
	"strconv"
	"time"
)

type discoveryFact = model.DiscoveryFact
type DiscoveryProof = contracts.DiscoveryProof
type publicationRecord struct {
	Kind, ID, Hash, Codec         string
	SourceMS, ObservedMS, KnownMS int64
	InstrumentID                  uint32
	EffectiveMS                   int64
	SourceCount                   uint16
	ProvenanceHash                string
}

const intentColumns = "source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,instrument_id,effective_ms,source_count,provenance_hash"
const statusColumns = "source_kind,source_id,content_hash,source_ms,observed_ms,known_from_ms,published_ms,complete,codec,instrument_id,effective_ms,source_count,provenance_hash"

func (c *Client) InitDiscoverySchema(ctx context.Context) error {
	if e := c.checkSchema(ctx, []string{"source_publication_intents", "source_batch_status", "public_observation_provenance", "cex_book_minute_commit", "funding_forecast_observation", "cex_trade_bar_1m", "cex_fee_schedule_rule", "cex_collateral_risk_rule", "okx_public_capital_parameters"}); e != nil {
		return e
	}
	if c.capitalRuleVersions {
		if err := c.checkSchema(ctx, CapitalVersionTableNames()); err != nil {
			return err
		}
	}
	c.discoveryEnabled.Store(true)
	return nil
}
func (c *Client) InitOKXPairSchema(ctx context.Context) error {
	if e := c.checkSchema(ctx, []string{"okx_public_loan_policy", "okx_paired_catalog"}); e != nil {
		return e
	}
	return c.InitDiscoverySchema(ctx)
}
func factSources(f discoveryFact) []model.PublicSource {
	switch x := f.(type) {
	case model.FundingPrediction:
		return x.Sources
	case model.TradeBar:
		return x.Sources
	case model.ReferenceFee:
		return x.Sources
	case model.CapitalParameters:
		return x.Sources
	case model.CapitalReference:
		return x.Parameters.Sources
	}
	return nil
}
func (c *Client) validateDiscoveryScope(ctx context.Context, f discoveryFact) error {
	b := f.Body()
	rows, e := c.records(ctx, c.db, "instrument", "WHERE instrument_id=?", b["instrument_id"])
	if e != nil {
		return e
	}
	if len(rows) != 1 {
		return fmt.Errorf("unregistered public fact instrument")
	}
	i, e := instrumentModel(rows[0])
	if e != nil {
		return e
	}
	return validateDiscoveryInstrument(f, i)
}

func validateDiscoveryInstrument(f discoveryFact, i model.Instrument) error {
	b := f.Body()
	sources := factSources(f)
	if e := model.ValidateDiscoverySources(f.Kind(), sources); e != nil {
		return e
	}
	if i.MarketType != model.MarketPerpetual || i.Exchange != "OKX" || i.QuoteAsset != "USDT" {
		return fmt.Errorf("unsupported discovery identity")
	}
	if base, ok := b["base"]; ok && base != i.BaseAsset {
		return fmt.Errorf("capital base mismatch")
	}
	var roles []map[string]string
	switch f.Kind() {
	case "funding_forecast_observation":
		roles = []map[string]string{{"instId": i.ExchangeSymbol}, {"instId": i.ExchangeSymbol, "instType": "SWAP"}, {"instId": i.BaseAsset + "-USD"}, {"instId": "USDT-USD"}}
		for n, k := range []string{"source_ms", "mark_ms", "base_index_ms", "quote_index_ms"} {
			if sources[n].SourceMS != b[k] {
				return fmt.Errorf("native source clock mismatch")
			}
		}
	case "cex_trade_bar_1m":
		roles = []map[string]string{{"instId": i.ExchangeSymbol, "bar": "1m"}}
		if sources[0].SourceMS != b["source_ms"] {
			return fmt.Errorf("bar source clock mismatch")
		}
	case "cex_fee_schedule_rule":
		roles = []map[string]string{{}, {"instType": "SPOT", "instId": i.BaseAsset + "-USDT"}, {"instType": "SWAP", "instId": i.ExchangeSymbol}}
	case "cex_collateral_risk_rule", "okx_public_capital_parameters":
		roles = []map[string]string{{}, {"instType": "SWAP", "tdMode": "cross", "instFamily": i.BaseAsset + "-USDT"}, {"instType": "MARGIN", "tdMode": "cross", "ccy": i.BaseAsset}}
	default:
		return fmt.Errorf("unsupported discovery kind")
	}
	first, last := sources[0].SourceMS, sources[0].ObservedMS
	for n, s := range sources {
		u, e := url.Parse(s.URL)
		if e != nil {
			return e
		}
		for k, want := range roles[n] {
			v := u.Query()[k]
			if len(v) != 1 || v[0] != want {
				return fmt.Errorf("source identity mismatch")
			}
		}
		first = min(first, s.SourceMS)
		last = max(last, s.ObservedMS)
	}
	if f.Kind() != "funding_forecast_observation" && (first != b["source_ms"] || last != b["observed_ms"]) {
		return fmt.Errorf("source time mismatch")
	}
	return nil
}
func (c *Client) beginDiscoveryIntent(ctx context.Context, p publicationRecord) (int64, error) {
	known, e := c.beginDiscoveryIntents(ctx, []publicationRecord{p})
	if e != nil {
		return 0, e
	}
	return known[0], nil
}
func (c *Client) beginDiscoveryIntents(ctx context.Context, publications []publicationRecord) ([]int64, error) {
	knownTimes := make([]int64, len(publications))
	err := c.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		knownTimes, e = c.discoveryIntents(ctx, tx, publications)
		return e
	})
	return knownTimes, err
}

// discoveryIntents participates in the caller's commit; durable book delivery
// already holds first knowledge before MySQL is available.
func (c *Client) discoveryIntents(ctx context.Context, tx *sql.Tx, publications []publicationRecord) ([]int64, error) {
	knownTimes := make([]int64, len(publications))
	now := model.CeilMilliseconds(time.Now().UTC())
	for _, p := range publications {
		if p.SourceMS <= 0 || p.SourceMS > p.ObservedMS || p.ObservedMS > p.KnownMS || p.KnownMS > now {
			return nil, fmt.Errorf("invalid first knowledge time")
		}
	}
	stored, err := c.publicationRows(ctx, tx, "source_publication_intents", publications)
	if err != nil {
		return nil, err
	}
	missing := make([]record, 0, len(publications))
	for n, p := range publications {
		knownTimes[n] = p.KnownMS
		if r, ok := stored[p.Kind+"\x00"+p.ID]; ok {
			old, err := scanPublication(r)
			if err != nil {
				return nil, err
			}
			knownTimes[n] = old.KnownMS
			old.KnownMS = p.KnownMS
			if old != p {
				return nil, contracts.ErrConflict
			}
			continue
		}
		r, err := namedRecord(intentColumns, []any{p.Kind, p.ID, p.Hash, p.Codec, p.SourceMS, p.ObservedMS, p.KnownMS, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
		if err != nil {
			return nil, err
		}
		missing = append(missing, r)
	}
	t, err := c.mapping("source_publication_intents")
	if err != nil {
		return nil, err
	}
	if err = insertAbsentImmutableRows(ctx, tx, c.table(t.Name), t, missing); err != nil {
		return nil, err
	}
	return knownTimes, nil
}

func (c *Client) publishDiscovery(ctx context.Context, tx *sql.Tx, p publicationRecord) error {
	return c.publishDiscoveryRows(ctx, tx, []publicationRecord{p})
}

func (c *Client) publishDiscoveryRows(ctx context.Context, tx *sql.Tx, publications []publicationRecord) error {
	stored, err := c.publicationRows(ctx, tx, "source_batch_status", publications)
	if err != nil {
		return err
	}
	missing := make([]record, 0, len(publications))
	for _, p := range publications {
		if r, ok := stored[p.Kind+"\x00"+p.ID]; ok {
			old, err := scanPublication(r)
			if err != nil {
				return err
			}
			if old != p || r["complete"] != uint8(1) || r["published_ms"].(int64) < p.KnownMS {
				return contracts.ErrConflict
			}
			continue
		}
		published := model.CeilMilliseconds(time.Now().UTC())
		if published < p.KnownMS {
			return fmt.Errorf("publication clock precedes knowledge")
		}
		r, err := namedRecord(statusColumns, []any{p.Kind, p.ID, p.Hash, p.SourceMS, p.ObservedMS, p.KnownMS, published, uint8(1), p.Codec, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
		if err != nil {
			return err
		}
		missing = append(missing, r)
	}
	t, err := c.mapping("source_batch_status")
	if err != nil {
		return err
	}
	return insertAbsentImmutableRows(ctx, tx, c.table(t.Name), t, missing)
}

func (c *Client) WriteDiscoveryFact(ctx context.Context, f discoveryFact) error {
	return c.WriteDiscoveryFacts(ctx, []model.DiscoveryFact{f})
}

// WriteDiscoveryFacts acknowledges only after the complete immutable group is
// committed. First-knowledge intents remain separately durable across a failed
// fact transaction; a retry preserves the original identities and timestamps.
func (c *Client) WriteDiscoveryFacts(ctx context.Context, facts []model.DiscoveryFact) error {
	if !c.discoveryEnabled.Load() {
		return fmt.Errorf("discovery schema not checked")
	}
	if len(facts) == 0 || len(facts) > 32 {
		return fmt.Errorf("invalid discovery group size")
	}
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	publications := make([]publicationRecord, 0, len(facts))
	groups := map[string][]record{}
	var provenance []record
	seen := map[string]bool{}
	preparedBytes := 0
	bodies := make([]map[string]any, len(facts))
	instrumentIDs := map[uint32]bool{}
	for n, f := range facts {
		if f == nil {
			return fmt.Errorf("nil discovery fact")
		}
		if err := f.Validate(); err != nil {
			return err
		}
		bodies[n] = f.Body()
		instrumentIDs[bodies[n]["instrument_id"].(uint32)] = true
	}
	ids := make([]uint32, 0, len(instrumentIDs))
	for id := range instrumentIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	args := make([]any, len(ids))
	for n, id := range ids {
		args[n] = id
	}
	rows, err := c.records(ctx, c.db, "instrument", "WHERE instrument_id IN ("+placeholders(len(args))+") ORDER BY instrument_id", args...)
	if err != nil {
		return err
	}
	instruments := make(map[uint32]model.Instrument, len(rows))
	for _, r := range rows {
		i, err := instrumentModel(r)
		if err != nil {
			return err
		}
		if !instrumentIDs[i.ID] {
			return contracts.ErrConflict
		}
		if _, duplicate := instruments[i.ID]; duplicate {
			return contracts.ErrConflict
		}
		instruments[i.ID] = i
	}
	for n, f := range facts {
		b := bodies[n]
		i, ok := instruments[b["instrument_id"].(uint32)]
		if !ok {
			return fmt.Errorf("unregistered public fact instrument")
		}
		if err := validateDiscoveryInstrument(f, i); err != nil {
			return err
		}
		sources := factSources(f)
		effective, _ := b["effective_ms"].(int64)
		p := publicationRecord{Kind: f.Kind(), ID: b["observation_id"].(string), Hash: model.DiscoveryHash(b), Codec: model.DiscoveryCodec, SourceMS: b["source_ms"].(int64), ObservedMS: b["observed_ms"].(int64), KnownMS: model.CeilMilliseconds(time.Now().UTC()), InstrumentID: b["instrument_id"].(uint32), EffectiveMS: effective, SourceCount: uint16(len(sources)), ProvenanceHash: model.DiscoverySourcesHash(sources)}
		key := p.Kind + "\x00" + p.ID
		if seen[key] {
			return contracts.ErrConflict
		}
		seen[key] = true
		publications = append(publications, p)
		b["row_hash"] = p.Hash
		if capital, ok := f.(model.CapitalParameters); ok {
			var err error
			p.Codec, err = c.capitalPublicationCodec(ctx, p)
			if err != nil {
				return err
			}
			publications[len(publications)-1] = p
			if p.Codec == model.CapitalVersionsCodec {
				if err = c.checkSchema(ctx, CapitalVersionTableNames()); err != nil {
					return err
				}
				versionRows, err := capitalVersionRecords(capital)
				if err != nil {
					return err
				}
				for table, rows := range versionRows {
					groups[table] = append(groups[table], rows...)
				}
			} else {
				groups[p.Kind] = append(groups[p.Kind], record(b))
			}
		} else {
			groups[p.Kind] = append(groups[p.Kind], record(b))
		}
		for n, source := range sources {
			r, err := namedRecord("source_kind,source_id,source_order,source_url,payload_hash,time_basis,source_ms,observed_ms", []any{p.Kind, p.ID, uint16(n), source.URL, source.PayloadHash, source.TimeBasis, source.SourceMS, source.ObservedMS})
			if err != nil {
				return err
			}
			provenance = append(provenance, r)
		}
	}
	groups["public_observation_provenance"] = provenance
	kinds := make([]string, 0, len(groups))
	for kind, rows := range groups {
		t, err := c.mapping(kind)
		if err != nil {
			return err
		}
		for _, r := range rows {
			raw, err := PackMigrationRow(t, MigrationRow(r))
			if err != nil {
				return err
			}
			preparedBytes += len(raw)
			if preparedBytes > 8<<20 {
				return fmt.Errorf("discovery group exceeds preparation budget")
			}
		}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	sort.Slice(publications, func(i, j int) bool {
		if publications[i].Kind != publications[j].Kind {
			return publications[i].Kind < publications[j].Kind
		}
		return publications[i].ID < publications[j].ID
	})
	known, err := c.beginDiscoveryIntents(ctx, publications)
	if err != nil {
		return err
	}
	for n := range publications {
		publications[n].KnownMS = known[n]
	}
	// The intent is already durable. Warm cold index leaves without holding
	// the exclusive generation fence; all facts and publication conditions
	// remain fully rechecked in the original atomic transaction below.
	for _, kind := range kinds {
		table, err := c.mapping(kind)
		if err != nil {
			return err
		}
		if err = c.prefetchImmutableKeys(ctx, table, groups[kind]); err != nil {
			return err
		}
	}
	statusTable, err := c.mapping("source_batch_status")
	if err != nil {
		return err
	}
	statusKeys := make([]record, 0, len(publications))
	for _, p := range publications {
		statusKeys = append(statusKeys, record{"source_kind": p.Kind, "source_id": p.ID})
	}
	if err = c.prefetchImmutableKeys(ctx, statusTable, statusKeys); err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		for _, kind := range kinds {
			t, err := c.mapping(kind)
			if err != nil {
				return err
			}
			if err = putImmutableRows(ctx, tx, c.table(t.Name), t, groups[kind]); err != nil {
				return err
			}
		}
		return c.publishDiscoveryRows(ctx, tx, publications)
	})
}

func (c *Client) WriteDiscoveryCapital(ctx context.Context, p model.CapitalParameters) error {
	facts := []model.DiscoveryFact{p, model.CapitalReference{Parameters: p}}
	if err := c.WriteDiscoveryFacts(ctx, facts); err == nil {
		return nil
	}
	// Preserve independent publication/retry behavior for pre-existing partial
	// histories or a conflicting sibling. Never recreate source observations.
	if err := c.WriteDiscoveryFact(ctx, facts[0]); err != nil {
		return err
	}
	return c.WriteDiscoveryFact(ctx, facts[1])
}
func minutePublication(b model.MinuteBatch, known int64) (publicationRecord, error) {
	return minutePublicationHash(b, known, model.DiscoveryHash(model.TradingMinuteBody(b)))
}
func minutePublicationHash(b model.MinuteBatch, known int64, hash string) (publicationRecord, error) {
	m := b.Minute
	if known < m.MinuteTime.UnixMilli()+60000 {
		return publicationRecord{}, fmt.Errorf("minute not sealed")
	}
	last := 0
	for sec := 0; sec < 60; sec++ {
		if m.ValidBitmap&(uint64(1)<<sec) != 0 {
			last = sec
		}
	}
	return publicationRecord{Kind: "cex_book_minute_commit", ID: strconv.FormatUint(m.ID, 10), Hash: hash, Codec: model.DiscoveryMinuteCodec, SourceMS: m.MinuteTime.UnixMilli() + int64(last)*1000, ObservedMS: m.MinuteTime.UnixMilli() + 60000, KnownMS: known, InstrumentID: m.InstrumentID, EffectiveMS: m.MinuteTime.UnixMilli()}, nil
}
func (c *Client) publishMinute(ctx context.Context, tx *sql.Tx, b model.MinuteBatch, known int64) error {
	p, e := minutePublication(b, known)
	if e != nil {
		return e
	}
	seconds := []uint8{}
	for _, d := range b.Deltas {
		seconds = append(seconds, d.SecondOffset)
	}
	if e = c.putValues(ctx, tx, "cex_book_minute_commit", "source_id,instrument_id,minute_ms,stored_depth,content_hash,delta_seconds,codec", []any{p.ID, p.InstrumentID, p.EffectiveMS, b.Minute.StoredDepth, p.Hash, seconds, p.Codec}); e != nil {
		return e
	}
	return c.publishDiscovery(ctx, tx, p)
}
