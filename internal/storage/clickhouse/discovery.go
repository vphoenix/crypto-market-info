package clickhouse

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed discovery_schema.sql
var discoverySchema string

func (c *Client) InitDiscoverySchema(ctx context.Context) error {
	for _, statement := range strings.Split(discoverySchema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if err := c.conn.Exec(ctx, strings.ReplaceAll(statement, "{database}", "`"+c.database+"`")); err != nil {
			return err
		}
	}
	// Completion metadata describes durable members, not merely acknowledged
	// in-memory inserts. These settings apply only when discovery is enabled.
	for _, name := range []string{"order_book_minute", "order_book_second_delta"} {
		var exists uint8
		if err := c.conn.QueryRow(ctx, "EXISTS TABLE "+c.table(name)).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			continue
		}
		if err := c.conn.Exec(ctx, "ALTER TABLE "+c.table(name)+" MODIFY SETTING fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1"); err != nil {
			return err
		}
	}
	c.discoveryEnabled.Store(true)
	return nil
}

type discoveryFact interface {
	Kind() string
	Body() map[string]any
	Validate() error
}
type publicationRecord struct {
	Kind, ID, Hash, Codec         string
	SourceMS, ObservedMS, KnownMS int64
	InstrumentID                  uint32
	EffectiveMS                   int64
	SourceCount                   uint16
	ProvenanceHash                string
}

func (c *Client) discoveryPublication(ctx context.Context, p publicationRecord) error {
	known, err := c.beginDiscoveryIntent(ctx, p)
	if err != nil {
		return err
	}
	p.KnownMS = known
	if p.ID == "" || !model.ValidDigest(p.Hash) || p.SourceMS <= 0 || p.SourceMS > p.ObservedMS || p.ObservedMS > p.KnownMS {
		return fmt.Errorf("invalid publication identity/time")
	}
	// Every producer client has one metadata serialization lock. Retrying an
	// ambiguous insert rereads the immutable first publication instead of giving
	// it a new timestamp or inserting a second marker.
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	return c.retryWrite(ctx, func(ctx context.Context) error {
		var hash, codec, provenance string
		var sourceCount uint16
		var instrument uint32
		var effective int64
		err := c.conn.QueryRow(ctx, "SELECT content_hash,codec,instrument_id,effective_ms,source_count,provenance_hash FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=? LIMIT 1", p.Kind, p.ID).Scan(&hash, &codec, &instrument, &effective, &sourceCount, &provenance)
		if err == nil {
			if hash != p.Hash || codec != p.Codec || instrument != p.InstrumentID || effective != p.EffectiveMS || sourceCount != p.SourceCount || provenance != p.ProvenanceHash {
				return fmt.Errorf("publication identity reused with changed content")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		published := model.CeilMilliseconds(time.Now().UTC())
		if published < p.KnownMS {
			return fmt.Errorf("publication clock precedes first knowledge")
		}
		return c.insertDerivativeRows(ctx, "source_batch_status", "source_kind,source_id,content_hash,source_ms,observed_ms,known_from_ms,published_ms,complete,codec,instrument_id,effective_ms,source_count,provenance_hash", [][]any{{p.Kind, p.ID, p.Hash, p.SourceMS, p.ObservedMS, p.KnownMS, published, uint8(1), p.Codec, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash}})
	})
}
func (c *Client) insertDiscoveryBody(ctx context.Context, kind string, body map[string]any) error {
	allowed := map[string]bool{"funding_forecast_observation": true, "cex_trade_bar_1m": true, "cex_fee_schedule_rule": true, "cex_collateral_risk_rule": true, "okx_public_capital_parameters": true}
	if !allowed[kind] {
		return fmt.Errorf("unsupported typed public fact")
	}
	columns := make([]string, 0, len(body))
	for k := range body {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	values := make([]any, 0, len(columns))
	for _, k := range columns {
		values = append(values, body[k])
	}
	return c.insertDerivativeRows(ctx, kind, strings.Join(columns, ","), [][]any{values})
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
func (c *Client) WriteDiscoveryFact(ctx context.Context, f discoveryFact) error {
	c.discoveryMu.Lock()
	defer c.discoveryMu.Unlock()
	if !c.discoveryEnabled.Load() {
		return fmt.Errorf("discovery schema not initialized")
	}
	if err := f.Validate(); err != nil {
		return err
	}
	sources := factSources(f)
	if err := c.validateDiscoveryScope(ctx, f); err != nil {
		return err
	}
	body := f.Body()
	id := body["observation_id"].(string)
	kind := f.Kind()
	hash := model.DiscoveryHash(body)
	known := model.CeilMilliseconds(time.Now().UTC())
	effective, _ := body["effective_ms"].(int64)
	intent := publicationRecord{Kind: kind, ID: id, Hash: hash, Codec: model.DiscoveryCodec, SourceMS: body["source_ms"].(int64), ObservedMS: body["observed_ms"].(int64), KnownMS: known, InstrumentID: body["instrument_id"].(uint32), EffectiveMS: effective, SourceCount: uint16(len(sources)), ProvenanceHash: model.DiscoverySourcesHash(sources)}
	first, err := c.beginDiscoveryIntent(ctx, intent)
	if err != nil {
		return err
	}
	known = first
	// Already complete rows are immutable and must not be rewritten on retry.
	var existing string
	err = c.conn.QueryRow(ctx, "SELECT content_hash FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=? LIMIT 1", kind, id).Scan(&existing)
	if err == nil {
		if existing != hash {
			return fmt.Errorf("public fact identity changed")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	body["row_hash"] = hash
	if err = c.retryWrite(ctx, func(ctx context.Context) error {
		// On an ambiguous fact insertion, use its existing hash before reinserting.
		var recorded string
		readErr := c.conn.QueryRow(ctx, "SELECT row_hash FROM "+c.table(kind)+" WHERE observation_id=? LIMIT 1", id).Scan(&recorded)
		if readErr == nil {
			if recorded != hash {
				return fmt.Errorf("public observation id changed")
			}
			return nil
		}
		if !errors.Is(readErr, sql.ErrNoRows) {
			return readErr
		}
		if err := c.insertDiscoveryBody(ctx, kind, body); err != nil {
			return err
		}
		return c.derivativeInserted(kind)
	}); err != nil {
		return err
	}
	rows := make([][]any, 0, len(sources))
	for n, s := range sources {
		rows = append(rows, []any{kind, id, uint16(n), s.URL, s.PayloadHash, s.TimeBasis, s.SourceMS, s.ObservedMS})
	}
	if err = c.retryWrite(ctx, func(ctx context.Context) error {
		return c.insertDerivativeRows(ctx, "public_observation_provenance", "source_kind,source_id,source_order,source_url,payload_hash,time_basis,source_ms,observed_ms", rows)
	}); err != nil {
		return err
	}
	return c.discoveryPublication(ctx, publicationRecord{Kind: kind, ID: id, Hash: hash, Codec: model.DiscoveryCodec, SourceMS: body["source_ms"].(int64), ObservedMS: body["observed_ms"].(int64), KnownMS: known, InstrumentID: body["instrument_id"].(uint32), EffectiveMS: effective, SourceCount: uint16(len(sources)), ProvenanceHash: model.DiscoverySourcesHash(sources)})
}
func (c *Client) WriteDiscoveryCapital(ctx context.Context, p model.CapitalParameters) error {
	if err := c.WriteDiscoveryFact(ctx, p); err != nil {
		return err
	}
	return c.WriteDiscoveryFact(ctx, model.CapitalReference{Parameters: p})
}
func (c *Client) publishMinute(ctx context.Context, batch model.MinuteBatch, known int64) error {
	m := batch.Minute
	hash := model.DiscoveryHash(model.TradingMinuteBody(batch))
	id := strconv.FormatUint(m.ID, 10)
	var existing string
	err := c.conn.QueryRow(ctx, "SELECT content_hash FROM "+c.table("cex_book_minute_commit")+" WHERE source_id=? LIMIT 1", id).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && existing != hash {
		return fmt.Errorf("minute id reused with different completed members")
	}
	seconds := make([]uint8, 0, len(batch.Deltas))
	for _, d := range batch.Deltas {
		seconds = append(seconds, d.SecondOffset)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if err = c.insertDerivativeRows(ctx, "cex_book_minute_commit", "source_id,instrument_id,minute_ms,stored_depth,content_hash,delta_seconds,codec", [][]any{{id, m.InstrumentID, m.MinuteTime.UnixMilli(), m.StoredDepth, hash, seconds, model.DiscoveryMinuteCodec}}); err != nil {
			return err
		}
	}
	publication, err := minutePublication(batch, known)
	if err != nil {
		return err
	}
	return c.discoveryPublication(ctx, publication)
}
